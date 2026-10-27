//! ACP over streamable HTTP and WebSocket, served with `--listen`.
//!
//! The SDK's [`AcpHttpServer`] implements the transport. This module adds what
//! the SDK leaves to its host: a bearer token on every request, `Host` and
//! `Origin` checks against DNS rebinding and browser pages, and serving one
//! ACP connection at a time.
//!
//! Each connection gets its own [`Harness`] and an empty in-memory session
//! store over the shared session files, so it starts exactly as a new stdio
//! process would. Serving one connection at a time keeps the rest of the agent
//! free to treat its memory as the connection's state: a turn's updates,
//! permission requests, and elicitations can only go to the connection that
//! started it, and no two connections can run turns on one session.

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError},
    time::{Duration, Instant},
};

use agent_client_protocol::{
    Agent, Client, ConnectTo, ConnectionTo, DynConnectTo, Error, Responder, UntypedMessage,
};
use agent_client_protocol_http::{AcpHttpServer, ServerOptions};
use axum::{
    body::Body,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header, uri::Authority},
    middleware::Next,
    response::{IntoResponse, Response},
    serve::ListenerExt as _,
};
use futures_util::StreamExt as _;
use serde_json::Value;
use subtle::ConstantTimeEq as _;
use tokio_util::sync::CancellationToken;

use crate::{agent::Harness, config::Config, session::SessionStore};

/// The environment variable that holds the bearer token.
pub const TOKEN_ENV: &str = "DSH_ACP_LISTEN_TOKEN";
const ENDPOINT: &str = "/acp";
const CONNECTION_ID_HEADER: &str = "acp-connection-id";
const MIN_TOKEN_CHARS: usize = 16;
/// How long an HTTP connection may go without an open event stream. A client
/// needs one to receive anything, so a connection without one for this long
/// was abandoned, usually by a client that exited without `DELETE`. Closing
/// it lets the next client connect.
const HTTP_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// How long a new connection waits for the previous one to finish closing.
const ADMISSION_WAIT: Duration = Duration::from_secs(2);
const BUSY: &str = "another ACP connection is active; dsh-acp serves one connection at a time";

/// Options for [`serve`].
#[derive(Debug, Clone)]
pub struct ListenOptions {
    /// `host:port` to listen on. Port 0 picks a free port.
    pub address: String,
    /// Allow an address other machines can reach.
    pub allow_remote: bool,
    /// The token clients must send. `None` generates one.
    pub token: Option<String>,
}

/// Serve ACP over streamable HTTP and WebSocket until the process is stopped.
pub async fn serve(config: Config, options: ListenOptions) -> Result<(), Error> {
    crate::agent::init_tracing(&config);
    let (token, generated) = match options.token {
        Some(token) => (checked_token(token)?, false),
        None => (generate_token()?, true),
    };
    let address = ListenAddress::resolve(&options.address).await?;
    if !options.allow_remote
        && let Some(remote) = address
            .sockets
            .iter()
            .find(|socket| !is_loopback(socket.ip()))
    {
        return Err(Error::invalid_params().data(format!(
            "refusing to listen on {remote}, which is not a loopback address; \
             pass --listen-allow-remote to accept connections from other machines"
        )));
    }
    let listener = address.bind().await?;
    let local = listener.local_addr().map_err(Error::into_internal_error)?;
    let store = SessionStore::open(&config).await?;
    let slot = Arc::new(ConnectionSlot::default());
    let checks = Arc::new(RequestChecks {
        token,
        hosts: AllowedHosts::new(local.ip(), options.allow_remote.then_some(&address)),
        slot: slot.clone(),
    });
    let server = AcpHttpServer::new({
        let slot = slot.clone();
        move || connection(&config, &store, &slot)
    })
    .with_options(
        ServerOptions::default()
            .with_path(ENDPOINT)
            .with_health_endpoint(false),
    );
    let app = server
        .into_router()
        .layer(axum::middleware::from_fn_with_state(
            checks.clone(),
            check_request,
        ));
    tokio::spawn(close_idle_connections(slot));

    let credential = if generated {
        checks.token.as_str()
    } else {
        "<the DSH_ACP_LISTEN_TOKEN value>"
    };
    // The only place the token is ever written. stderr never carries ACP.
    eprintln!(
        "dsh-acp: serving ACP at http://{local}{ENDPOINT} and ws://{local}{ENDPOINT}; \
         send \"Authorization: Bearer {credential}\""
    );
    let listener = listener.tap_io(|stream| {
        // Streamed deltas are small; do not hold them back.
        let _ = stream.set_nodelay(true);
    });
    axum::serve(listener, app)
        .await
        .map_err(Error::into_internal_error)
}

/// The agent for a connection the SDK is creating, or one that refuses it if
/// another connection holds the slot.
fn connection(
    config: &Config,
    store: &SessionStore,
    slot: &Arc<ConnectionSlot>,
) -> DynConnectTo<Client> {
    let created_by = CREATED_BY_POST.try_with(Arc::clone).ok();
    let Some(lease) = slot.claim(created_by.is_some()) else {
        tracing::warn!("refused an ACP connection while another one is active");
        return DynConnectTo::new(busy_agent());
    };
    if let Some(created_by) = created_by {
        let _ = created_by.set(lease.generation);
    }
    let harness = Harness::new(config.clone(), store.fresh());
    DynConnectTo::new(Leased {
        agent: crate::agent::component(harness),
        lease,
    })
}

/// Answers every request with an error. Only reached when two connections
/// race past the admission check; the loser gets this.
fn busy_agent() -> impl ConnectTo<Client> {
    Agent.builder().name("dsh-acp-busy").on_receive_request(
        async |_request: UntypedMessage,
               responder: Responder<Value>,
               _connection: ConnectionTo<Client>| {
            responder.respond_with_error(Error::invalid_request().data(BUSY))
        },
        agent_client_protocol::on_receive_request!(),
    )
}

/// A connection's agent, holding the slot until the connection ends.
struct Leased<C> {
    agent: C,
    lease: Lease,
}

impl<C: ConnectTo<Client>> ConnectTo<Client> for Leased<C> {
    async fn connect_to(self, client: impl ConnectTo<Agent>) -> Result<(), Error> {
        let Self { agent, lease } = self;
        let closed = lease.closed.clone();
        // Ending the agent is how an abandoned connection is closed: the SDK
        // then drops it and its streams.
        let result = tokio::select! {
            result = agent.connect_to(client) => result,
            () = closed.cancelled() => Ok(()),
        };
        drop(lease);
        result
    }
}

tokio::task_local! {
    /// Set while the SDK handles a POST, so a connection that POST creates
    /// is known to be HTTP and can be matched to its `Acp-Connection-Id`.
    static CREATED_BY_POST: Arc<OnceLock<u64>>;
}

struct RequestChecks {
    token: String,
    hosts: AllowedHosts,
    slot: Arc<ConnectionSlot>,
}

impl RequestChecks {
    fn authorized(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let (Some(value), None) = (values.next(), values.next()) else {
            return false;
        };
        let Some((scheme, credentials)) = value
            .as_bytes()
            .iter()
            .position(|byte| *byte == b' ')
            .map(|space| value.as_bytes().split_at(space))
        else {
            return false;
        };
        let credentials = credentials.trim_ascii_start();
        scheme.eq_ignore_ascii_case(b"bearer")
            && bool::from(credentials.ct_eq(self.token.as_bytes()))
    }
}

/// Runs before the SDK sees a request. Nothing reaches ACP processing unless
/// the `Host` is allowed, there is no browser `Origin`, and the bearer token
/// matches.
async fn check_request(
    State(checks): State<Arc<RequestChecks>>,
    request: Request,
    next: Next,
) -> Response {
    if !checks.hosts.allows(&request) {
        return refuse(StatusCode::FORBIDDEN, "Host is not allowed");
    }
    if request.headers().contains_key(header::ORIGIN) {
        return refuse(StatusCode::FORBIDDEN, "browser origins are not allowed");
    }
    if !checks.authorized(request.headers()) {
        let mut response = refuse(StatusCode::UNAUTHORIZED, "a valid bearer token is required");
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    }
    let connection_id = header_text(request.headers(), CONNECTION_ID_HEADER).map(str::to_owned);
    let websocket = is_websocket_upgrade(&request);
    let opens_connection =
        websocket || (request.method() == Method::POST && connection_id.is_none());
    if opens_connection && !checks.slot.wait_until_free(ADMISSION_WAIT).await {
        return refuse(StatusCode::CONFLICT, BUSY);
    }
    if let Some(connection_id) = &connection_id {
        checks.slot.touch(connection_id);
    }
    if request.method() == Method::POST {
        let created_by = Arc::new(OnceLock::new());
        let response = CREATED_BY_POST
            .scope(created_by.clone(), next.run(request))
            .await;
        if let (Some(generation), Some(connection_id)) = (
            created_by.get(),
            header_text(response.headers(), CONNECTION_ID_HEADER),
        ) {
            checks.slot.bind(*generation, connection_id);
        }
        return response;
    }
    let event_stream = request.method() == Method::GET && !websocket;
    let response = next.run(request).await;
    if event_stream
        && response.status() == StatusCode::OK
        && let Some(connection_id) = &connection_id
        && let Some(open) = checks.slot.open_stream(connection_id)
    {
        let (parts, body) = response.into_parts();
        let body = body.into_data_stream().map(move |chunk| {
            // Counted as open until the client goes away and the body drops.
            let _ = &open;
            chunk
        });
        return Response::from_parts(parts, Body::from_stream(body));
    }
    response
}

fn refuse(status: StatusCode, reason: &'static str) -> Response {
    tracing::debug!(%status, reason, "refused an HTTP request");
    (status, reason).into_response()
}

fn header_text<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn is_websocket_upgrade(request: &Request) -> bool {
    request.method() == Method::GET
        && request
            .headers()
            .get_all(header::UPGRADE)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|protocol| protocol.trim().eq_ignore_ascii_case("websocket"))
}

/// The host names a request may address. A DNS rebinding page reaches the
/// server under its own name, which is not on this list.
#[derive(Debug)]
struct AllowedHosts {
    names: Vec<String>,
    addresses: Vec<IpAddr>,
}

impl AllowedHosts {
    /// Loopback names and the bound address, plus the configured host when
    /// remote connections are allowed.
    fn new(bound: IpAddr, remote: Option<&ListenAddress>) -> Self {
        let mut names = vec!["localhost".to_string()];
        let mut addresses = vec![
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
        ];
        let mut add = |address: IpAddr| {
            let address = address.to_canonical();
            if !address.is_unspecified() && !addresses.contains(&address) {
                addresses.push(address);
            }
        };
        add(bound);
        if let Some(remote) = remote {
            match &remote.name {
                Some(name) => names.push(name.clone()),
                None => remote.sockets.iter().for_each(|socket| add(socket.ip())),
            }
        }
        Self { names, addresses }
    }

    /// Every host the request names, in its URI or `Host` header, must be
    /// allowed, and it must name one.
    fn allows(&self, request: &Request) -> bool {
        let mut hosts = Vec::new();
        if let Some(authority) = request.uri().authority() {
            hosts.push(authority.host().to_string());
        }
        let mut values = request.headers().get_all(header::HOST).iter();
        match (values.next(), values.next()) {
            (Some(_), Some(_)) => return false,
            (Some(value), None) => {
                let Some(authority) = value
                    .to_str()
                    .ok()
                    .filter(|value| !value.contains('@'))
                    .and_then(|value| value.parse::<Authority>().ok())
                else {
                    return false;
                };
                hosts.push(authority.host().to_string());
            }
            (None, _) => {}
        }
        !hosts.is_empty() && hosts.iter().all(|host| self.allows_host(host))
    }

    fn allows_host(&self, host: &str) -> bool {
        let host = host.to_ascii_lowercase();
        let literal = host
            .strip_prefix('[')
            .and_then(|host| host.strip_suffix(']'))
            .unwrap_or(&host);
        match literal.parse::<IpAddr>() {
            Ok(address) => self.addresses.contains(&address.to_canonical()),
            Err(_) => self.names.contains(&host),
        }
    }
}

/// A parsed `--listen` value.
#[derive(Debug)]
struct ListenAddress {
    /// The host name, when the value named one rather than an IP address.
    name: Option<String>,
    sockets: Vec<SocketAddr>,
}

impl ListenAddress {
    async fn resolve(value: &str) -> Result<Self, Error> {
        if let Ok(socket) = value.parse::<SocketAddr>() {
            return Ok(Self {
                name: None,
                sockets: vec![socket],
            });
        }
        let invalid = || {
            Error::invalid_params().data(format!(
                "--listen must be HOST:PORT, such as 127.0.0.1:8787 or [::1]:8787; got {value}"
            ))
        };
        let (host, port) = value.rsplit_once(':').ok_or_else(invalid)?;
        let port = port.parse::<u16>().map_err(|_| invalid())?;
        if host.is_empty() || host.contains([':', '[', ']']) {
            return Err(invalid());
        }
        let sockets = tokio::net::lookup_host((host, port))
            .await
            .map_err(|error| {
                Error::invalid_params()
                    .data(format!("cannot resolve --listen host {host}: {error}"))
            })?
            .collect::<Vec<_>>();
        if sockets.is_empty() {
            return Err(
                Error::invalid_params().data(format!("--listen host {host} has no addresses"))
            );
        }
        Ok(Self {
            name: Some(host.to_ascii_lowercase()),
            sockets,
        })
    }

    async fn bind(&self) -> Result<tokio::net::TcpListener, Error> {
        let mut last_error = None;
        for socket in &self.sockets {
            match tokio::net::TcpListener::bind(socket).await {
                Ok(listener) => return Ok(listener),
                Err(error) => last_error = Some((socket, error)),
            }
        }
        let (socket, error) = last_error.expect("a resolved address has at least one socket");
        Err(Error::internal_error().data(format!("cannot listen on {socket}: {error}")))
    }
}

fn is_loopback(address: IpAddr) -> bool {
    address.to_canonical().is_loopback()
}

fn checked_token(token: String) -> Result<String, Error> {
    if token.len() < MIN_TOKEN_CHARS || !token.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(Error::invalid_params().data(format!(
            "{TOKEN_ENV} must be at least {MIN_TOKEN_CHARS} visible ASCII characters, without spaces"
        )));
    }
    Ok(token)
}

/// 256 bits from the operating system's generator, as hex.
fn generate_token() -> Result<String, Error> {
    let mut bytes = [0u8; 32];
    getrandom::fill(&mut bytes).map_err(|error| {
        Error::internal_error().data(format!("cannot generate a listen token: {error}"))
    })?;
    Ok(bytes
        .iter()
        .fold(String::with_capacity(64), |mut token, byte| {
            use std::fmt::Write as _;
            let _ = write!(token, "{byte:02x}");
            token
        }))
}

async fn close_idle_connections(slot: Arc<ConnectionSlot>) {
    let mut ticks = tokio::time::interval(Duration::from_secs(1));
    loop {
        ticks.tick().await;
        if slot.close_idle(Instant::now(), HTTP_IDLE_TIMEOUT) {
            tracing::info!("closing an HTTP ACP connection that has no open event stream");
        }
    }
}

/// Which connection, if any, the server is serving.
#[derive(Debug, Default)]
struct ConnectionSlot {
    state: Mutex<SlotState>,
    released: tokio::sync::Notify,
}

#[derive(Debug, Default)]
struct SlotState {
    generations: u64,
    active: Option<ActiveConnection>,
}

#[derive(Debug)]
struct ActiveConnection {
    generation: u64,
    closed: CancellationToken,
    /// `None` for a WebSocket connection, which ends with its socket.
    http: Option<HttpActivity>,
}

#[derive(Debug)]
struct HttpActivity {
    /// Learned from the `initialize` response.
    connection_id: Option<String>,
    open_streams: usize,
    last_seen: Instant,
}

/// Holds the slot for one connection until dropped.
#[derive(Debug)]
struct Lease {
    slot: Arc<ConnectionSlot>,
    generation: u64,
    closed: CancellationToken,
}

impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.slot.state();
        if state
            .active
            .as_ref()
            .is_some_and(|active| active.generation == self.generation)
        {
            state.active = None;
        }
        drop(state);
        self.slot.released.notify_waiters();
    }
}

/// An event stream of the active HTTP connection, open until dropped.
#[derive(Debug)]
struct OpenStream {
    slot: Arc<ConnectionSlot>,
    generation: u64,
}

impl Drop for OpenStream {
    fn drop(&mut self) {
        if let Some(http) = self.slot.state().http(self.generation) {
            http.open_streams = http.open_streams.saturating_sub(1);
            http.last_seen = Instant::now();
        }
    }
}

impl SlotState {
    fn http(&mut self, generation: u64) -> Option<&mut HttpActivity> {
        self.active
            .as_mut()
            .filter(|active| active.generation == generation)
            .and_then(|active| active.http.as_mut())
    }

    fn http_by_id(&mut self, connection_id: &str) -> Option<(u64, &mut HttpActivity)> {
        let active = self.active.as_mut()?;
        let http = active.http.as_mut()?;
        (http.connection_id.as_deref() == Some(connection_id)).then_some((active.generation, http))
    }
}

impl ConnectionSlot {
    fn state(&self) -> MutexGuard<'_, SlotState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn claim(self: &Arc<Self>, http: bool) -> Option<Lease> {
        let mut state = self.state();
        if state.active.is_some() {
            return None;
        }
        state.generations += 1;
        let generation = state.generations;
        let closed = CancellationToken::new();
        state.active = Some(ActiveConnection {
            generation,
            closed: closed.clone(),
            http: http.then(|| HttpActivity {
                connection_id: None,
                open_streams: 0,
                last_seen: Instant::now(),
            }),
        });
        Some(Lease {
            slot: self.clone(),
            generation,
            closed,
        })
    }

    fn is_free(&self) -> bool {
        self.state().active.is_none()
    }

    /// Wait up to `timeout` for the active connection to finish closing.
    async fn wait_until_free(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let released = self.released.notified();
            if self.is_free() {
                return true;
            }
            if tokio::time::timeout_at(deadline, released).await.is_err() {
                return self.is_free();
            }
        }
    }

    /// Record the `Acp-Connection-Id` of the HTTP connection `generation`.
    fn bind(&self, generation: u64, connection_id: &str) {
        if let Some(http) = self.state().http(generation) {
            http.connection_id = Some(connection_id.to_string());
            http.last_seen = Instant::now();
        }
    }

    fn touch(&self, connection_id: &str) {
        if let Some((_, http)) = self.state().http_by_id(connection_id) {
            http.last_seen = Instant::now();
        }
    }

    fn open_stream(self: &Arc<Self>, connection_id: &str) -> Option<OpenStream> {
        let mut state = self.state();
        let (generation, http) = state.http_by_id(connection_id)?;
        http.open_streams += 1;
        http.last_seen = Instant::now();
        Some(OpenStream {
            slot: self.clone(),
            generation,
        })
    }

    /// Close the active HTTP connection if it has had no open event stream
    /// for `idle`. Returns whether it did.
    fn close_idle(&self, now: Instant, idle: Duration) -> bool {
        let state = self.state();
        let Some(active) = &state.active else {
            return false;
        };
        let Some(http) = &active.http else {
            return false;
        };
        if http.open_streams > 0
            || now.saturating_duration_since(http.last_seen) < idle
            || active.closed.is_cancelled()
        {
            return false;
        }
        active.closed.cancel();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(uri: &str, host: Option<&str>) -> Request {
        let mut builder = axum::http::Request::builder().uri(uri);
        if let Some(host) = host {
            builder = builder.header(header::HOST, host);
        }
        builder.body(Body::empty()).unwrap()
    }

    #[test]
    fn hosts_are_loopback_names_and_the_bound_address() {
        let hosts = AllowedHosts::new("127.0.0.1".parse().unwrap(), None);
        for host in [
            "localhost",
            "LOCALHOST:8787",
            "127.0.0.1:1",
            "[::1]:8787",
            "[0:0:0:0:0:0:0:1]",
        ] {
            assert!(hosts.allows(&request("/acp", Some(host))), "{host}");
        }
        for host in [
            "evil.example:8787",
            "localhost.evil.example",
            "127.0.0.2",
            "0.0.0.0:8787",
            "user@localhost",
            "",
        ] {
            assert!(!hosts.allows(&request("/acp", Some(host))), "{host}");
        }
        assert!(
            !hosts.allows(&request("/acp", None)),
            "a request must name a host"
        );
        assert!(
            !hosts.allows(&request("http://evil.example/acp", Some("localhost"))),
            "every host the request names must be allowed"
        );
        let mut duplicate = request("/acp", Some("localhost"));
        duplicate
            .headers_mut()
            .append(header::HOST, HeaderValue::from_static("localhost"));
        assert!(!hosts.allows(&duplicate));
    }

    #[test]
    fn the_configured_host_is_allowed_only_for_remote_listening() {
        let address = ListenAddress {
            name: Some("devbox.example".to_string()),
            sockets: vec!["192.0.2.10:8787".parse().unwrap()],
        };
        let bound = "192.0.2.10".parse().unwrap();
        let remote = AllowedHosts::new(bound, Some(&address));
        assert!(remote.allows(&request("/acp", Some("DevBox.example:8787"))));
        assert!(remote.allows(&request("/acp", Some("192.0.2.10:8787"))));
        let local = AllowedHosts::new(bound, None);
        assert!(!local.allows(&request("/acp", Some("devbox.example:8787"))));
        let wildcard = AllowedHosts::new("0.0.0.0".parse().unwrap(), None);
        assert_eq!(
            wildcard.addresses,
            [
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                IpAddr::V6(Ipv6Addr::LOCALHOST)
            ]
        );
    }

    #[test]
    fn only_the_exact_bearer_token_is_authorized() {
        let checks = RequestChecks {
            token: "0123456789abcdef-token".to_string(),
            hosts: AllowedHosts::new("127.0.0.1".parse().unwrap(), None),
            slot: Arc::default(),
        };
        let headers = |values: &[&str]| {
            let mut headers = HeaderMap::new();
            for value in values {
                headers.append(header::AUTHORIZATION, HeaderValue::from_str(value).unwrap());
            }
            headers
        };
        assert!(checks.authorized(&headers(&["Bearer 0123456789abcdef-token"])));
        assert!(checks.authorized(&headers(&["bearer  0123456789abcdef-token"])));
        for rejected in [
            &[][..],
            &["Bearer 0123456789abcdef-toke"],
            &["Bearer 0123456789abcdef-tokenx"],
            &["Basic 0123456789abcdef-token"],
            &["0123456789abcdef-token"],
            &["Bearer"],
            &[
                "Bearer 0123456789abcdef-token",
                "Bearer 0123456789abcdef-token",
            ],
        ] {
            assert!(!checks.authorized(&headers(rejected)), "{rejected:?}");
        }
    }

    #[test]
    fn tokens_are_long_visible_ascii() {
        let token = generate_token().unwrap();
        assert_eq!(token.len(), 64);
        assert!(token.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_ne!(token, generate_token().unwrap());
        assert!(checked_token(token).is_ok());
        assert!(checked_token("short".to_string()).is_err());
        assert!(checked_token("sixteen chars no".to_string()).is_err());
        assert!(checked_token("sixteen-chars-ok".to_string()).is_ok());
    }

    #[tokio::test]
    async fn listen_addresses_must_name_a_port() {
        let socket = ListenAddress::resolve("[::1]:0").await.unwrap();
        assert_eq!(socket.name, None);
        assert!(socket.sockets[0].ip().is_loopback());
        let named = ListenAddress::resolve("LocalHost:8787").await.unwrap();
        assert_eq!(named.name.as_deref(), Some("localhost"));
        for value in ["127.0.0.1", "localhost", ":8787", "::1:8787", "localhost:x"] {
            assert!(ListenAddress::resolve(value).await.is_err(), "{value}");
        }
        assert!(is_loopback("::ffff:127.0.0.1".parse().unwrap()));
        assert!(!is_loopback("0.0.0.0".parse().unwrap()));
    }

    #[tokio::test]
    async fn one_connection_holds_the_slot_until_its_lease_drops() {
        let slot = Arc::new(ConnectionSlot::default());
        let first = slot.claim(true).unwrap();
        assert!(slot.claim(false).is_none());
        assert!(!slot.wait_until_free(Duration::from_millis(10)).await);
        let waiter = tokio::spawn({
            let slot = slot.clone();
            async move { slot.wait_until_free(Duration::from_secs(5)).await }
        });
        tokio::task::yield_now().await;
        drop(first);
        assert!(waiter.await.unwrap());
        let second = slot.claim(false).unwrap();
        assert_eq!(second.generation, 2);
    }

    #[test]
    fn only_an_http_connection_without_open_streams_goes_idle() {
        let slot = Arc::new(ConnectionSlot::default());
        let idle = Duration::from_secs(30);
        let later = || Instant::now() + idle;
        let lease = slot.claim(true).unwrap();
        slot.bind(lease.generation, "conn-1");
        let stream = slot.open_stream("conn-1").unwrap();
        assert!(slot.open_stream("conn-2").is_none());
        assert!(!slot.close_idle(later(), idle), "a stream is open");
        drop(stream);
        assert!(
            !slot.close_idle(Instant::now(), idle),
            "not idle long enough"
        );
        assert!(slot.close_idle(later(), idle));
        assert!(lease.closed.is_cancelled());
        assert!(!slot.close_idle(later(), idle), "already closing");
        drop(lease);

        let unbound = slot.claim(true).unwrap();
        assert!(
            slot.close_idle(later(), idle),
            "an HTTP connection whose initialize response never arrived"
        );
        drop(unbound);

        let websocket = slot.claim(false).unwrap();
        assert!(!slot.close_idle(later(), idle));
        assert!(!websocket.closed.is_cancelled());
    }

    #[tokio::test]
    async fn a_busy_server_refuses_initialize_with_an_error() {
        use agent_client_protocol::{Channel, TransportFrame};

        let (mut client, server) = Channel::duplex();
        let agent = tokio::spawn(busy_agent().connect_to(server));
        client
            .tx
            .unbounded_send(TransportFrame::parse_json(
                r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":1}}"#,
            ))
            .unwrap();
        let reply = tokio::time::timeout(Duration::from_secs(5), client.rx.next())
            .await
            .unwrap()
            .unwrap();
        let reply: Value = serde_json::from_str(&reply.to_json().unwrap()).unwrap();
        assert_eq!(reply["id"], 1, "{reply}");
        assert_eq!(reply["error"]["code"], -32600, "{reply}");
        assert_eq!(reply["error"]["data"], BUSY, "{reply}");
        drop(client);
        let _ = agent.await;
    }
}
