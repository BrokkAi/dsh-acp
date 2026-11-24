/**
 * DSH runtime surface used by the Rust ACP frontend.
 *
 * This extends the official SDK runtime rather than reimplementing it. The SDK
 * server still owns prompts, approvals, user questions, model execution, and
 * durable session events. The additions expose the controls an ACP server
 * needs but the SDK intentionally omits: explicit session open/close, list,
 * cancellation, model/mode changes, commands, snapshots, and live stream
 * frames.
 */

import { createRequire } from 'node:module'
import { rm } from 'node:fs/promises'
import path from 'node:path'
import { pathToFileURL } from 'node:url'

export const name = 'dsh-acp-runtime'
export const inject = ['agents', 'workingDirectory']

const runtimeModules = process.env.DSH_ACP_RUNTIME_MODULES
const require = createRequire(
  runtimeModules === undefined
    ? import.meta.url
    : path.join(runtimeModules, '.dsh-acp-runtime-loader.cjs'),
)

const loadPackage = async (specifier) =>
  import(pathToFileURL(require.resolve(specifier)).href)

const [
  { default: Schema },
  { createUserMessage },
  { JsonRpcLineTransport },
  { HarnessSdkJsonRpcServer },
  McpClient,
  { buildForkSeed },
] =
  await Promise.all([
    loadPackage('@deepseek-ai/schemastery'),
    loadPackage('@deepseek-ai/dsh-llm'),
    loadPackage('@deepseek-ai/dsh-sdk-protocol'),
    loadPackage('@deepseek-ai/dsh-sdk-jsonrpc-server'),
    loadPackage('@deepseek-ai/dsh-mcp-client'),
    loadPackage('@deepseek-ai/dsh-session/fork'),
  ])

export const Config = Schema.object({})

function protocolVersion() {
  return require('@deepseek-ai/dsh/package.json').version
}

function sessionId(value) {
  if (typeof value !== 'string' || value.length === 0) {
    throw new TypeError('a non-empty sessionId is required')
  }
  return value
}

function modelSelection(params = {}) {
  return {
    provider: params.provider ?? 'deepseek-official',
    model: params.model ?? 'deepseek-v4-flash',
    ...params.reasoningEffort === undefined ? {} : { reasoningEffort: params.reasoningEffort },
    ...params.maxTokens === undefined ? {} : { maxTokens: params.maxTokens },
  }
}

function permissionPreset(mode) {
  return {
    ask: 'workspace-write',
    allow: 'danger-full-access',
    deny: 'read-only',
  }[mode]
}

function mcpConfig(server, cwd) {
  if (server === null || typeof server !== 'object' || typeof server.name !== 'string') {
    throw new TypeError('each MCP server requires a name')
  }
  const env = Object.fromEntries(
    (server.env ?? []).map((entry) => [entry.name, entry.value]),
  )
  const headers = Object.fromEntries(
    (server.headers ?? []).map((entry) => [entry.name, entry.value]),
  )
  if (server.type === 'http') {
    return McpClient.Config({
      transport: 'streamable-http',
      serverName: server.name,
      url: server.url,
      headers,
      failOnStartupError: true,
    })
  }
  if (server.type !== undefined) {
    throw new TypeError(`unsupported MCP transport: ${String(server.type)}`)
  }
  return McpClient.Config({
    transport: 'stdio',
    serverName: server.name,
    command: server.command,
    args: server.args ?? [],
    env,
    cwd,
    failOnStartupError: true,
  })
}

export function apply(ctx) {
  const transport = new JsonRpcLineTransport(process.stdin, process.stdout)
  const server = new HarnessSdkJsonRpcServer(ctx, transport, { maxTokensAsSuccess: true })
  ctx.provide('sdkSessionActivation', { ready: true })
  let route = modelSelection()
  let initialized = false
  let initialization
  const mcpFibers = new Map()

  const ensureInitialized = async (cwd) => {
    if (initialized) return
    initialization ??= server.initialize({
      cwd,
      provider: route.provider,
      model: route.model,
      approvalRequests: true,
      capabilities: { userQuestions: true },
      ...route.reasoningEffort === undefined ? {} : { reasoningEffort: route.reasoningEffort },
      ...route.maxTokens === undefined ? {} : { maxTokens: route.maxTokens },
    }).then((result) => {
      initialized = true
      return result
    })
    await initialization
  }

  const record = (id) => server.sessions.get(id)
  const liveAgent = (id) => {
    const rec = record(id)
    if (rec === undefined) throw new Error(`unknown session: ${id}`)
    server.assertLiveAgent(rec, id)
    return rec.handle.agent
  }

  const selectModel = (agent, params) => {
    const selection = modelSelection(params)
    agent.model.select(selection)
    return selection
  }

  const configOptions = async (agent) => {
    const llm = ctx.get('llm')
    if (llm === undefined) return []
    const providers = await Promise.all(llm.listProviders().map(async (provider) => ({
      provider,
      models: await llm.listModels(provider.id),
    })))
    const current = agent.model.current()
    const options = providers.flatMap(({ provider, models }) => models.map((model) => ({
      value: JSON.stringify([provider.id, model.id]),
      name: model.name,
      description: model.description,
      group: provider.id,
      groupName: provider.name,
    })))
    const reasoning = []
    if (current !== undefined) {
      const info = await llm.resolveModelInfo(current.provider, current.model)
      for (const effort of info.reasoning?.efforts ?? []) {
        reasoning.push({ value: effort.id, name: effort.name })
      }
    }
    return [
      ...options.length === 0 ? [] : [{
        id: 'model',
        name: 'Model',
        category: 'model',
        currentValue: current === undefined ? undefined : JSON.stringify([current.provider, current.model]),
        options,
      }],
      ...reasoning.length === 0 ? [] : [{
        id: 'reasoning_effort',
        name: 'Reasoning effort',
        category: 'thought_level',
        currentValue: current?.reasoningEffort,
        options: reasoning,
      }],
    ]
  }

  ctx.on('agent/assistant-stream', ({ agent, frame }) => {
    if (server.sessions.get(agent.session.id)?.handle.agent !== agent) return
    transport.notify('dsh/assistant-stream', {
      sessionId: String(agent.session.id),
      frame,
    })
  })

  transport.onRequest(async (method, params) => {
    switch (method) {
      case 'dsh/hello':
        return {
          runtime: '@deepseek-ai/dsh',
          version: protocolVersion(),
          methods: [
            'dsh/session/open',
            'dsh/session/close',
            'dsh/session/delete',
            'dsh/session/list',
            'dsh/session/resume',
            'dsh/session/fork',
            'dsh/session/cancel',
            'dsh/session/steer',
            'dsh/session/set_config',
            'dsh/session/set_mode',
            'dsh/session/mcp',
            'dsh/session/set_title',
            'dsh/session/commands',
            'dsh/session/snapshot',
            'dsh/provider/set',
            'dsh/provider/disable',
          ],
        }
      case 'dsh/initialize':
        route = modelSelection(params)
        return { version: protocolVersion() }
      case 'dsh/session/open': {
        const id = sessionId(params?.sessionId)
        const cwd = params.cwd
        if (typeof cwd !== 'string' || cwd.length === 0) throw new TypeError('cwd is required')
        await ensureInitialized(cwd)
        const rec = await server.getOrCreateSession(id)
        server.assertLiveAgent(rec, id)
        await server.setWorkingDirectory({ sessionId: id, path: cwd })
        const agent = rec.handle.agent
        const selection = selectModel(agent, params)
        const preset = permissionPreset(params?.permissionMode)
        if (preset !== undefined) {
          const presets = ctx.get('permissionPresets')
          if (presets === undefined) throw new Error('permission presets are not composed in this DSH profile')
          presets.set(agent.session, preset)
        }
        return {
          sessionId: id,
          cwd,
          model: selection,
          configOptions: await configOptions(agent),
        }
      }
      case 'dsh/session/resume': {
        const id = sessionId(params?.sessionId)
        const cwd = params?.cwd
        if (typeof cwd !== 'string' || cwd.length === 0) throw new TypeError('cwd is required')
        await ensureInitialized(cwd)
        const stored = await ctx.get('sessionPersistence')?.stat(id)
        if (stored === undefined) throw new Error(`session is not resumable: ${id}`)
        if (stored.header.origin === 'subagent') throw new Error(`subagent session cannot be resumed directly: ${id}`)
        if (stored.header.cwd !== cwd) throw new Error(`session cwd does not match: ${cwd}`)
        await server.resume({ sessionId: id })
        const agent = liveAgent(id)
        const selection = selectModel(agent, params)
        const preset = permissionPreset(params?.permissionMode)
        if (preset !== undefined) {
          const presets = ctx.get('permissionPresets')
          if (presets === undefined) throw new Error('permission presets are not composed in this DSH profile')
          presets.set(agent.session, preset)
        }
        return { configOptions: await configOptions(agent), model: selection }
      }
      case 'dsh/session/fork': {
        const sourceId = sessionId(params?.sourceSessionId)
        const childId = sessionId(params?.childSessionId)
        const cwd = params?.cwd
        if (typeof cwd !== 'string' || cwd.length === 0) throw new TypeError('cwd is required')
        await ensureInitialized(cwd)
        const sourceWasLive = record(sourceId) !== undefined
        if (!sourceWasLive) await server.resume({ sessionId: sourceId })
        const source = liveAgent(sourceId)
        const events = source.session.snapshotEvents()
        const boundary = typeof params?.boundary === 'number' ? params.boundary : events.at(-1)?.seq
        const seed = boundary === undefined ? [] : buildForkSeed(events, boundary)
        const selection = modelSelection(params)
        const handle = await ctx.agents.create({
          sessionId: childId,
          meta: {
            cwd,
            parentSession: sourceId,
            isSeeded: boundary !== undefined,
          },
          ...boundary === undefined ? {} : {
            seed,
            inheritedEventCount: boundary + 1,
            parentSource: {
              sessionId: sourceId,
              endSeq: boundary + 1,
            },
          },
          agentOptions: selection,
        })
        if (!sourceWasLive) {
          const sourceRecord = record(sourceId)
          server.sessions.delete(sourceId)
          await sourceRecord?.handle.dispose()
        }
        server.sessions.set(childId, { handle, failure: undefined, disposal: undefined })
        const child = liveAgent(childId)
        selectModel(child, params)
        return {
          sessionId: childId,
          cwd,
          model: selection,
          configOptions: await configOptions(child),
        }
      }
      case 'dsh/session/close': {
        const id = sessionId(params?.sessionId)
        const rec = record(id)
        if (rec === undefined) return {}
        const fibers = mcpFibers.get(id) ?? []
        mcpFibers.delete(id)
        await Promise.allSettled(fibers.map((fiber) => fiber.dispose()))
        server.sessions.delete(id)
        server.sessionCreations.delete(id)
        await rec.handle.dispose()
        return {}
      }
      case 'dsh/session/delete': {
        const id = sessionId(params?.sessionId)
        const rec = record(id)
        const fibers = mcpFibers.get(id) ?? []
        mcpFibers.delete(id)
        await Promise.allSettled(fibers.map((fiber) => fiber.dispose()))
        if (rec !== undefined) {
          server.sessions.delete(id)
          server.sessionCreations.delete(id)
          await rec.handle.dispose()
        }
        const persistence = ctx.get('sessionPersistence')
        if (persistence === undefined) throw new Error('session persistence is not composed')
        const snapshot = await persistence.stat(id)
        if (snapshot === undefined) return {}
        const target = persistence.locate(snapshot.header)
        await rm(path.dirname(target.path), { recursive: true, force: true })
        return {}
      }
      case 'dsh/session/list': {
        const snapshots = await ctx.get('sessionPersistence')?.list() ?? []
        const cwd = typeof params?.cwd === 'string' ? params.cwd : undefined
        return {
          sessions: snapshots
            .filter((snapshot) => snapshot.header.origin !== 'subagent')
            .filter((snapshot) => cwd === undefined || snapshot.header.cwd === cwd)
            .map((snapshot) => ({
              sessionId: String(snapshot.header.id),
              cwd: snapshot.header.cwd,
              title: snapshot.title,
              createdAt: snapshot.header.createdAt,
              updatedAt: snapshot.header.updatedAt,
              parentSessionId: snapshot.header.parentSession === undefined
                ? undefined
                : String(snapshot.header.parentSession),
            })),
        }
      }
      case 'dsh/session/cancel': {
        const agent = liveAgent(sessionId(params?.sessionId))
        agent.cancel({ kind: 'user' })
        return {}
      }
      case 'dsh/session/steer': {
        const agent = liveAgent(sessionId(params?.sessionId))
        if (!Array.isArray(params?.contentBlocks) || params.contentBlocks.length === 0) {
          throw new TypeError('contentBlocks must be a non-empty array')
        }
        const message = createUserMessage({
          content: params.contentBlocks,
          source: { kind: 'user' },
        })
        agent.steer(message)
        return { messageId: message.id }
      }
      case 'dsh/session/set_config': {
        const agent = liveAgent(sessionId(params?.sessionId))
        let selection
        if (params?.model !== undefined || params?.provider !== undefined || params?.reasoningEffort !== undefined) {
          selection = selectModel(agent, params)
        }
        if (params?.permissionMode !== undefined) {
          const presets = ctx.get('permissionPresets')
          if (presets === undefined) throw new Error('permission presets are not composed in this DSH profile')
          const preset = permissionPreset(params.permissionMode)
          if (preset === undefined) throw new TypeError(`unknown permission mode: ${String(params.permissionMode)}`)
          presets.set(agent.session, preset)
        }
        return {
          ...selection === undefined ? {} : { model: selection },
          configOptions: await configOptions(agent),
        }
      }
      case 'dsh/session/set_mode': {
        const agent = liveAgent(sessionId(params?.sessionId))
        const planMode = ctx.get('planMode')
        if (planMode === undefined) throw new Error('plan mode is not composed in this DSH profile')
        if (params?.modeId !== 'agent' && params?.modeId !== 'plan') {
          throw new TypeError(`unknown session mode: ${String(params?.modeId)}`)
        }
        planMode.set(agent, params.modeId === 'plan')
        return { modeId: params.modeId }
      }
      case 'dsh/session/mcp': {
        const agent = liveAgent(sessionId(params?.sessionId))
        const previous = mcpFibers.get(agent.session.id) ?? []
        mcpFibers.delete(agent.session.id)
        await Promise.allSettled(previous.map((fiber) => fiber.dispose()))
        if (!Array.isArray(params?.servers) || params.servers.length === 0) return {}
        const fibers = []
        try {
          for (const server of params.servers) {
            fibers.push(await agent.ctx.plugin(McpClient, mcpConfig(server, agent.session.header.cwd)))
          }
        } catch (error) {
          await Promise.allSettled(fibers.map((fiber) => fiber.dispose()))
          throw error
        }
        mcpFibers.set(agent.session.id, fibers)
        return {}
      }
      case 'dsh/session/set_title': {
        const id = sessionId(params?.sessionId)
        if (typeof params?.title !== 'string') throw new TypeError('title must be a string')
        const title = ctx.get('sessionTitle')
        if (title === undefined) throw new Error('session titles are not composed in this DSH profile')
        const live = record(id)
        if (live !== undefined) {
          return { title: title.rename(live.handle.agent.session, params.title).title }
        }
        await server.resume({ sessionId: id })
        const restored = liveAgent(id)
        try {
          return { title: title.rename(restored.session, params.title).title }
        } finally {
          const rec = record(id)
          server.sessions.delete(id)
          await rec?.handle.dispose()
        }
      }
      case 'dsh/session/commands': {
        const agent = liveAgent(sessionId(params?.sessionId))
        const commands = ctx.get('commands')
        return {
          commands: commands?.list(agent).map((command) => ({
            name: command.name,
            description: command.description,
            input: command.input,
          })) ?? [],
        }
      }
      case 'dsh/session/snapshot': {
        const id = sessionId(params?.sessionId)
        const live = record(id)
        if (live !== undefined) {
          return { events: live.handle.agent.session.snapshotEvents() }
        }
        const persistence = ctx.get('sessionPersistence')
        if (persistence === undefined) throw new Error('session persistence is not composed')
        const handle = await persistence.open(id, 'read')
        try {
          return { events: (await handle.read()).events }
        } finally {
          await handle.close()
        }
      }
      case 'dsh/provider/set': {
        if (typeof params?.baseUrl !== 'string') throw new TypeError('baseUrl is required')
        process.env.DEEPSEEK_BASE_URL = params.baseUrl
        const headers = Array.isArray(params.headers)
          ? params.headers
          : Object.entries(params.headers ?? {}).map(([name, value]) => ({ name, value }))
        const authorization = headers.find(
          (entry) => entry.name.toLowerCase() === 'authorization',
        )
        if (authorization !== undefined) {
          const value = authorization.value.trim()
          const separator = value.indexOf(' ')
          process.env.DEEPSEEK_API_KEY = separator === -1
            ? value
            : value.slice(separator + 1).trim()
        }
        return {}
      }
      case 'dsh/provider/disable':
        delete process.env.DEEPSEEK_API_KEY
        return {}
      default:
        return server.handleRequest(method, params)
    }
  })

  ctx.effect(() => {
    transport.start()
    return async () => {
      await server.shutdown()
      transport.close()
    }
  }, 'dsh-acp-runtime.serve')
}
