use std::{
    fmt, io,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use agent_client_protocol::Error;
use cap_std::fs::{Dir, File, OpenOptions};

/// Capability handles for the session's workspace roots. The handles remain
/// anchored to the directories opened for the session even if a local process
/// later renames or replaces one of their paths.
#[derive(Clone)]
pub struct WorkspaceRoots {
    roots: Arc<[WorkspaceRoot]>,
}

struct WorkspaceRoot {
    path: PathBuf,
    dir: Dir,
}

impl fmt::Debug for WorkspaceRoots {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceRoots")
            .field(
                "roots",
                &self.roots.iter().map(|root| &root.path).collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// A request path resolved against one stable workspace root.
#[derive(Clone, Debug)]
pub struct WorkspacePath {
    pub(crate) root_index: usize,
    pub(crate) relative: PathBuf,
    pub(crate) absolute: PathBuf,
}

impl WorkspaceRoots {
    pub fn open(cwd: &Path, additional: &[PathBuf]) -> Result<Self, Error> {
        let cwd = canonical_root(cwd, "session cwd", false)?;
        let mut roots = Vec::with_capacity(1 + additional.len());
        roots.push(open_root_handle(cwd)?);
        for path in additional {
            let path = canonical_root(path, "additional directory", false)?;
            roots.push(open_root_handle(path)?);
        }
        Ok(Self {
            roots: Arc::from(roots),
        })
    }

    /// Open roots already stored in canonical form. They must still resolve to
    /// the same path, so replacing a stored root path with a symlink cannot
    /// move an existing session's boundary when it is loaded again.
    pub fn open_stored(cwd: &Path, additional: &[PathBuf]) -> Result<Self, Error> {
        let cwd = canonical_root(cwd, "stored session cwd", true)?;
        let mut roots = Vec::with_capacity(1 + additional.len());
        roots.push(open_root_handle(cwd)?);
        for path in additional {
            let path = canonical_root(path, "stored additional directory", true)?;
            roots.push(open_root_handle(path)?);
        }
        Ok(Self {
            roots: Arc::from(roots),
        })
    }

    pub fn cwd(&self) -> &Path {
        &self.roots[0].path
    }

    pub fn additional(&self) -> impl Iterator<Item = &Path> {
        self.roots.iter().skip(1).map(|root| root.path.as_path())
    }

    /// Resolve an absolute or cwd-relative path to one stored root. `..` is
    /// removed lexically before the prefix check, so it cannot cross a root.
    pub fn resolve(&self, path: &str) -> Result<WorkspacePath, Error> {
        let path = Path::new(path);
        let candidate = if path.is_absolute() {
            path.to_path_buf()
        } else {
            self.cwd().join(path)
        };
        let absolute = normalize_lexically(&candidate);
        let (root_index, root) = self
            .roots
            .iter()
            .enumerate()
            .find(|(_, root)| absolute == root.path || absolute.starts_with(&root.path))
            .ok_or_else(|| {
                Error::invalid_params().data(format!(
                    "path {} is outside the session workspace roots",
                    absolute.display()
                ))
            })?;
        let relative = absolute
            .strip_prefix(&root.path)
            .map(Path::to_path_buf)
            .map_err(Error::into_internal_error)?;
        Ok(WorkspacePath {
            root_index,
            relative,
            absolute,
        })
    }

    pub fn open_existing(&self, path: &str) -> Result<(WorkspacePath, File), Error> {
        let path = self.resolve(path)?;
        let root = self.root(path.root_index);
        let file = root.dir.open(&path.relative).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                Error::resource_not_found(Some(path.absolute.display().to_string()))
            } else {
                Error::into_internal_error(error)
            }
        })?;
        reject_unsafe_file(&file).map_err(|message| Error::invalid_request().data(message))?;
        Ok((path, file))
    }

    /// Open a regular file without truncating it. Creation is relative to the
    /// root capability, and parent creation has the same confinement.
    pub fn open_writable(
        &self,
        path: &str,
        create_directories: bool,
    ) -> Result<(WorkspacePath, File), Error> {
        let path = self.resolve(path)?;
        let root = self.root(path.root_index);
        if create_directories
            && let Some(parent) = path.relative.parent()
            && !parent.as_os_str().is_empty()
        {
            root.dir
                .create_dir_all(parent)
                .map_err(Error::into_internal_error)?;
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        let file = root
            .dir
            .open_with(&path.relative, &options)
            .map_err(Error::into_internal_error)?;
        reject_unsafe_file(&file).map_err(|message| Error::invalid_request().data(message))?;
        Ok((path, file))
    }

    pub fn root_path(&self, root_index: usize) -> &Path {
        &self.roots[root_index].path
    }

    pub(crate) fn root_dir(&self, root_index: usize) -> &Dir {
        &self.roots[root_index].dir
    }

    fn root(&self, root_index: usize) -> &WorkspaceRoot {
        &self.roots[root_index]
    }
}

pub fn canonical_directory(path: &Path) -> Result<PathBuf, Error> {
    canonical_root(path, "workspace root", false)
}

fn canonical_root(path: &Path, label: &str, require_unchanged: bool) -> Result<PathBuf, Error> {
    if !path.is_absolute() {
        return Err(
            Error::invalid_params().data(format!("{label} must be absolute: {}", path.display()))
        );
    }
    let canonical = path.canonicalize().map_err(|error| {
        if error.kind() == io::ErrorKind::NotFound {
            Error::resource_not_found(Some(path.display().to_string()))
        } else {
            Error::into_internal_error(error)
        }
    })?;
    if require_unchanged && normalize_lexically(path) != canonical {
        return Err(Error::invalid_params().data(format!(
            "{label} {} changed through a symlink or non-canonical path",
            path.display()
        )));
    }
    if !canonical.is_dir() {
        return Err(Error::resource_not_found(Some(path.display().to_string())));
    }
    Ok(canonical)
}

fn open_root_handle(path: PathBuf) -> Result<WorkspaceRoot, Error> {
    #[cfg(unix)]
    let file = {
        use rustix::fs::{Mode, OFlags, open};
        let descriptor = open(
            &path,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(Error::into_internal_error)?;
        std::fs::File::from(descriptor)
    };
    #[cfg(not(unix))]
    let file = Dir::open_ambient_dir(&path, cap_std::ambient_authority())
        .map_err(Error::into_internal_error)?
        .into_std_file();
    Ok(WorkspaceRoot {
        path,
        dir: Dir::from_std_file(file),
    })
}

pub(crate) fn reject_unsafe_file(file: &File) -> Result<(), String> {
    let metadata = file
        .metadata()
        .map_err(|error| format!("failed to inspect opened file: {error}"))?;
    if !metadata.is_file() {
        return Err("workspace file is not a regular file".to_string());
    }
    if has_multiple_links(&metadata) {
        return Err(
            "workspace file has multiple hard links, so its other names cannot be confined"
                .to_string(),
        );
    }
    Ok(())
}

#[cfg(unix)]
fn has_multiple_links(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt;
    metadata.nlink() > 1
}

#[cfg(windows)]
fn has_multiple_links(metadata: &cap_std::fs::Metadata) -> bool {
    use cap_std::fs::MetadataExt;
    metadata
        .number_of_links()
        .is_none_or(|number_of_links| number_of_links > 1)
}

#[cfg(not(any(unix, windows)))]
fn has_multiple_links(_metadata: &cap_std::fs::Metadata) -> bool {
    false
}

pub(crate) fn normalize_lexically(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_may_not_escape_a_root_lexically() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        std::fs::create_dir(&workspace).unwrap();
        let roots = WorkspaceRoots::open(&workspace, &[]).unwrap();
        assert!(roots.resolve("inside/file").is_ok());
        assert!(roots.resolve("../outside").is_err());
        assert!(roots.resolve("/").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_root_symlink_replacement_does_not_change_the_open_capability() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(workspace.join("inside.txt"), "inside").unwrap();
        std::fs::write(outside.join("inside.txt"), "outside").unwrap();
        let roots = WorkspaceRoots::open(&workspace, &[]).unwrap();
        std::fs::rename(&workspace, temp.path().join("workspace-old")).unwrap();
        std::os::unix::fs::symlink(&outside, &workspace).unwrap();

        let (_, mut file) = roots.open_existing("inside.txt").unwrap();
        let mut contents = String::new();
        std::io::Read::read_to_string(&mut file, &mut contents).unwrap();
        assert_eq!(contents, "inside");
        assert!(WorkspaceRoots::open_stored(&workspace, &[]).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn an_ancestor_swap_cannot_redirect_a_read_or_write() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::create_dir(workspace.join("nested")).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(workspace.join("nested/file.txt"), "inside").unwrap();
        std::fs::write(outside.join("file.txt"), "outside").unwrap();
        let roots = WorkspaceRoots::open(&workspace, &[]).unwrap();
        std::fs::rename(workspace.join("nested"), workspace.join("nested-old")).unwrap();
        std::os::unix::fs::symlink(&outside, workspace.join("nested")).unwrap();

        assert!(roots.open_existing("nested/file.txt").is_err());
        assert!(roots.open_writable("nested/file.txt", false).is_err());
        assert_eq!(
            std::fs::read_to_string(outside.join("file.txt")).unwrap(),
            "outside"
        );
    }

    #[cfg(unix)]
    #[test]
    fn hard_linked_files_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let workspace = temp.path().join("workspace");
        let outside = temp.path().join("outside.txt");
        std::fs::create_dir(&workspace).unwrap();
        std::fs::write(&outside, "outside").unwrap();
        std::fs::hard_link(&outside, workspace.join("linked.txt")).unwrap();
        let roots = WorkspaceRoots::open(&workspace, &[]).unwrap();
        let error = roots.open_existing("linked.txt").unwrap_err();
        assert!(
            error
                .data
                .as_ref()
                .and_then(serde_json::Value::as_str)
                .is_some_and(|message| message.contains("multiple hard links"))
        );
        assert!(roots.open_writable("linked.txt", false).is_err());
    }
}
