//! Cross-process single-writer lock over one workspace `work.db`
//! (CP-08-106 slice A). Every authoritative M1-family writer — desktop
//! commands, CLI serve RPCs, one-shot `work` commands — reaches the database
//! through [`crate::WorkStore::open`], so acquiring the lock there covers all
//! three doors structurally.
//!
//! WHY an advisory OS lock instead of a sentinel-file protocol: a crashed
//! holder must release automatically, which means the lock's lifetime has to
//! be owned by the kernel, not by file contents. `flock` (Unix) and
//! `LockFileEx` (Windows) tie exclusivity to an open file handle, so process
//! death closes the handle and the lock evaporates with it. There is no
//! stale-lock policy because there is no way to become stale — no timestamp
//! check, PID probe, or recovery sweep can exist or is needed.
//!
//! WHY the lock file is never deleted or truncated: deleting it while
//! another opener races through create+lock would let two processes hold
//! locks on different inodes, each believing it won. Leaving the file in
//! place keeps every contender on the same inode forever; the file carries
//! no state, so its contents are irrelevant.
//!
//! Same-process double-open also fails closed: each open creates its own
//! file description/handle, and both mechanisms arbitrate between
//! descriptions, not between processes.

use std::fs::{File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};

/// A held advisory lock over one workspace database. Dropping it closes the
/// handle and the kernel releases the lock.
#[derive(Debug)]
pub struct WorkspaceFileLock {
    _file: File,
}

#[derive(Debug)]
pub enum WorkspaceLockAcquireError {
    /// Another live opener — in any process, including this one — already
    /// holds the workspace's single-writer lock.
    Held,
    Io(io::Error),
}

impl WorkspaceFileLock {
    /// Acquire the exclusive advisory lock beside `database_path`
    /// (`<database file>.lock`, created if missing). Fails immediately
    /// instead of waiting: a second writer must fail closed, not queue
    /// behind a holder it cannot observe.
    pub fn acquire(database_path: &Path) -> Result<Self, WorkspaceLockAcquireError> {
        let lock_path = lock_path_for(database_path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(WorkspaceLockAcquireError::Io)?;
        try_lock_exclusive(&file)?;
        Ok(Self { _file: file })
    }
}

/// The advisory lock file for a database: a sibling of the database file, so
/// the default workspace layout puts it at `<workspace>/.altai/work.db.lock`.
pub fn lock_path_for(database_path: &Path) -> PathBuf {
    let mut name = database_path.file_name().unwrap_or_default().to_os_string();
    name.push(".lock");
    database_path.with_file_name(name)
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> Result<(), WorkspaceLockAcquireError> {
    use std::os::unix::io::AsRawFd;

    // LOCK_NB keeps contention fail-closed; EWOULDBLOCK/EAGAIN mean held.
    let result = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN => {
            Err(WorkspaceLockAcquireError::Held)
        }
        _ => Err(WorkspaceLockAcquireError::Io(error)),
    }
}

#[cfg(windows)]
fn try_lock_exclusive(file: &File) -> Result<(), WorkspaceLockAcquireError> {
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    // One locked byte at offset zero is enough: exclusivity is what matters,
    // not the range. A zeroed OVERLAPPED addresses the start of the file.
    let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
    let locked = unsafe {
        LockFileEx(
            file.as_raw_handle(),
            LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
            0,
            1,
            0,
            &mut overlapped,
        )
    };
    if locked != 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(code) if code == ERROR_LOCK_VIOLATION as i32 => Err(WorkspaceLockAcquireError::Held),
        _ => Err(WorkspaceLockAcquireError::Io(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lock_path_sits_beside_the_database_file() {
        let database = std::path::Path::new("/tmp/workspace/.altai/work.db");
        assert_eq!(
            lock_path_for(database),
            std::path::PathBuf::from("/tmp/workspace/.altai/work.db.lock")
        );
    }
}
