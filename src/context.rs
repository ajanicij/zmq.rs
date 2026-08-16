//! Process-local shared state for transports that need rendezvous.
//!
//! TCP and IPC sockets do not require a [`Context`]. Future `inproc://` support
//! will use one shared [`Context`] so bind and connect can find each other by name.

use crate::{ZmqError, ZmqResult};

use parking_lot::Mutex;

use std::collections::HashSet;
use std::sync::Arc;

/// Shared context for in-process endpoint rendezvous.
///
/// Cloning a `Context` shares the same registry (`Arc`). Sockets that use
/// `inproc://` must be created against the same `Context` instance (or clones of it).
#[derive(Clone, Debug, Default)]
pub struct Context {
    inner: Arc<ContextInner>,
}

#[derive(Debug, Default)]
struct ContextInner {
    /// Bound inproc endpoint names.
    ///
    /// Later PRs will attach accept queues / pending connects when duplex I/O
    /// is wired up; for now this is only a name reservation table.
    inproc: Mutex<HashSet<String>>,
}

impl Context {
    /// Creates a new empty context.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `name` as a bound inproc endpoint in this context.
    ///
    /// # Errors
    /// - Empty names are rejected.
    /// - Returns an error if `name` is already bound (libzmq `EADDRINUSE` analogue).
    // Wired up by the inproc transport follow-up.
    #[allow(dead_code)]
    pub(crate) fn register_inproc(&self, name: &str) -> ZmqResult<()> {
        if name.is_empty() {
            return Err(ZmqError::Socket("inproc endpoint name must not be empty"));
        }
        let mut set = self.inner.inproc.lock();
        if !set.insert(name.to_string()) {
            return Err(ZmqError::Socket("Address already in use"));
        }
        Ok(())
    }

    /// Removes a previously bound inproc endpoint name.
    ///
    /// # Errors
    /// Returns an error if `name` is not currently bound in this context.
    // Wired up by the inproc transport follow-up.
    #[allow(dead_code)]
    pub(crate) fn unregister_inproc(&self, name: &str) -> ZmqResult<()> {
        let mut set = self.inner.inproc.lock();
        if !set.remove(name) {
            return Err(ZmqError::Socket("No such inproc endpoint"));
        }
        Ok(())
    }

    /// Returns whether `name` is currently bound in this context.
    // Wired up by the inproc transport follow-up.
    #[allow(dead_code)]
    pub(crate) fn inproc_is_bound(&self, name: &str) -> bool {
        self.inner.inproc.lock().contains(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_unregister_inproc_name() {
        let ctx = Context::new();
        assert!(!ctx.inproc_is_bound("step2"));

        ctx.register_inproc("step2").unwrap();
        assert!(ctx.inproc_is_bound("step2"));

        ctx.unregister_inproc("step2").unwrap();
        assert!(!ctx.inproc_is_bound("step2"));
    }

    #[test]
    fn clones_share_the_same_registry() {
        let ctx = Context::new();
        let clone = ctx.clone();

        ctx.register_inproc("shared").unwrap();
        assert!(clone.inproc_is_bound("shared"));

        clone.unregister_inproc("shared").unwrap();
        assert!(!ctx.inproc_is_bound("shared"));
    }

    #[test]
    fn double_bind_is_rejected() {
        let ctx = Context::new();
        ctx.register_inproc("dup").unwrap();
        let err = ctx.register_inproc("dup").unwrap_err();
        assert!(matches!(err, ZmqError::Socket("Address already in use")));
    }

    #[test]
    fn empty_name_is_rejected() {
        let ctx = Context::new();
        let err = ctx.register_inproc("").unwrap_err();
        assert!(matches!(
            err,
            ZmqError::Socket("inproc endpoint name must not be empty")
        ));
    }

    #[test]
    fn unregister_missing_name_errors() {
        let ctx = Context::new();
        let err = ctx.unregister_inproc("missing").unwrap_err();
        assert!(matches!(err, ZmqError::Socket("No such inproc endpoint")));
    }

    #[test]
    fn separate_contexts_do_not_share_names() {
        let a = Context::new();
        let b = Context::new();
        a.register_inproc("only-a").unwrap();
        assert!(!b.inproc_is_bound("only-a"));
        b.register_inproc("only-a").unwrap();
    }
}
