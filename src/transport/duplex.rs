//! In-memory duplex byte streams for the upcoming `inproc://` transport.
//!
//! Each call to [`duplex_pair`] returns two ends of a linked pipe: bytes written
//! on one end are readable on the other (and vice versa). The streams implement
//! [`futures::AsyncRead`] / [`futures::AsyncWrite`] so they can be wrapped in
//! [`FramedIo`] without an OS socket.

use futures::{AsyncRead, AsyncWrite};

use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use parking_lot::Mutex;

/// One direction of buffered bytes between two [`DuplexStream`] ends.
#[derive(Default)]
struct Pipe {
    buffer: VecDeque<u8>,
    /// True once the writer end has been closed or dropped.
    writer_closed: bool,
    /// True once the reader end has been closed or dropped.
    reader_closed: bool,
    reader_waker: Option<Waker>,
}

impl Pipe {
    fn wake_reader(&mut self) {
        if let Some(waker) = self.reader_waker.take() {
            waker.wake();
        }
    }
}

/// One end of an in-memory duplex connection.
// Wired up by the inproc transport follow-up.
#[allow(dead_code)]
pub(crate) struct DuplexStream {
    incoming: Arc<Mutex<Pipe>>,
    outgoing: Arc<Mutex<Pipe>>,
}

/// Creates a linked pair of duplex streams.
///
/// Bytes written to the first stream are readable from the second, and vice versa.
// Wired up by the inproc transport follow-up.
#[allow(dead_code)]
pub(crate) fn duplex_pair() -> (DuplexStream, DuplexStream) {
    let a_to_b = Arc::new(Mutex::new(Pipe::default()));
    let b_to_a = Arc::new(Mutex::new(Pipe::default()));

    let a = DuplexStream {
        incoming: Arc::clone(&b_to_a),
        outgoing: Arc::clone(&a_to_b),
    };
    let b = DuplexStream {
        incoming: a_to_b,
        outgoing: b_to_a,
    };
    (a, b)
}

impl Drop for DuplexStream {
    fn drop(&mut self) {
        {
            let mut outgoing = self.outgoing.lock();
            outgoing.writer_closed = true;
            outgoing.wake_reader();
        }
        {
            let mut incoming = self.incoming.lock();
            incoming.reader_closed = true;
        }
    }
}

impl AsyncRead for DuplexStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let mut incoming = self.incoming.lock();

        if !incoming.buffer.is_empty() {
            let n = incoming.buffer.len().min(buf.len());
            for (dst, src) in buf[..n].iter_mut().zip(incoming.buffer.drain(..n)) {
                *dst = src;
            }
            return Poll::Ready(Ok(n));
        }

        if incoming.writer_closed {
            return Poll::Ready(Ok(0));
        }

        incoming.reader_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for DuplexStream {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }

        let mut outgoing = self.outgoing.lock();
        if outgoing.reader_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "inproc peer closed",
            )));
        }

        outgoing.buffer.extend(buf.iter().copied());
        outgoing.wake_reader();
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut outgoing = self.outgoing.lock();
        outgoing.writer_closed = true;
        outgoing.wake_reader();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_rt;
    use futures::{AsyncReadExt, AsyncWriteExt};

    #[async_rt::test]
    async fn bytes_cross_from_a_to_b() {
        let (mut a, mut b) = duplex_pair();
        a.write_all(b"hello").await.unwrap();
        a.flush().await.unwrap();

        let mut buf = [0u8; 5];
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[async_rt::test]
    async fn bytes_cross_both_directions() {
        let (mut a, mut b) = duplex_pair();

        a.write_all(b"ping").await.unwrap();
        b.write_all(b"pong").await.unwrap();

        let mut from_a = [0u8; 4];
        let mut from_b = [0u8; 4];
        b.read_exact(&mut from_a).await.unwrap();
        a.read_exact(&mut from_b).await.unwrap();
        assert_eq!(&from_a, b"ping");
        assert_eq!(&from_b, b"pong");
    }

    #[async_rt::test]
    async fn read_returns_eof_after_peer_drop() {
        let (mut a, b) = duplex_pair();
        drop(b);

        let mut buf = [0u8; 4];
        let n = a.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }

    #[async_rt::test]
    async fn write_fails_after_peer_drop() {
        let (mut a, b) = duplex_pair();
        drop(b);

        let err = a.write_all(b"nope").await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[async_rt::test]
    async fn close_signals_eof_to_peer() {
        let (mut a, mut b) = duplex_pair();
        a.write_all(b"x").await.unwrap();
        a.close().await.unwrap();

        let mut buf = [0u8; 8];
        let n = b.read(&mut buf).await.unwrap();
        assert_eq!(n, 1);
        assert_eq!(buf[0], b'x');

        let n = b.read(&mut buf).await.unwrap();
        assert_eq!(n, 0);
    }
}
