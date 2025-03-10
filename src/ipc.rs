use bytes::{Buf, BufMut, BytesMut};
use std::{
    io::{Read, Write},
    net::Shutdown,
    os::unix::net::UnixStream,
    path::Path,
    thread::JoinHandle,
};

use crate::{connection::Connection, errors::ConnectionError};

/// Indicates closing of the IPC stream
const EOF: usize = 0;

pub(crate) type IpcParallelRW = (
    JoinHandle<Result<(), ConnectionError>>,
    JoinHandle<Result<(), ConnectionError>>,
);

pub(crate) struct Ipc<T> {
    connection: T,
    stream: UnixStream,
}

impl<T> Ipc<T>
where
    T: Connection + Send + Clone + 'static,
{
    pub(crate) fn try_start(path: &Path, connection: T) -> Result<IpcParallelRW, ConnectionError> {
        let ipc = Self::try_connect(path, connection)?;
        ipc.start()
    }

    pub(crate) fn try_connect(path: &Path, connection: T) -> Result<Self, ConnectionError> {
        let stream = UnixStream::connect(path)?;

        Ok(Self { stream, connection })
    }

    pub(crate) fn start(self) -> Result<IpcParallelRW, ConnectionError> {
        // 32MB
        const INTERNAL_READ_BUF_CAPACITY: usize = 4 * 1024 * 1024;

        let (mut ipc_writer, mut ipc_reader) = (self.stream.try_clone()?, self.stream);
        let (connection_w, connection_r) = (self.connection.clone(), self.connection);

        //Inspired by  alloy.rs async transport IPC implementation
        //https://github.com/alloy-rs/alloy/blob/main/crates/transport-ipc/src/lib.rs
        let read_jh = std::thread::spawn(move || -> Result<(), ConnectionError> {
            let mut buf = BytesMut::with_capacity(INTERNAL_READ_BUF_CAPACITY);

            //prety much the same way poll_read_buff in tokio is implemented
            //https://docs.rs/tokio-util/latest/tokio_util/io/fn.poll_read_buf.html
            let reader_result = 'reader: loop {
                let dst = buf.chunk_mut();
                let dst = unsafe { std::slice::from_raw_parts_mut(dst.as_mut_ptr(), dst.len()) };

                // Read data from the IPC reader into the spare capacity.
                let n = ipc_reader.read(dst)?;
                if n == EOF {
                    break 'reader Ok(());
                }

                unsafe {
                    // Mark the newly read bytes as initialized.
                    buf.advance_mut(n);
                }

                // Try to deserialize as many complete messages as possible.
                'deserializer: loop {
                    if buf.is_empty() {
                        break 'deserializer; // Nothing left to process go fetch more bytes
                    }

                    let mut de = serde_json::Deserializer::from_slice(&buf)
                        .into_iter::<alloy_json_rpc::Response>();

                    match de.next() {
                        Some(Ok(response)) => {
                            connection_r.recv(Some(response))?;

                            // Remove the consumed bytes from the buffer.
                            let consumed = de.byte_offset();
                            buf.advance(consumed);
                        }
                        Some(Err(err)) => {
                            // Check if the error is recoverable (likely due to incomplete data).
                            let is_recoverable = err.is_eof() || err.is_data();
                            if is_recoverable {
                                // we errored on this read, so these bytes were part of malformed
                                // JOSN, consume them
                                let consumed = de.byte_offset();
                                buf.advance(consumed);
                                break 'deserializer;
                            } else {
                                break 'reader Err(ConnectionError::from(err));
                            }
                        }
                        None => {
                            // No complete messages found, go and fetch more bytes
                            break 'deserializer;
                        }
                    }
                }
            };

            //let the manager threads know server exited
            connection_r.recv(None)?;

            // The intention of this lib is to mimic request - response pattern
            // If we cannot receive any more responses, we close IPC completely
            // Will error if socket is no longer (or never was) connected, we don't care
            let _ = ipc_reader.shutdown(Shutdown::Both);
            reader_result
        });

        let write_jh = std::thread::spawn(move || -> Result<(), ConnectionError> {
            while let Ok(Some(msg)) = connection_w.send() {
                ipc_writer.write_all(&msg)?;
            }

            // The intention of this lib is to mimic request - response pattern
            // If we cannot send any more requests, we close IPC completely
            // Will error if socket is no longer(or never was) connected, we don't care
            let _ = ipc_writer.shutdown(Shutdown::Both);

            Ok(())
        });

        Ok((read_jh, write_jh))
    }
}

#[cfg(test)]
mod tests {
    use crate::errors::ConnectionError;

    use super::*;
    use alloy_json_rpc::Response;
    use bytes::{Bytes, BytesMut};
    use pretty_assertions::assert_eq;
    use serde_json::json;
    use std::io::Read;
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::thread;
    use tempfile::tempdir;

    #[derive(Clone)]
    struct MockConnection {
        to_send: crossbeam::channel::Receiver<Bytes>,
        to_recv: crossbeam::channel::Sender<Response>,
    }

    impl MockConnection {
        fn new(
            to_send: crossbeam::channel::Receiver<Bytes>,
            to_recv: crossbeam::channel::Sender<Response>,
        ) -> Self {
            Self { to_send, to_recv }
        }
    }

    impl Connection for MockConnection {
        fn send(&self) -> Result<Option<Bytes>, ConnectionError> {
            let b = self.to_send.recv()?;
            Ok(Some(b))
        }

        fn recv(&self, r: Option<Response>) -> Result<(), ConnectionError> {
            if let Some(r) = r {
                self.to_recv.send(r)?;
            }
            Ok(())
        }
    }

    #[test]
    fn test_ipc_send() -> Result<(), Box<dyn std::error::Error>> {
        // Crates temp socket, deleted after test
        let dir = tempdir()?;
        let socket_path = dir.path().join("test_socket");

        //channels for server to communicate results
        let (server_tx, server_rx) = crossbeam::channel::unbounded();
        let server_thread = spawn_test_server(socket_path.clone(), server_tx);

        let (send_to_ipc, send_to_ipc_rx) = crossbeam::channel::unbounded();
        let (recv_from_ipc_tx, recv_from_ipc) = crossbeam::channel::unbounded();

        let ipc = Ipc::try_connect(
            socket_path.as_path(),
            MockConnection::new(send_to_ipc_rx, recv_from_ipc_tx),
        )?;
        let (ipc_r_jh, ipc_w_jh) = ipc.start()?;

        send_to_ipc.send(Bytes::from_static(b"ping_1"))?;
        assert_eq!(server_rx.recv()?, Bytes::from_static(b"ping_1"));
        assert_eq!(
            serde_json::to_string_pretty(&recv_from_ipc.recv()?)?,
            serde_json::to_string_pretty(&make_resp(1))?
        );

        send_to_ipc.send(Bytes::from_static(b"ping_2"))?;
        assert_eq!(server_rx.recv()?, Bytes::from_static(b"ping_2"));
        assert_eq!(
            serde_json::to_string_pretty(&recv_from_ipc.recv()?)?,
            serde_json::to_string_pretty(&make_resp(2))?
        );

        // closes communication channels, effectively closing IPC connection
        // unless this is done, test hangs because server thread doesn't exit
        drop(send_to_ipc);

        ipc_r_jh.join().unwrap()?;
        ipc_w_jh.join().unwrap()?;
        server_thread.join().unwrap()?;

        Ok(())
    }

    fn spawn_test_server(
        socket_path: PathBuf,
        tx: crossbeam::channel::Sender<Bytes>,
    ) -> thread::JoinHandle<Result<(), ConnectionError>> {
        let server_thread = thread::spawn(move || -> Result<(), ConnectionError> {
            let listener = UnixListener::bind(&socket_path)?;
            let mut stream = listener.incoming().next().unwrap()?;

            let mut buf = BytesMut::zeroed(1024);
            let mut msg_count = 0;
            while let Ok(n) = stream.read(&mut buf) {
                msg_count += 1;
                if n == EOF {
                    break;
                }

                tx.send(buf.split_to(n).freeze()).unwrap();
                let b = serde_json::to_vec(&make_resp(msg_count))?;
                stream.write_all(&b)?;
            }

            Ok(())
        });

        // Give the server a moment to start up.
        thread::sleep(std::time::Duration::from_millis(50));
        server_thread
    }

    fn make_resp(id: usize) -> Response {
        let response = json!({
            "jsonrpc": "2.0",
            "result": "pong",
            "id": id
        })
        .to_string();

        serde_json::from_str(&response).unwrap()
    }
}
