use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::time::{Duration, Instant};
use std::fs::OpenOptions;
use std::io::{BufWriter, Write};

pub struct WalWriter {
    sender: Sender<String>,
}

pub struct WalWriterHandle {
    receiver: Receiver<String>,
    batch_interval: Duration,
}

impl WalWriter {
    // Returns a WalWriter and its associated handle.
    pub fn new(batch_interval: Duration) -> (Self, WalWriterHandle) {
        let (sender, receiver) = channel();
        (
            WalWriter { sender },
            WalWriterHandle { receiver, batch_interval },
        )
    }

    pub fn log(&self, op: String) {
        let _ = self.sender.send(op);
    }
}

impl WalWriterHandle {
    pub fn start(self, wal_file: String) {
        thread::spawn(move || {
            let mut buffer = Vec::new();
            let mut last_flush = Instant::now();
            loop {
                // Try to receive new WAL operations until the batch_interval or a batch size threshold is met.
                match self.receiver.recv_timeout(self.batch_interval) {
                    Ok(op) => buffer.push(op),
                    Err(_) => {
                        // Timeout expired: time to flush the current batch.
                    },
                }

                if last_flush.elapsed() >= self.batch_interval || buffer.len() >= 10 {
                    if !buffer.is_empty() {
                        let file = OpenOptions::new()
                            .append(true)
                            .create(true)
                            .open(&wal_file);
                        if let Ok(file) = file {
                            let mut writer = BufWriter::new(file);
                            for op in &buffer {
                                if writeln!(writer, "{}", op).is_err() {
                                    eprintln!("Error writing to WAL file.");
                                }
                            }
                            let _ = writer.flush();
                        } else {
                            eprintln!("Could not open WAL file: {}", wal_file);
                        }
                        buffer.clear();
                        last_flush = Instant::now();
                    }
                }
            }
        });
    }
}