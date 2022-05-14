//! The `bounded_streamer` module defines a set of services for safely & efficiently pushing & pulling packet batches between threads.
//!

use {
    crate::packet::PacketBatch,
    crossbeam_channel::{Receiver, TryRecvError, RecvError, RecvTimeoutError, SendError, Sender, TrySendError},
    std::{
        collections::VecDeque,
        result::Result,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

/// 10k batches means up to 1.3M packets, roughly 1.6GB of memory
pub const DEFAULT_MAX_QUEUED_BATCHES: usize = 10_000;

#[derive(Clone)]
pub struct BoundedPacketBatchReceiver {
    receiver: Receiver<PacketBatch>,
}

#[derive(Clone)]
pub struct BoundedPacketBatchSender {
    sender: Sender<PacketBatch>,
}

impl BoundedPacketBatchReceiver {
    /// Receives up to `max_packet_count` packets from the channel.
    ///
    /// If no data is available, the function will wait up to `timeout`, then
    /// return RecvTimeoutError::Timeout if no data came in.
    ///
    /// If all senders have been dropped and there's no data avilable, it returns
    /// RecvTimeoutError::Disconnected.
    ///
    /// Returns (list of batches, packet count)
    pub fn recv_timeout(
        &self,
        max_packet_count: usize,
        timeout: Duration,
    ) -> Result<(Vec<PacketBatch>, usize), RecvTimeoutError> {
        let first_batch = self.receiver.recv_timeout(timeout)?;
        Ok(self.greedy_receive(first_batch, max_packet_count))
    }

    /// Receives up to `max_packet_count` packets from the channel.
    ///
    /// Waits until there's data or the channel has been disconnected.
    ///
    /// Returns (vec-of-batches, packet count)
    pub fn recv(&self, max_packet_count: usize) -> Result<(Vec<PacketBatch>, usize), RecvError> {
        let first_batch = self.receiver.recv()?;
        Ok(self.greedy_receive(first_batch, max_packet_count))
    }

    fn greedy_receive(&self, first_batch: PacketBatch, max_packet_count: usize) -> (Vec<PacketBatch>, usize) {
        let mut packets = first_batch.packets.len();
        let mut batches = Vec::with_capacity(1 + self.receiver.len());
        batches.push(first_batch);
        while batches.len() < batches.capacity() && packets < max_packet_count {
            if let Ok(batch) = self.receiver.try_recv() {
                packets += batch.packets.len();
                batches.push(batch);
            } else {
                break;
            }
        }
        (batches, packets)
    }

    /// Like `recv_timeout()` with a 1s timeout
    pub fn recv_default_timeout(
        &self,
        max_packet_count: usize,
    ) -> Result<(Vec<PacketBatch>, usize), RecvTimeoutError> {
        self.recv_timeout(max_packet_count, Duration::new(1, 0))
    }

    /// Like `recv_default_timeout()` that also measures its own runtime.
    pub fn recv_duration_default_timeout(
        &self,
        max_packet_count: usize,
    ) -> Result<(Vec<PacketBatch>, usize, Duration), RecvTimeoutError> {
        let now = Instant::now();
        match self.recv_default_timeout(max_packet_count) {
            Ok((batches, packets)) => Ok((batches, packets, now.elapsed())),
            Err(err) => Err(err),
        }
    }

    /// Number of batches in the queue
    pub fn batch_count(&self) -> usize {
        self.receiver.len()
    }

    /// Number of packets in the queue
    pub fn packet_count(&self) -> usize {
        0
    }
}

impl BoundedPacketBatchSender {
    /// Sends a single batch.
    ///
    /// If the queue was full and an old batch needed to be discarded, it returns
    /// Ok(true), otherwise Ok(false).
    ///
    /// SendErrors happen when all receivers have been dropped.
    pub fn send_batch(&self, batch: PacketBatch) -> Result<bool, SendError<()>> {
        match self.sender.try_send(batch) {
            Ok(()) => Ok(false),
            Err(TrySendError::Full(_)) => Ok(true),
            Err(TrySendError::Disconnected(_)) => Err(SendError(())),
        }
    }

    /// Sends several batches.
    ///
    /// Returns the number of old batches that needed to be dropped to make space
    /// for the new.
    ///
    /// SendErrors happen when all receivers have been dropped.
    pub fn send_batches(
        &self,
        batches: Vec<PacketBatch>,
    ) -> std::result::Result<usize, SendError<()>> {
        let to_send = batches.len();
        for (i, batch) in batches.into_iter().enumerate() {
            if self.send_batch(batch)? {
                return Ok(to_send - i);
            }
        }
        Ok(0)
    }

    /// Number of batches in the queue
    pub fn batch_count(&self) -> usize {
        self.sender.len()
    }

    /// Number of packets in the queue
    pub fn packet_count(&self) -> usize {
        0
    }
}

/// Creates the sender and receiver for a channel of packet batches.
///
/// The channel is multi-producer multi-consumer.
///
/// It is bounded to hold at most `max_queued_batches`. If more batches are
/// pushed into the channel, the oldest queued batches will be discarded.
///
/// It also counts the number of packets that are available in the channel,
/// but is not aware of a Packet's meta.discard flag.
pub fn packet_batch_channel(
    max_queued_batches: usize,
) -> (BoundedPacketBatchSender, BoundedPacketBatchReceiver) {
    let (sender, receiver) = crossbeam_channel::bounded(max_queued_batches);
    let sender = BoundedPacketBatchSender {
        sender,
    };
    let receiver = BoundedPacketBatchReceiver {
        receiver,
    };
    (sender, receiver)
}

#[cfg(test)]
mod test {
    use {
        super::*,
        solana_perf::packet::{Packet, PacketBatch},
    };

    #[test]
    fn bounded_streamer_test() {
        let num_packets = 10;
        let packets_batch_size = 50;
        let max_batches = 10;
        let (sender, receiver) = packet_batch_channel(max_batches);

        let mut packet_batch = PacketBatch::default();
        for _ in 0..num_packets {
            let p = Packet::default();
            packet_batch.packets.push(p);
        }

        // Case 1: Send a single batch
        match sender.send_batch(packet_batch.clone()) {
            Ok(dropped_packet) => assert_eq!(dropped_packet, false),
            Err(_err) => (),
        }

        match receiver.recv(packets_batch_size) {
            Ok((_batches, packets)) => assert_eq!(packets, num_packets),
            Err(_err) => (),
        }

        // Case2: Fully load the queue with batches
        let mut packet_batches = vec![];
        for _ in 0..max_batches + 1 {
            packet_batches.push(packet_batch.clone());
        }
        match sender.send_batches(packet_batches) {
            Ok(discarded) => assert_eq!(discarded, 1),
            Err(_err) => (),
        }

        // One batch should get dropped because queue is full.
        match sender.send_batch(packet_batch.clone()) {
            Ok(dropped_packet) => assert_eq!(dropped_packet, true),
            Err(_err) => (),
        }

        // Receive batches up until the limit
        match receiver.recv(packets_batch_size) {
            Ok((batches, packets)) => {
                assert_eq!(batches.len(), packets_batch_size / num_packets);
                assert_eq!(packets, packets_batch_size);
            }
            Err(_err) => (),
        }

        // Receive the rest of the batches
        match receiver.recv(packets_batch_size) {
            Ok((batches, packets)) => {
                assert_eq!(batches.len(), packets_batch_size / num_packets);
                assert_eq!(packets, packets_batch_size);
            }
            Err(_err) => (),
        }
    }

    #[test]
    fn bounded_streamer_disconnect() {
        let num_packets = 10;
        let max_batches = 10;
        let timeout = Duration::from_millis(1);
        let (sender1, receiver) = packet_batch_channel(max_batches);

        let mut packet_batch = PacketBatch::default();
        for _ in 0..num_packets {
            let p = Packet::default();
            packet_batch.packets.push(p);
        }

        // CHECK: Receiving when no more data is present causes a timeout

        sender1.send_batch(packet_batch.clone()).unwrap();

        match receiver.recv(12) {
            Ok((batches, packets)) => {
                assert_eq!(batches.len(), 1);
                assert_eq!(packets, 10);
            }
            Err(_err) => (),
        }

        assert_eq!(
            receiver.recv_timeout(12, timeout).unwrap_err(),
            RecvTimeoutError::Timeout
        );

        // CHECK: Receiving when all senders are dropped causes Disconnected

        sender1.send_batch(packet_batch.clone()).unwrap();

        {
            let sender2 = sender1.clone();
            sender2.send_batch(packet_batch.clone()).unwrap();
        }

        drop(sender1);

        match receiver.recv(12) {
            Ok((batches, packets)) => {
                assert_eq!(batches.len(), 1);
                assert_eq!(packets, 10);
            }
            Err(_err) => (),
        }

        match receiver.recv(12) {
            Ok((batches, packets)) => {
                assert_eq!(batches.len(), 1);
                assert_eq!(packets, 10);
            }
            Err(_err) => (),
        }

        assert_eq!(
            receiver.recv_timeout(12, timeout).unwrap_err(),
            RecvTimeoutError::Disconnected
        );
    }
}
