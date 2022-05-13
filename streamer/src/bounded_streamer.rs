//! The `bounded_streamer` module defines a set of services for safely & efficiently pushing & pulling packet batches between threads.
//!

use {
    crate::packet::PacketBatch,
    crossbeam_channel::{Receiver, RecvError, RecvTimeoutError, SendError, Sender, TrySendError},
    std::{
        collections::VecDeque,
        result::Result,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

/// 10k batches means up to 1.3M packets, roughly 1.6GB of memory
pub const DEFAULT_MAX_QUEUED_BATCHES: usize = 10_000;

struct PacketBatchChannelData {
    queue: VecDeque<PacketBatch>,
    /// Number of packets currently queued
    packet_count: usize,
    /// Constrain the number of queued batches to avoid exploding memory
    max_queued_batches: usize,
    /// How many senders are sending to this channel
    sender_count: usize,
}

#[derive(Clone)]
pub struct BoundedPacketBatchReceiver {
    signal_receiver: Receiver<()>,
    /// Instance of sender for waking up receivers (e.g. because there are more batches to receive)
    signal_sender: Sender<()>,
    data: Arc<RwLock<PacketBatchChannelData>>,
}

pub struct BoundedPacketBatchSender {
    signal_sender: Sender<()>,
    data: Arc<RwLock<PacketBatchChannelData>>,
}

impl Clone for BoundedPacketBatchSender {
    fn clone(&self) -> Self {
        {
            let mut locked_data = self.data.write().unwrap();
            locked_data.sender_count += 1;
        }
        Self {
            signal_sender: self.signal_sender.clone(),
            data: self.data.clone(),
        }
    }
}

impl Drop for BoundedPacketBatchSender {
    fn drop(&mut self) {
        let dropping_last_sender = {
            let mut locked_data = self.data.write().unwrap();
            locked_data.sender_count -= 1;
            locked_data.sender_count == 0
        };
        if dropping_last_sender {
            // notify receivers, otherwise they may be waiting forever on a
            // disconnected channel
            self.signal_sender.try_send(()).unwrap_or(());
        }
    }
}

impl PacketBatchChannelData {
    fn add_packet_count(&mut self, amount: usize) {
        self.packet_count = self.packet_count.saturating_add(amount);
    }
    fn sub_packet_count(&mut self, amount: usize) {
        self.packet_count = self.packet_count.saturating_sub(amount);
    }
}

enum TryRecvError {
    NoData,
    DisconnectedAndNoData,
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
        let deadline = Instant::now() + timeout;
        loop {
            self.signal_receiver.recv_deadline(deadline)?;
            return match self.try_recv(max_packet_count) {
                Ok(r) => Ok(r),
                Err(TryRecvError::NoData) => continue,
                Err(TryRecvError::DisconnectedAndNoData) => Err(RecvTimeoutError::Disconnected),
            };
        }
    }

    /// Receives up to `max_packet_count` packets from the channel.
    ///
    /// Waits until there's data or the channel has been disconnected.
    ///
    /// Returns (vec-of-batches, packet count)
    pub fn recv(&self, max_packet_count: usize) -> Result<(Vec<PacketBatch>, usize), RecvError> {
        loop {
            self.signal_receiver.recv()?;
            return match self.try_recv(max_packet_count) {
                Ok(r) => Ok(r),
                Err(TryRecvError::NoData) => continue,
                Err(TryRecvError::DisconnectedAndNoData) => Err(RecvError),
            };
        }
    }

    // Returns (vec-of-batches, packet-count)
    fn try_recv(&self, max_packet_count: usize) -> Result<(Vec<PacketBatch>, usize), TryRecvError> {
        let mut batches = 0;
        let mut packets = 0;
        let mut packets_dropped = 0;
        let mut first_batch_exceeds_threshold = false;
        let mut locked_data = self.data.write().unwrap();
        let disconnected = locked_data.sender_count == 0;

        for batch in locked_data.queue.iter() {
            let new_packets = packets + batch.packets.len();
            if new_packets > max_packet_count {
                if batches == 0 {
                    first_batch_exceeds_threshold = true;
                    batches = 1;
                }
                break;
            }
            packets = new_packets;
            batches += 1;
        }
        if batches == 0 {
            drop(locked_data);
            return if disconnected {
                // Wake up ourselves or other receivers again
                self.signal_sender.try_send(()).unwrap_or(());
                Err(TryRecvError::DisconnectedAndNoData)
            } else {
                Err(TryRecvError::NoData)
            };
        }

        let mut recv_data = locked_data.queue.drain(0..batches).collect::<Vec<_>>();
        if first_batch_exceeds_threshold {
            // First batch exceeds the max packet count.
            // Silently drop the tail packets of the batch to make progress.
            recv_data[0].packets.truncate(max_packet_count);
            packets = max_packet_count;
            packets_dropped = recv_data[0].packets.len() - max_packet_count;
        }
        let has_more = locked_data.queue.len() > 0;
        locked_data.sub_packet_count(packets + packets_dropped);

        drop(locked_data);

        // If there's more data in the queue, then notify another receiver.
        // Also, if we're disconnected but still return data, wake up again to
        // signal the disconnected error without delay.
        if has_more || disconnected {
            self.signal_sender.try_send(()).unwrap_or(());
        }

        Ok((recv_data, packets))
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
        self.data.read().unwrap().queue.len()
    }

    /// Number of packets in the queue
    pub fn packet_count(&self) -> usize {
        self.data.read().unwrap().packet_count
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
        if batch.packets.len() == 0 {
            return Ok(false);
        }
        let batch_discarded = {
            let mut locked_data = self.data.write().unwrap();
            let discarded = if locked_data.queue.len() >= locked_data.max_queued_batches {
                if let Some(old_batch) = locked_data.queue.pop_front() {
                    locked_data.sub_packet_count(old_batch.packets.len());
                    true
                } else {
                    false
                }
            } else {
                false
            };
            locked_data.add_packet_count(batch.packets.len());
            locked_data.queue.push_back(batch);
            discarded
        };

        // wake a receiver
        match self.signal_sender.try_send(()) {
            Err(TrySendError::Disconnected(v)) => Err(SendError(v)),
            _ => Ok(batch_discarded),
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
        if batches.len() == 0 {
            return Ok(0);
        }
        // note: this allows adding batches without packets
        let batches_discarded = {
            let mut locked_data = self.data.write().unwrap();
            let max_queued_batches = locked_data.max_queued_batches;
            let batches_to_queue = std::cmp::min(batches.len(), max_queued_batches);
            let discard_new_batches = batches.len() - batches_to_queue;

            let discard_old_batches =
                (locked_data.queue.len() + batches_to_queue).saturating_sub(max_queued_batches);
            let discard_old_packets = locked_data
                .queue
                .drain(0..discard_old_batches)
                .map(|b| b.packets.len())
                .sum();
            locked_data.sub_packet_count(discard_old_packets);

            let new_packets = batches
                .iter()
                .take(max_queued_batches)
                .map(|b| b.packets.len())
                .sum();
            locked_data.add_packet_count(new_packets);
            locked_data
                .queue
                .extend(batches.into_iter().take(max_queued_batches));

            discard_old_batches + discard_new_batches
        };

        // wake a receiver
        match self.signal_sender.try_send(()) {
            Err(TrySendError::Disconnected(v)) => Err(SendError(v)),
            _ => Ok(batches_discarded),
        }
    }

    /// Number of batches in the queue
    pub fn batch_count(&self) -> usize {
        self.data.read().unwrap().queue.len()
    }

    /// Number of packets in the queue
    pub fn packet_count(&self) -> usize {
        self.data.read().unwrap().packet_count
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
    let (signal_sender, signal_receiver) = crossbeam_channel::bounded::<()>(1);
    let data = Arc::new(RwLock::new(PacketBatchChannelData {
        queue: VecDeque::new(),
        packet_count: 0,
        max_queued_batches,
        sender_count: 1,
    }));
    let sender = BoundedPacketBatchSender {
        signal_sender: signal_sender.clone(),
        data: data.clone(),
    };
    let receiver = BoundedPacketBatchReceiver {
        signal_receiver,
        signal_sender,
        data,
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
