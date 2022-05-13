//! The `bounded_streamer` module defines a set of services for safely & efficiently pushing & pulling data from UDP sockets.
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

struct PacketBatchChannelData {
    queue: VecDeque<PacketBatch>,
    packet_count: usize,
    max_queued_batches: usize,
    sender_count: usize,
}

#[derive(Clone)]
pub struct BoundedPacketBatchReceiver {
    signal_receiver: Receiver<()>,
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
    // Returns (vec-of-batches, packet-count)
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

    pub fn recv_until_empty_timeout_or_max_packets(
        &self,
        recv_timeout: Duration,
        batching_timeout: Duration,
        batch_size_upperbound: usize,
        max_packet_count: usize,
    ) -> Result<Vec<PacketBatch>, RecvTimeoutError> {
        let start = Instant::now();
        let (mut packet_batches, _) = self.recv_timeout(max_packet_count, recv_timeout)?;
        while let Ok((packet_batch, _)) = self.try_recv(max_packet_count) {
            trace!("got more packets");
            packet_batches.extend(packet_batch);
            if start.elapsed() >= batching_timeout || packet_batches.len() >= batch_size_upperbound
            {
                break;
            }
        }
        Ok(packet_batches)
    }

    // Returns (vec-of-batches, packet-count)
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
        let mut locked_data = self.data.write().unwrap();

        let disconnected = locked_data.sender_count == 0;

        let mut batches = 0;
        let mut packets = 0;
        for batch in locked_data.queue.iter() {
            let new_packets = packets + batch.packets.len();
            // Do we rather allow receiving 0 batches if the first batch
            // exceeds max_packet_count, or do we allow exceeding
            // max_packet_count?
            if batches > 0 && new_packets > max_packet_count {
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

        let recv_data = locked_data.queue.drain(0..batches).collect::<Vec<_>>();
        locked_data.sub_packet_count(packets);
        let has_more = batches < locked_data.queue.len();

        drop(locked_data);

        // If there's more data in the queue then notify another receiver.
        // Also, if we're disconnected but still return data, wake up again to
        // signal the disconnected error without delay.
        if has_more || disconnected {
            self.signal_sender.try_send(()).unwrap_or(());
        }

        Ok((recv_data, packets))
    }

    // Returns (vec-of-batches, packet-count)
    pub fn recv_default_timeout(
        &self,
        max_packet_count: usize,
    ) -> Result<(Vec<PacketBatch>, usize), RecvTimeoutError> {
        self.recv_timeout(max_packet_count, Duration::new(1, 0))
    }

    // Returns (vec-of-batches, packet-count, duration of receive incl wait)
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

    pub fn batch_count(&self) -> usize {
        self.data.read().unwrap().queue.len()
    }

    pub fn packet_count(&self) -> usize {
        self.data.read().unwrap().packet_count
    }
}

impl BoundedPacketBatchSender {
    // Ok(true) means an existing batch was discarded
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

    // Ok(n) means that n batches were discarded
    pub fn send_batches(
        &self,
        batches: Vec<PacketBatch>,
    ) -> std::result::Result<usize, SendError<()>> {
        if batches.len() == 0 {
            return Ok(0);
        }
        // TODO: this allows adding batches without packets
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

    pub fn batch_count(&self) -> usize {
        self.data.read().unwrap().queue.len()
    }

    pub fn packet_count(&self) -> usize {
        self.data.read().unwrap().packet_count
    }
}

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
        crate::bounded_streamer::packet_batch_channel,
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
}
