//! The `bounded_streamer` module defines a set of services for safely & efficiently pushing & pulling data from UDP sockets.
//!

use {
    crate::packet::PacketBatch,
    crossbeam_channel::{Receiver, RecvError, RecvTimeoutError, SendError, Sender},
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
    batches_batch_size: usize,
    max_queued_batches: usize,
}

#[derive(Clone)]
pub struct BoundedPacketBatchReceiver {
    receiver: Receiver<()>,
    data: Arc<RwLock<PacketBatchChannelData>>,
}

#[derive(Clone)]
pub struct BoundedPacketBatchSender {
    sender: Sender<()>,
    data: Arc<RwLock<PacketBatchChannelData>>,
}

impl PacketBatchChannelData {
    fn add_packet_count(&mut self, amount: usize) {
        self.packet_count = self.packet_count.saturating_add(amount);
    }
    fn sub_packet_count(&mut self, amount: usize) {
        self.packet_count = self.packet_count.saturating_sub(amount);
    }
}

impl BoundedPacketBatchReceiver {
    // TODO: can return Ok(no-batches)
    pub fn recv_timeout(
        &self,
        timeout: Duration,
    ) -> Result<(Vec<PacketBatch>, usize), RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        loop {
            return match self.receiver.recv_deadline(deadline) {
                Ok(()) => {
                    if let Some(r) = self.try_recv() {
                        Ok(r)
                    } else {
                        continue;
                    }
                }
                Err(err) => Err(err),
            };
        }
    }

    pub fn recv_until_empty_timeout_or_max_packets(
        &self,
        recv_timeout: Duration,
        batching_timeout: Duration,
        batch_size_upperbound: usize,
    ) -> Result<Vec<PacketBatch>, RecvTimeoutError> {
        let start = Instant::now();
        let (mut packet_batches, _) = self.recv_timeout(recv_timeout)?;
        while let Some((packet_batch, _)) = self.try_recv() {
            trace!("got more packets");
            packet_batches.extend(packet_batch);
            if start.elapsed() >= batching_timeout || packet_batches.len() >= batch_size_upperbound
            {
                break;
            }
        }
        Ok(packet_batches)
    }

    pub fn recv(&self) -> Result<(Vec<PacketBatch>, usize), RecvError> {
        loop {
            return match self.receiver.recv() {
                Ok(()) => {
                    if let Some(r) = self.try_recv() {
                        Ok(r)
                    } else {
                        continue;
                    }
                }
                Err(err) => Err(err),
            };
        }
    }

    fn try_recv(&self) -> Option<(Vec<PacketBatch>, usize)> {
        let (recv_data, packets) = {
            let mut locked_data = self.data.write().unwrap();

            let mut batches = 0;
            let mut packets = 0;
            for batch in locked_data.queue.iter() {
                let new_packets = packets + batch.packets.len();
                if new_packets > locked_data.batches_batch_size {
                    break;
                }
                packets = new_packets;
                batches += 1;
            }
            if batches == 0 {
                return None;
            }
            locked_data.sub_packet_count(packets);
            (
                locked_data.queue.drain(0..batches).collect::<Vec<_>>(),
                packets,
            )
        };

        Some((recv_data, packets))
    }

    pub fn recv_default_timeout(&self) -> Result<(Vec<PacketBatch>, usize), RecvTimeoutError> {
        let timer = Duration::new(1, 0);
        self.recv_timeout(timer)
    }

    pub fn recv_duration_default_timeout(
        &self,
    ) -> Result<(Vec<PacketBatch>, usize, Duration), RecvTimeoutError> {
        let now = Instant::now();
        match self.recv_default_timeout() {
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
        if let Err(err) = self.sender.send(()) {
            return Err(err);
        }
        Ok(batch_discarded)
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
        if let Err(err) = self.sender.send(()) {
            return Err(err);
        }
        Ok(batches_discarded)
    }

    pub fn batch_count(&self) -> usize {
        self.data.read().unwrap().queue.len()
    }

    pub fn packet_count(&self) -> usize {
        self.data.read().unwrap().packet_count
    }
}

pub fn packet_batch_channel(
    batches_batch_size: usize,
    max_queued_batches: usize,
) -> (BoundedPacketBatchSender, BoundedPacketBatchReceiver) {
    let (sig_sender, sig_receiver) = crossbeam_channel::unbounded::<()>();
    let data = Arc::new(RwLock::new(PacketBatchChannelData {
        queue: VecDeque::new(),
        packet_count: 0,
        max_queued_batches,
        batches_batch_size,
    }));
    let sender = BoundedPacketBatchSender {
        sender: sig_sender.clone(),
        data: data.clone(),
    };
    let receiver = BoundedPacketBatchReceiver {
        receiver: sig_receiver,
        data: data,
    };
    (sender, receiver)
}

#[cfg(test)]
mod test {
    use {
        crate::{
            bounded_streamer::{packet_batch_channel},
        },
        solana_perf::packet::{Packet, PacketBatch},
    };

    #[test]
    fn bounded_streamer_test() {
        let num_packets = 10;
        let (sender, receiver) = packet_batch_channel(5, 10);
        
        let mut packet_batch = PacketBatch::default();
        for _i in 0..num_packets {
            let p = Packet::default();
            packet_batch.packets.push(p);
        }

        println!("Sending batch");
        match sender.send_batch(packet_batch) {
            Ok(_x) => {
                println!("Send success!");
            }
            Err(err) => {
                println!("Send Fail!");
            }
        }
        println!("Sent batch");
        match receiver.recv() {
            Ok((_batches, packets)) => {
                assert_eq!(packets, num_packets);
                println!("PASS!");
            }
            Err(_err) => {
                println!("Error!");
            }
        }
    }
}