use core::fmt;
use core::num::NonZeroU16;
use std::collections::VecDeque;
use std::io;
use std::net::{IpAddr, SocketAddr};
use std::os::fd::OwnedFd;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use fast_socket_rs::{
    EcnCodepoint, Error, PacketBufferMut, PollDriver, RecvBatch, SendError, TxSlot, UdpReceive,
    UdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut, WaitDrivenDriverKind,
};
use tokio::io::unix::AsyncFd;
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

/// Configuration for a Tokio UDP socket actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActorConfig {
    /// Number of packets requested from the socket in one receive pass.
    pub recv_batch_size: usize,
    /// Capacity of the actor command queue.
    pub command_queue_capacity: usize,
    /// Capacity of the receive batch queue exposed to [`AsyncUdpRx`].
    pub rx_queue_capacity: usize,
    /// Maximum number of queued transmit packets held before the actor stops
    /// accepting more TX commands in one pass.
    pub pending_tx_capacity: usize,
}

impl Default for ActorConfig {
    fn default() -> Self {
        Self {
            recv_batch_size: 64,
            command_queue_capacity: 1024,
            rx_queue_capacity: 1024,
            pending_tx_capacity: 1024,
        }
    }
}

impl ActorConfig {
    fn normalize(self) -> Self {
        Self {
            recv_batch_size: self.recv_batch_size.max(1),
            command_queue_capacity: self.command_queue_capacity.max(1),
            rx_queue_capacity: self.rx_queue_capacity.max(1),
            pending_tx_capacity: self.pending_tx_capacity.max(1),
        }
    }
}

/// Error returned by Tokio UDP actor operations.
#[derive(Debug)]
pub enum AsyncUdpError {
    /// The socket driver did not expose a Unix wake handle.
    MissingWakeHandle,
    /// Tokio failed to register or wait on a file descriptor.
    Io(io::Error),
    /// The actor task has closed.
    ActorClosed,
    /// The socket returned a core operation error.
    Socket(Error),
    /// The socket returned a send error after accepting a prefix.
    Send(SendError),
    /// The actor task panicked or was cancelled.
    Join(tokio::task::JoinError),
}

impl fmt::Display for AsyncUdpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingWakeHandle => f.write_str("wait-driven socket did not expose a wake fd"),
            Self::Io(error) => write!(f, "async fd operation failed: {error}"),
            Self::ActorClosed => f.write_str("UDP actor is closed"),
            Self::Socket(error) => write!(f, "UDP socket operation failed: {error}"),
            Self::Send(error) => write!(f, "UDP send failed: {error}"),
            Self::Join(error) => write!(f, "UDP actor task failed: {error}"),
        }
    }
}

impl std::error::Error for AsyncUdpError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Socket(error) => Some(error),
            Self::Send(error) => Some(error),
            Self::Join(error) => Some(error),
            Self::MissingWakeHandle | Self::ActorClosed => None,
        }
    }
}

impl From<Error> for AsyncUdpError {
    fn from(value: Error) -> Self {
        Self::Socket(value)
    }
}

impl From<SendError> for AsyncUdpError {
    fn from(value: SendError) -> Self {
        Self::Send(value)
    }
}

impl From<ActorClosed> for AsyncUdpError {
    fn from(_: ActorClosed) -> Self {
        Self::ActorClosed
    }
}

/// Error returned when an actor handle or receive stream has closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActorClosed;

impl fmt::Display for ActorClosed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("UDP actor is closed")
    }
}

impl std::error::Error for ActorClosed {}

#[derive(Debug)]
struct ActorState {
    outstanding_buffers: AtomicUsize,
    empty: Notify,
}

impl ActorState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            outstanding_buffers: AtomicUsize::new(0),
            empty: Notify::new(),
        })
    }

    fn lease(self: &Arc<Self>) -> ActorBufferLease {
        self.outstanding_buffers.fetch_add(1, Ordering::Relaxed);
        ActorBufferLease {
            state: Some(Arc::clone(self)),
        }
    }

    async fn wait_for_buffers(&self) {
        loop {
            if self.outstanding_buffers.load(Ordering::Acquire) == 0 {
                return;
            }
            self.empty.notified().await;
        }
    }
}

/// Lease held by actor buffer wrappers while application code owns a socket
/// buffer.
#[derive(Debug)]
pub struct ActorBufferLease {
    state: Option<Arc<ActorState>>,
}

impl ActorBufferLease {
    fn release(&mut self) {
        let Some(state) = self.state.take() else {
            return;
        };
        if state.outstanding_buffers.fetch_sub(1, Ordering::AcqRel) == 1 {
            state.empty.notify_waiters();
        }
    }
}

impl Drop for ActorBufferLease {
    fn drop(&mut self) {
        self.release();
    }
}

/// Transmit metadata applied to a batch of actor-owned transmit buffers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ActorTxMeta {
    /// Remote destination address.
    pub destination: SocketAddr,
    /// Optional source IP selection.
    pub source_ip: Option<IpAddr>,
    /// Optional source UDP port selection.
    pub source_port: Option<u16>,
    /// Optional ECN codepoint.
    pub ecn: Option<EcnCodepoint>,
    /// Optional UDP segmentation size.
    pub gso_segment_size: Option<NonZeroU16>,
}

impl ActorTxMeta {
    /// Creates metadata for a UDP transmit batch.
    #[must_use]
    pub const fn new(destination: SocketAddr) -> Self {
        Self {
            destination,
            source_ip: None,
            source_port: None,
            ecn: None,
            gso_segment_size: None,
        }
    }
}

impl From<SocketAddr> for ActorTxMeta {
    fn from(value: SocketAddr) -> Self {
        Self::new(value)
    }
}

/// Mutable transmit buffer loaned from an actor-owned socket.
pub struct ActorTxBuffer<S: UdpSocket> {
    buffer: Option<UdpTxBufferMut<S>>,
    lease: ActorBufferLease,
}

impl<S: UdpSocket> ActorTxBuffer<S> {
    fn new(buffer: UdpTxBufferMut<S>, lease: ActorBufferLease) -> Self {
        Self {
            buffer: Some(buffer),
            lease,
        }
    }

    /// Borrows the mutable socket transmit buffer.
    #[must_use]
    pub fn buffer(&self) -> &UdpTxBufferMut<S> {
        self.buffer
            .as_ref()
            .expect("actor TX buffer was already consumed")
    }

    /// Mutably borrows the socket transmit buffer.
    #[must_use]
    pub fn buffer_mut(&mut self) -> &mut UdpTxBufferMut<S> {
        self.buffer
            .as_mut()
            .expect("actor TX buffer was already consumed")
    }

    /// Freezes this buffer into an actor transmit packet.
    #[must_use]
    pub fn freeze(mut self, destination: SocketAddr) -> ActorTxPacket<S> {
        let packet = self
            .buffer
            .take()
            .expect("actor TX buffer was already consumed")
            .freeze();
        ActorTxPacket::from_parts(packet, ActorTxMeta::new(destination), self.lease)
    }

    fn into_transmit(mut self, meta: ActorTxMeta) -> UdpTransmit<UdpTxBuffer<S>> {
        let packet = self
            .buffer
            .take()
            .expect("actor TX buffer was already consumed")
            .freeze();
        self.lease.release();
        UdpTransmit {
            packet,
            destination: meta.destination,
            source_ip: meta.source_ip,
            source_port: meta.source_port,
            ecn: meta.ecn,
            gso_segment_size: meta.gso_segment_size,
        }
    }
}

/// Frozen transmit packet loaned from an actor-owned socket.
pub struct ActorTxPacket<S: UdpSocket> {
    packet: Option<UdpTxBuffer<S>>,
    /// Remote destination address.
    pub destination: SocketAddr,
    /// Optional source IP selection.
    pub source_ip: Option<IpAddr>,
    /// Optional source UDP port selection.
    pub source_port: Option<u16>,
    /// Optional ECN codepoint.
    pub ecn: Option<EcnCodepoint>,
    /// Optional UDP segmentation size.
    pub gso_segment_size: Option<NonZeroU16>,
    lease: ActorBufferLease,
}

impl<S: UdpSocket> ActorTxPacket<S> {
    fn from_parts(packet: UdpTxBuffer<S>, meta: ActorTxMeta, lease: ActorBufferLease) -> Self {
        Self {
            packet: Some(packet),
            destination: meta.destination,
            source_ip: meta.source_ip,
            source_port: meta.source_port,
            ecn: meta.ecn,
            gso_segment_size: meta.gso_segment_size,
            lease,
        }
    }

    /// Borrows the UDP payload buffer.
    #[must_use]
    pub fn packet(&self) -> &UdpTxBuffer<S> {
        self.packet
            .as_ref()
            .expect("actor TX packet was already consumed")
    }

    fn into_transmit(mut self) -> UdpTransmit<UdpTxBuffer<S>> {
        let packet = self
            .packet
            .take()
            .expect("actor TX packet was already consumed");
        self.lease.release();
        UdpTransmit {
            packet,
            destination: self.destination,
            source_ip: self.source_ip,
            source_port: self.source_port,
            ecn: self.ecn,
            gso_segment_size: self.gso_segment_size,
        }
    }
}

/// One UDP packet received by an actor-owned socket.
pub struct ActorRxPacket<S: UdpSocket> {
    packet: Option<fast_socket_rs::UdpRxBuffer<S>>,
    /// Receive metadata produced by the socket backend.
    pub meta: S::RecvMeta,
    lease: ActorBufferLease,
}

impl<S: UdpSocket> ActorRxPacket<S> {
    fn new(
        receive: UdpReceive<fast_socket_rs::UdpRxBuffer<S>, S::RecvMeta>,
        lease: ActorBufferLease,
    ) -> Self {
        Self {
            packet: Some(receive.packet),
            meta: receive.meta,
            lease,
        }
    }

    /// Borrows the UDP payload buffer.
    #[must_use]
    pub fn packet(&self) -> &fast_socket_rs::UdpRxBuffer<S> {
        self.packet
            .as_ref()
            .expect("actor RX packet was already consumed")
    }

    /// Mutably borrows the UDP payload buffer.
    #[must_use]
    pub fn packet_mut(&mut self) -> &mut fast_socket_rs::UdpRxBuffer<S> {
        self.packet
            .as_mut()
            .expect("actor RX packet was already consumed")
    }
}

impl<S> ActorRxPacket<S>
where
    S: UdpSocket,
    fast_socket_rs::UdpRxBuffer<S>: PacketBufferMut<Frozen = UdpTxBuffer<S>>,
{
    /// Converts this received packet into a frozen transmit packet.
    #[must_use]
    pub fn into_transmit(mut self, destination: SocketAddr) -> ActorTxPacket<S> {
        let packet = self
            .packet
            .take()
            .expect("actor RX packet was already consumed")
            .freeze();
        ActorTxPacket::from_parts(packet, ActorTxMeta::new(destination), self.lease)
    }
}

/// A batch of UDP packets received by an actor-owned socket.
pub struct ActorRxBatch<S: UdpSocket> {
    packets: Vec<ActorRxPacket<S>>,
}

impl<S: UdpSocket> ActorRxBatch<S> {
    fn new(packets: Vec<ActorRxPacket<S>>) -> Self {
        Self { packets }
    }

    /// Returns the number of packets in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.packets.len()
    }

    /// Returns `true` when the batch is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.packets.is_empty()
    }

    /// Borrows all packets in the batch.
    #[must_use]
    pub fn as_slice(&self) -> &[ActorRxPacket<S>] {
        &self.packets
    }

    /// Borrows all packets in the batch mutably.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [ActorRxPacket<S>] {
        &mut self.packets
    }

    /// Drains all packets from the batch.
    pub fn drain(&mut self) -> std::vec::Drain<'_, ActorRxPacket<S>> {
        self.packets.drain(..)
    }
}

/// Running Tokio UDP actor.
pub struct AsyncUdpActor<S: UdpSocket> {
    handle: AsyncUdpHandle<S>,
    rx: AsyncUdpRx<S>,
    join: JoinHandle<Result<(), AsyncUdpError>>,
}

impl<S> AsyncUdpActor<S>
where
    S: UdpSocket + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: 'static,
{
    /// Returns a cloneable actor handle for TX and buffer allocation.
    #[must_use]
    pub fn handle(&self) -> AsyncUdpHandle<S> {
        self.handle.clone()
    }

    /// Splits this actor into a cloneable handle, receive stream, and join
    /// handle.
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        AsyncUdpHandle<S>,
        AsyncUdpRx<S>,
        JoinHandle<Result<(), AsyncUdpError>>,
    ) {
        (self.handle, self.rx, self.join)
    }

    /// Requests shutdown and waits for the actor task to finish.
    pub async fn shutdown(self) -> Result<(), AsyncUdpError> {
        let _ = self.handle.commands.send(ActorCommand::Shutdown).await;
        drop(self.handle);
        drop(self.rx);
        self.join.await.map_err(AsyncUdpError::Join)?
    }
}

/// Cloneable handle for actor transmit, allocation, and control operations.
pub struct AsyncUdpHandle<S: UdpSocket> {
    state: Arc<ActorState>,
    commands: mpsc::Sender<ActorCommand<S>>,
}

impl<S: UdpSocket> Clone for AsyncUdpHandle<S> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            commands: self.commands.clone(),
        }
    }
}

impl<S> AsyncUdpHandle<S>
where
    S: UdpSocket + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: 'static,
{
    /// Allocates up to `max` mutable transmit buffers from the actor-owned
    /// socket and appends them to `out`.
    pub async fn alloc_tx_batch(
        &self,
        max: usize,
        out: &mut Vec<ActorTxBuffer<S>>,
    ) -> Result<usize, AsyncUdpError> {
        if max == 0 {
            return Ok(0);
        }
        let (reply, response) = oneshot::channel();
        self.commands
            .send(ActorCommand::Allocate { max, reply })
            .await
            .map_err(|_| AsyncUdpError::ActorClosed)?;
        let buffers = response.await.map_err(|_| AsyncUdpError::ActorClosed)??;
        let count = buffers.len();
        out.extend(buffers);
        Ok(count)
    }

    /// Queues filled mutable transmit buffers for actor-owned transmission.
    ///
    /// On success, every buffer is removed from `buffers` and accepted by the
    /// actor. Socket-level send errors are reported by the actor task.
    pub async fn send_tx_buffers(
        &self,
        buffers: &mut Vec<ActorTxBuffer<S>>,
        meta: impl Into<ActorTxMeta>,
    ) -> Result<usize, ActorClosed> {
        let count = buffers.len();
        if count == 0 {
            return Ok(0);
        }
        let mut moved = Vec::with_capacity(count);
        moved.append(buffers);
        match self
            .commands
            .send(ActorCommand::SendBuffers {
                buffers: moved,
                meta: meta.into(),
            })
            .await
        {
            Ok(()) => Ok(count),
            Err(error) => {
                if let ActorCommand::SendBuffers {
                    buffers: mut returned,
                    ..
                } = error.0
                {
                    buffers.append(&mut returned);
                }
                Err(ActorClosed)
            }
        }
    }

    /// Queues frozen transmit packets for actor-owned transmission.
    ///
    /// On success, every packet is removed from `packets` and accepted by the
    /// actor. Socket-level send errors are reported by the actor task.
    pub async fn send_tx_packets(
        &self,
        packets: &mut Vec<ActorTxPacket<S>>,
    ) -> Result<usize, ActorClosed> {
        let count = packets.len();
        if count == 0 {
            return Ok(0);
        }
        let mut moved = Vec::with_capacity(count);
        moved.append(packets);
        match self
            .commands
            .send(ActorCommand::SendPackets { packets: moved })
            .await
        {
            Ok(()) => Ok(count),
            Err(error) => {
                if let ActorCommand::SendPackets {
                    packets: mut returned,
                } = error.0
                {
                    packets.append(&mut returned);
                }
                Err(ActorClosed)
            }
        }
    }

    /// Requests actor shutdown.
    pub async fn shutdown(&self) -> Result<(), ActorClosed> {
        self.commands
            .send(ActorCommand::Shutdown)
            .await
            .map_err(|_| ActorClosed)
    }
}

/// Single-consumer receive stream for actor-delivered UDP batches.
pub struct AsyncUdpRx<S: UdpSocket> {
    batches: mpsc::Receiver<ActorRxBatch<S>>,
}

impl<S: UdpSocket> AsyncUdpRx<S> {
    /// Receives the next batch from the actor.
    pub async fn recv_batch(&mut self) -> Result<ActorRxBatch<S>, ActorClosed> {
        self.batches.recv().await.ok_or(ActorClosed)
    }
}

enum ActorCommand<S: UdpSocket> {
    Allocate {
        max: usize,
        reply: oneshot::Sender<Result<Vec<ActorTxBuffer<S>>, Error>>,
    },
    SendBuffers {
        buffers: Vec<ActorTxBuffer<S>>,
        meta: ActorTxMeta,
    },
    SendPackets {
        packets: Vec<ActorTxPacket<S>>,
    },
    Shutdown,
}

/// Starts a Tokio actor task for a wait-driven UDP socket.
///
/// This variant uses [`tokio::spawn`] and therefore requires a `Send` socket.
/// For queue-local sockets that are intentionally `!Send`, use
/// [`spawn_udp_actor_local`] inside a Tokio [`LocalSet`](tokio::task::LocalSet).
pub fn spawn_udp_actor<S>(socket: S, config: ActorConfig) -> Result<AsyncUdpActor<S>, AsyncUdpError>
where
    S: UdpSocket + Send + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: Send + 'static,
{
    let config = config.normalize();
    let wait_fd = socket_wait_fd(&socket)?;
    let state = ActorState::new();
    let (commands_tx, commands_rx) = mpsc::channel(config.command_queue_capacity);
    let (rx_tx, rx) = mpsc::channel(config.rx_queue_capacity);
    let handle = AsyncUdpHandle {
        state: Arc::clone(&state),
        commands: commands_tx,
    };
    let join = tokio::spawn(run_actor(
        socket,
        wait_fd,
        state,
        commands_rx,
        rx_tx,
        config,
    ));
    Ok(AsyncUdpActor {
        handle,
        rx: AsyncUdpRx { batches: rx },
        join,
    })
}

/// Starts a Tokio local actor task for a wait-driven UDP socket.
///
/// This variant uses [`tokio::task::spawn_local`] and supports wait-driven
/// sockets that are not `Send`, such as queue-local AF_XDP sockets. It must be
/// called from within a Tokio [`LocalSet`](tokio::task::LocalSet) or another
/// local task context.
pub fn spawn_udp_actor_local<S>(
    socket: S,
    config: ActorConfig,
) -> Result<AsyncUdpActor<S>, AsyncUdpError>
where
    S: UdpSocket + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: 'static,
{
    let config = config.normalize();
    let wait_fd = socket_wait_fd(&socket)?;
    let state = ActorState::new();
    let (commands_tx, commands_rx) = mpsc::channel(config.command_queue_capacity);
    let (rx_tx, rx) = mpsc::channel(config.rx_queue_capacity);
    let handle = AsyncUdpHandle {
        state: Arc::clone(&state),
        commands: commands_tx,
    };
    let join = tokio::task::spawn_local(run_actor(
        socket,
        wait_fd,
        state,
        commands_rx,
        rx_tx,
        config,
    ));
    Ok(AsyncUdpActor {
        handle,
        rx: AsyncUdpRx { batches: rx },
        join,
    })
}

fn socket_wait_fd<S>(socket: &S) -> Result<AsyncFd<OwnedFd>, AsyncUdpError>
where
    S: UdpSocket,
    S::Driver: WaitDrivenDriverKind,
{
    let wake = socket
        .driver()
        .wake_handle()
        .ok_or(AsyncUdpError::MissingWakeHandle)?;
    let fd = wake
        .borrowed_fd()
        .try_clone_to_owned()
        .map_err(AsyncUdpError::Io)?;
    AsyncFd::new(fd).map_err(AsyncUdpError::Io)
}

async fn run_actor<S>(
    mut socket: S,
    wait_fd: AsyncFd<OwnedFd>,
    state: Arc<ActorState>,
    mut commands: mpsc::Receiver<ActorCommand<S>>,
    rx_tx: mpsc::Sender<ActorRxBatch<S>>,
    config: ActorConfig,
) -> Result<(), AsyncUdpError>
where
    S: UdpSocket + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: 'static,
{
    let result =
        run_actor_inner(&mut socket, &wait_fd, &state, &mut commands, &rx_tx, config).await;

    commands.close();
    while commands.recv().await.is_some() {}
    state.wait_for_buffers().await;
    result
}

async fn run_actor_inner<S>(
    socket: &mut S,
    wait_fd: &AsyncFd<OwnedFd>,
    state: &Arc<ActorState>,
    commands: &mut mpsc::Receiver<ActorCommand<S>>,
    rx_tx: &mpsc::Sender<ActorRxBatch<S>>,
    config: ActorConfig,
) -> Result<(), AsyncUdpError>
where
    S: UdpSocket + 'static,
    S::Driver: WaitDrivenDriverKind,
    S::RecvMeta: 'static,
{
    let mut recv_batch =
        RecvBatch::<UdpReceive<fast_socket_rs::UdpRxBuffer<S>, S::RecvMeta>>::with_capacity(
            config.recv_batch_size,
        );
    let mut pending_tx = VecDeque::<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>::new();
    let mut rx_open = true;
    let mut commands_open = true;
    let mut shutdown_requested = false;

    loop {
        let mut progressed = false;

        while commands_open && !shutdown_requested && pending_tx.len() < config.pending_tx_capacity
        {
            match commands.try_recv() {
                Ok(command) => {
                    progressed = true;
                    if handle_command(socket, state, command, &mut pending_tx)? {
                        shutdown_requested = true;
                        commands_open = false;
                        commands.close();
                        break;
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    commands_open = false;
                    break;
                }
            }
        }

        progressed |= flush_pending_tx(socket, &mut pending_tx)?;
        progressed |= socket.drain_tx_completions()? != 0;
        progressed |= flush_pending_tx(socket, &mut pending_tx)?;

        if rx_open && !shutdown_requested {
            recv_batch.clear();
            match socket.recv(&mut recv_batch) {
                Ok(count) if count != 0 => {
                    progressed = true;
                    let batch = wrap_rx_batch(state, &mut recv_batch);
                    if rx_tx.send(batch).await.is_err() {
                        rx_open = false;
                    }
                }
                Ok(_) | Err(Error::WouldBlock) => {}
                Err(error) => return Err(AsyncUdpError::Socket(error)),
            }
        }

        if (shutdown_requested || (!commands_open && !rx_open)) && pending_tx.is_empty() {
            return Ok(());
        }

        if !progressed {
            if commands_open && !shutdown_requested {
                tokio::select! {
                    command = commands.recv() => {
                        match command {
                            Some(command) => {
                                if handle_command(socket, state, command, &mut pending_tx)? {
                                    shutdown_requested = true;
                                    commands_open = false;
                                    commands.close();
                                }
                            }
                            None => commands_open = false,
                        }
                    }
                    wait = wait_fd.readable() => {
                        let mut guard = wait.map_err(AsyncUdpError::Io)?;
                        guard.clear_ready();
                    }
                }
            } else {
                let mut guard = wait_fd.readable().await.map_err(AsyncUdpError::Io)?;
                guard.clear_ready();
            }
        }
    }
}

fn handle_command<S>(
    socket: &mut S,
    state: &Arc<ActorState>,
    command: ActorCommand<S>,
    pending_tx: &mut VecDeque<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
) -> Result<bool, AsyncUdpError>
where
    S: UdpSocket,
{
    match command {
        ActorCommand::Allocate { max, reply } => {
            let mut buffers = Vec::with_capacity(max);
            let result = socket.allocate_tx_batch(&mut buffers, max).map(|_| {
                buffers
                    .into_iter()
                    .map(|buffer| ActorTxBuffer::new(buffer, state.lease()))
                    .collect()
            });
            let _ = reply.send(result);
            Ok(false)
        }
        ActorCommand::SendBuffers { buffers, meta } => {
            pending_tx.extend(
                buffers
                    .into_iter()
                    .map(|buffer| TxSlot::Ready(buffer.into_transmit(meta))),
            );
            Ok(false)
        }
        ActorCommand::SendPackets { packets } => {
            pending_tx.extend(
                packets
                    .into_iter()
                    .map(|packet| TxSlot::Ready(packet.into_transmit())),
            );
            Ok(false)
        }
        ActorCommand::Shutdown => Ok(true),
    }
}

fn wrap_rx_batch<S>(
    state: &Arc<ActorState>,
    recv_batch: &mut RecvBatch<UdpReceive<fast_socket_rs::UdpRxBuffer<S>, S::RecvMeta>>,
) -> ActorRxBatch<S>
where
    S: UdpSocket,
{
    let packets = recv_batch
        .drain()
        .map(|receive| ActorRxPacket::new(receive, state.lease()))
        .collect();
    ActorRxBatch::new(packets)
}

fn flush_pending_tx<S>(
    socket: &mut S,
    pending_tx: &mut VecDeque<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
) -> Result<bool, AsyncUdpError>
where
    S: UdpSocket,
{
    let mut progressed = false;
    while !pending_tx.is_empty() {
        let (front, _) = pending_tx.as_mut_slices();
        if front.is_empty() {
            pending_tx.make_contiguous();
            continue;
        }

        match socket.send(front) {
            Ok(0) => break,
            Ok(accepted) => {
                pending_tx.drain(..accepted);
                progressed = true;
            }
            Err(error) => {
                if error.accepted != 0 {
                    pending_tx.drain(..error.accepted);
                }
                return Err(AsyncUdpError::Send(error));
            }
        }
    }
    if progressed {
        socket.notify_tx()?;
    }
    Ok(progressed)
}

#[cfg(test)]
mod tests {
    use std::net::UdpSocket as StdUdpSocket;
    use std::os::fd::AsFd;
    use std::sync::{Arc, Mutex};

    use fast_socket_rs::{
        BufferAccessError, BufferLayout, PacketBuffer, ReserveError, Segment, Segments,
        SegmentsMut, SocketId, UdpRecvMeta, WaitOutcome, WakeHandle,
    };

    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestBuf {
        bytes: Vec<u8>,
        layout: BufferLayout,
    }

    impl TestBuf {
        fn new(layout: BufferLayout) -> Self {
            Self {
                bytes: Vec::new(),
                layout,
            }
        }

        fn with_bytes(bytes: &[u8], layout: BufferLayout) -> Self {
            Self {
                bytes: bytes.to_vec(),
                layout,
            }
        }
    }

    impl PacketBuffer for TestBuf {
        type Segments<'a> = Segments<'a>;

        fn len(&self) -> usize {
            self.bytes.len()
        }

        fn headroom(&self) -> usize {
            0
        }

        fn tailroom(&self) -> usize {
            self.layout
                .payload_capacity()
                .saturating_sub(self.bytes.len())
        }

        fn layout(&self) -> &BufferLayout {
            &self.layout
        }

        fn segments(&self) -> Self::Segments<'_> {
            Some(self.bytes.as_slice() as Segment<'_>).into_iter()
        }

        fn read_at_exact(&self, offset: usize, dst: &mut [u8]) -> Result<(), BufferAccessError> {
            let end = offset.saturating_add(dst.len());
            if end > self.bytes.len() {
                return Err(BufferAccessError::OutOfBounds {
                    offset,
                    len: dst.len(),
                    packet_len: self.bytes.len(),
                });
            }
            dst.copy_from_slice(&self.bytes[offset..end]);
            Ok(())
        }
    }

    impl PacketBufferMut for TestBuf {
        type Frozen = Self;
        type SegmentsMut<'a> = SegmentsMut<'a>;

        fn segments_mut(&mut self) -> Self::SegmentsMut<'_> {
            if self.is_empty() {
                None.into_iter()
            } else {
                Some(self.bytes.as_mut_slice()).into_iter()
            }
        }

        fn prepend(&mut self, bytes: &[u8]) -> Result<(), ReserveError> {
            if bytes.is_empty() {
                return Ok(());
            }
            Err(ReserveError::InsufficientHeadroom {
                available: 0,
                requested: bytes.len(),
            })
        }

        fn extend_from_slice(&mut self, bytes: &[u8]) -> Result<(), BufferAccessError> {
            if bytes.len() > self.tailroom() {
                return Err(BufferAccessError::InsufficientTailroom {
                    available: self.tailroom(),
                    requested: bytes.len(),
                });
            }
            self.bytes.extend_from_slice(bytes);
            Ok(())
        }

        fn trim_prefix(&mut self, len: usize) -> Result<(), BufferAccessError> {
            if len > self.bytes.len() {
                return Err(BufferAccessError::OutOfBounds {
                    offset: 0,
                    len,
                    packet_len: self.bytes.len(),
                });
            }
            self.bytes.drain(..len);
            Ok(())
        }

        fn trim_suffix(&mut self, len: usize) -> Result<(), BufferAccessError> {
            if len > self.bytes.len() {
                return Err(BufferAccessError::OutOfBounds {
                    offset: self.bytes.len().saturating_sub(len),
                    len,
                    packet_len: self.bytes.len(),
                });
            }
            self.bytes.truncate(self.bytes.len() - len);
            Ok(())
        }

        fn freeze(self) -> Self::Frozen {
            self
        }
    }

    struct TestPool {
        layout: BufferLayout,
        remaining: usize,
    }

    impl TestPool {
        fn new(remaining: usize) -> Self {
            Self {
                layout: BufferLayout::new(2048),
                remaining,
            }
        }
    }

    impl TestPool {
        fn allocate(&mut self) -> Option<TestBuf> {
            if self.remaining == 0 {
                return None;
            }
            self.remaining -= 1;
            Some(TestBuf::new(self.layout))
        }
    }

    struct TestWaitDriver {
        socket: StdUdpSocket,
    }

    impl TestWaitDriver {
        fn new() -> Self {
            Self {
                socket: StdUdpSocket::bind("127.0.0.1:0").expect("bind wake socket"),
            }
        }
    }

    impl PollDriver for TestWaitDriver {
        const MODE: fast_socket_rs::PollMode = fast_socket_rs::PollMode::WaitDriven;

        fn wait(&mut self, _timeout: Option<core::time::Duration>) -> Result<WaitOutcome, Error> {
            Ok(WaitOutcome::Spurious)
        }

        fn wake_handle(&self) -> Option<WakeHandle<'_>> {
            Some(WakeHandle::from_fd(self.socket.as_fd()))
        }
    }

    impl WaitDrivenDriverKind for TestWaitDriver {}

    struct TestSocket {
        tx_pool: TestPool,
        driver: TestWaitDriver,
        rx: VecDeque<UdpReceive<TestBuf, UdpRecvMeta>>,
        sent: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl TestSocket {
        fn new(rx: VecDeque<UdpReceive<TestBuf, UdpRecvMeta>>, tx_buffers: usize) -> Self {
            Self {
                tx_pool: TestPool::new(tx_buffers),
                driver: TestWaitDriver::new(),
                rx,
                sent: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn sent(&self) -> Arc<Mutex<Vec<Vec<u8>>>> {
            Arc::clone(&self.sent)
        }
    }

    impl UdpSocket for TestSocket {
        type RxBuffer = TestBuf;
        type TxBufferMut = TestBuf;
        type Driver = TestWaitDriver;
        type RecvMeta = UdpRecvMeta;
        type Endpoint = fast_socket_rs::GenericUdpEndpoint;

        fn socket_id(&self) -> SocketId {
            SocketId::new(0)
        }

        fn mtu(&self) -> usize {
            1500
        }

        fn driver(&self) -> &Self::Driver {
            &self.driver
        }

        fn driver_mut(&mut self) -> &mut Self::Driver {
            &mut self.driver
        }

        fn allocate_tx_batch(
            &mut self,
            out: &mut Vec<UdpTxBufferMut<Self>>,
            max: usize,
        ) -> Result<usize, Error> {
            let start_len = out.len();
            while out.len() - start_len < max {
                let Some(buffer) = self.tx_pool.allocate() else {
                    break;
                };
                out.push(buffer);
            }
            Ok(out.len() - start_len)
        }

        fn send(&mut self, batch: &mut [TxSlot<UdpTransmit<TestBuf>>]) -> Result<usize, SendError> {
            for slot in batch.iter_mut() {
                let Some(tx) = slot.take() else {
                    return Err(SendError {
                        accepted: 0,
                        kind: Error::InvalidBatch,
                    });
                };
                self.sent.lock().expect("sent lock").push(tx.packet.bytes);
            }
            Ok(batch.len())
        }

        fn prepare_udp_endpoint(
            &mut self,
            spec: fast_socket_rs::UdpEndpointSpec,
        ) -> Result<Self::Endpoint, Error> {
            fast_socket_rs::prepare_generic_udp_endpoint(self, spec)
        }

        fn udp_endpoint_spec<'a>(
            &self,
            endpoint: &'a Self::Endpoint,
        ) -> &'a fast_socket_rs::UdpEndpointSpec {
            endpoint.spec()
        }

        fn udp_endpoint_info(&self, endpoint: &Self::Endpoint) -> fast_socket_rs::UdpEndpointInfo {
            endpoint.info()
        }

        fn send_to_udp_endpoint(
            &mut self,
            endpoint: &mut Self::Endpoint,
            batch: &mut [TxSlot<fast_socket_rs::UdpEndpointTransmit<TestBuf>>],
        ) -> Result<usize, SendError> {
            fast_socket_rs::send_generic_udp_endpoint(self, endpoint, batch)
        }

        fn recv(
            &mut self,
            out: &mut RecvBatch<UdpReceive<TestBuf, Self::RecvMeta>>,
        ) -> Result<usize, Error> {
            let mut received = 0;
            while out.remaining() != 0 {
                let Some(packet) = self.rx.pop_front() else {
                    break;
                };
                out.push(packet).map_err(|_| Error::BatchFull)?;
                received += 1;
            }
            Ok(received)
        }

        fn drain_tx_completions(&mut self) -> Result<usize, Error> {
            Ok(0)
        }
    }

    fn recv_meta(len: usize) -> UdpRecvMeta {
        UdpRecvMeta {
            source: "127.0.0.1:12345".parse().unwrap(),
            destination: Some("127.0.0.1".parse().unwrap()),
            destination_port: Some(4444),
            ecn: None,
            len,
            gro_stride: None,
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actor_allocates_and_sends_tx_buffers() {
        let socket = TestSocket::new(VecDeque::new(), 2);
        let sent = socket.sent();
        let actor = spawn_udp_actor(socket, ActorConfig::default()).expect("spawn actor");
        let handle = actor.handle();

        let mut buffers = Vec::new();
        assert_eq!(handle.alloc_tx_batch(2, &mut buffers).await.unwrap(), 2);
        buffers[0].buffer_mut().extend_from_slice(b"one").unwrap();
        buffers[1].buffer_mut().extend_from_slice(b"two").unwrap();

        let destination = "127.0.0.1:9".parse::<SocketAddr>().unwrap();
        assert_eq!(
            handle
                .send_tx_buffers(&mut buffers, ActorTxMeta::new(destination))
                .await
                .unwrap(),
            2
        );
        assert!(buffers.is_empty());

        actor.shutdown().await.unwrap();
        assert_eq!(
            sent.lock().expect("sent lock").as_slice(),
            &[b"one".to_vec(), b"two".to_vec()]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn actor_delivers_rx_batches() {
        let layout = BufferLayout::new(2048);
        let mut rx = VecDeque::new();
        rx.push_back(UdpReceive::new(
            TestBuf::with_bytes(b"hello", layout),
            recv_meta(5),
        ));
        let actor =
            spawn_udp_actor(TestSocket::new(rx, 0), ActorConfig::default()).expect("spawn actor");
        let (handle, mut rx, join) = actor.into_parts();

        let mut batch = rx.recv_batch().await.unwrap();
        assert_eq!(batch.len(), 1);
        let packet = &batch.as_slice()[0];
        assert_eq!(packet.meta.len, 5);
        let mut bytes = [0u8; 5];
        packet.packet().read_at_exact(0, &mut bytes).unwrap();
        assert_eq!(&bytes, b"hello");
        batch.drain().for_each(drop);
        drop(batch);

        handle.shutdown().await.unwrap();
        drop(handle);
        drop(rx);
        join.await.unwrap().unwrap();
    }
}
