#[path = "../common.rs"]
mod common;

use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use clap::Parser;
use fast_socket_rs::{
    LinkAddr, NeighborTable, PacketBufferMut, RouteHop, RouteTable, TxSlot,
    UdpSocket as FastUdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut, V4Only,
};
use fast_socket_xdp_rs::{
    InterfaceSelector, PortFilter, XdpEgress, XdpFactoryBuilder, XdpRouteContext, XdpUdpRouter,
    XdpUdpSocket,
};

use common::{
    BoxError, dynamic_source_port, install_shutdown_signal_handlers, interface_ipv4_addr,
    shutdown_requested,
};

const PAYLOAD_LEN: usize = 64;
const SEND_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug, Parser)]
struct Args {
    /// Device name to attach to.
    #[arg(long)]
    device: String,

    /// Target UDP endpoint as IPv4:PORT.
    #[arg(long)]
    target: SocketAddrV4,

    /// Destination MAC address used for every routed IP.
    #[arg(long)]
    mac: LinkAddr,

    /// Number of worker threads. All NIC queues are used and split into this
    /// many contiguous blocks; each thread drives one aggregate socket (with the
    /// custom router) over its queues/threads queues. Must divide the queue count.
    #[arg(long, default_value_t = 1)]
    threads: usize,
}

#[derive(Clone, Copy, Debug)]
struct DefaultRouteTable {
    ifindex: fast_socket_rs::IfIndex,
}

impl RouteTable<V4Only> for DefaultRouteTable {
    fn resolve_route(&self, _dst: Ipv4Addr) -> Option<RouteHop<Ipv4Addr>> {
        Some(RouteHop {
            ifindex: self.ifindex,
            next_hop: Ipv4Addr::UNSPECIFIED,
        })
    }
}

#[derive(Clone, Copy, Debug)]
struct ConstantArpTable {
    mac: LinkAddr,
}

impl NeighborTable<V4Only> for ConstantArpTable {
    fn resolve_l2(&self, _next_hop: Ipv4Addr) -> Option<LinkAddr> {
        Some(self.mac)
    }
}

#[derive(Clone, Copy, Debug)]
struct CustomRouter {
    routes: DefaultRouteTable,
    arp: ConstantArpTable,
    src_mac: LinkAddr,
    mtu: u32,
}

impl XdpUdpRouter for CustomRouter {
    fn resolve_udp_egress(&self, dst: Ipv4Addr, context: XdpRouteContext) -> Option<XdpEgress> {
        let route = self.routes.resolve_route(dst)?;
        if route.ifindex != context.ifindex {
            return None;
        }
        let dst_mac = self.arp.resolve_l2(route.next_hop)?;
        Some(XdpEgress::ipv4(
            context.ifindex,
            context.queue,
            dst_mac,
            self.src_mac,
            self.mtu.min(context.mtu as u32),
        ))
    }
}

fn main() -> Result<(), BoxError> {
    install_shutdown_signal_handlers()?;
    let args = Args::parse();
    let target: SocketAddr = args.target.into();
    let local = SocketAddrV4::new(interface_ipv4_addr(&args.device)?, dynamic_source_port());
    let mtu = interface_mtu(&args.device)?;
    let src_mac = interface_mac(&args.device)?;
    // Phase 1: discover queues, attach the program, partition into `threads`
    // worker plans (one aggregate socket each).
    let factory = XdpFactoryBuilder::new(InterfaceSelector::Name(args.device.clone()))?
        .threads(args.threads)
        .port_filter(PortFilter::UdpPorts(vec![local.port()]))
        .mtu(mtu as usize)
        .build()?;
    let plans = factory.into_worker_plans();
    eprintln!(
        "custom-router: {} aggregate socket(s) / thread(s) sending 64-byte UDP payloads from \
         {local} to {} via {:?} every second",
        plans.len(),
        args.target,
        args.mac
    );

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::with_capacity(plans.len());
    for plan in plans {
        let stop = Arc::clone(&stop);
        let mac = args.mac;
        handles.push(thread::spawn(move || -> Result<(), String> {
            let router = CustomRouter {
                routes: DefaultRouteTable {
                    ifindex: plan.ifindex(),
                },
                arp: ConstantArpTable { mac },
                src_mac,
                mtu,
            };
            // Phase 2: pin + open this worker's aggregate with the custom router.
            let mut aggregate = plan
                .open_udp_busy_poll_with_router(local, || router)
                .map_err(|error| error.to_string())?;
            run_custom_router(&mut aggregate, target, &stop).map_err(|error| error.to_string())
        }));
    }

    while !shutdown_requested() {
        thread::sleep(Duration::from_millis(200));
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error.into()),
            Err(_) => return Err("custom-router worker thread panicked".into()),
        }
    }
    Ok(())
}

/// A UDP socket using the [`CustomRouter`], and its TX scratch types.
type RouterSocket = XdpUdpSocket<fast_socket_rs::BusyPollDriver, CustomRouter>;
type RouterAggregate =
    fast_socket_xdp_rs::XdpUdpAggregate<fast_socket_rs::BusyPollDriver, CustomRouter>;
type RouterTxBufMut = UdpTxBufferMut<RouterSocket>;
type RouterTxItem = TxSlot<UdpTransmit<UdpTxBuffer<RouterSocket>>>;

/// Sends one 64-byte payload per member queue every [`SEND_INTERVAL`] until
/// `stop`, exercising the custom router on each member.
fn run_custom_router(
    aggregate: &mut RouterAggregate,
    target: SocketAddr,
    stop: &AtomicBool,
) -> Result<(), BoxError> {
    let payload = payload();
    let mut tx_buffers: Vec<RouterTxBufMut> = Vec::with_capacity(1);
    let mut batch: Vec<RouterTxItem> = Vec::with_capacity(1);
    while !stop.load(Ordering::Relaxed) && !shutdown_requested() {
        for socket in aggregate.members_mut() {
            send_one(socket, target, &payload, &mut tx_buffers, &mut batch)?;
            socket.drain_tx_completions()?;
        }
        thread::sleep(SEND_INTERVAL);
    }
    Ok(())
}

fn send_one<S>(
    socket: &mut S,
    target: SocketAddr,
    payload: &[u8; PAYLOAD_LEN],
    tx_buffers: &mut Vec<UdpTxBufferMut<S>>,
    batch: &mut Vec<TxSlot<UdpTransmit<UdpTxBuffer<S>>>>,
) -> Result<(), BoxError>
where
    S: FastUdpSocket,
{
    // 1 Hz sender, so back-pressure is essentially never hit. When it is, a
    // tiny sleep yields the core; spin_loop would burn the CPU between the
    // 1 s send intervals for no benefit.
    const BACKPRESSURE_SLEEP: Duration = Duration::from_micros(100);

    while !shutdown_requested() {
        tx_buffers.clear();
        if socket.allocate_tx_batch(tx_buffers, 1)? == 0 {
            socket.drain_tx_completions()?;
            thread::sleep(BACKPRESSURE_SLEEP);
            continue;
        }

        let mut packet = tx_buffers.pop().expect("one TX buffer was allocated");
        packet.extend_from_slice(payload)?;
        batch.clear();
        batch.push(TxSlot::Ready(UdpTransmit::new(packet.freeze(), target)));
        match socket.send(batch.as_mut_slice())? {
            1 => {
                socket.drain_tx_completions()?;
                return Ok(());
            }
            0 => {
                socket.drain_tx_completions()?;
                thread::sleep(BACKPRESSURE_SLEEP);
            }
            _ => unreachable!("single-packet batch cannot accept more than one packet"),
        }
    }
    Ok(())
}

fn payload() -> [u8; PAYLOAD_LEN] {
    let mut payload = [0u8; PAYLOAD_LEN];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = index as u8;
    }
    payload
}

fn interface_mac(iface: &str) -> Result<LinkAddr, BoxError> {
    let raw = fs::read_to_string(format!("/sys/class/net/{iface}/address"))?;
    raw.trim()
        .parse()
        .map_err(|error| format!("{error}").into())
}

fn interface_mtu(iface: &str) -> Result<u32, BoxError> {
    let raw = fs::read_to_string(format!("/sys/class/net/{iface}/mtu"))?;
    raw.trim().parse().map_err(Into::into)
}
