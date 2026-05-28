#[path = "../common.rs"]
mod common;

use std::fs;
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
use std::thread;
use std::time::Duration;

use clap::Parser;
use fast_socket_rs::{
    LinkAddr, NeighborTable, PacketBufferMut, QueueId, RouteHop, RouteTable, TxSlot,
    UdpSocket as FastUdpSocket, UdpTransmit, UdpTxBuffer, UdpTxBufferMut, V4Only,
};
use fast_socket_xdp_rs::{
    resolve_xdp_queue_slot, XdpEgress, XdpRouteContext, XdpUdpRouter, XdpUdpSocket,
};

use common::{
    dynamic_source_port, install_shutdown_signal_handlers, interface_ipv4_addr, shutdown_requested,
    BoxError,
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
    let slot = resolve_xdp_queue_slot(&args.device, QueueId::new(0))?;
    let local = SocketAddrV4::new(interface_ipv4_addr(&args.device)?, dynamic_source_port());
    let mtu = interface_mtu(&slot.iface)?;
    let router = CustomRouter {
        routes: DefaultRouteTable {
            ifindex: slot.ifindex,
        },
        arp: ConstantArpTable { mac: args.mac },
        src_mac: interface_mac(&slot.iface)?,
        mtu,
    };
    let mut socket = XdpUdpSocket::builder(slot.ifindex, slot.queue, local)
        .mtu(mtu as usize)
        .router(router)
        .open_busy_poll()?;
    let payload = payload();
    let mut sent = 0u64;

    eprintln!(
        "custom-router: sending 64-byte UDP payloads from {local} to {} via {:?} every second",
        args.target, args.mac
    );

    // Reuse the same scratch Vec across iterations so the 1 Hz sender does
    // not allocate per call. send_one only needs space for one packet.
    let mut tx_buffers: Vec<UdpTxBufferMut<XdpUdpSocket>> = Vec::with_capacity(1);
    let mut batch: Vec<TxSlot<UdpTransmit<UdpTxBuffer<XdpUdpSocket>>>> = Vec::with_capacity(1);

    while !shutdown_requested() {
        send_one(&mut socket, target, &payload, &mut tx_buffers, &mut batch)?;
        sent = sent.saturating_add(1);
        eprintln!("custom-router: sent {sent} packets");
        thread::sleep(SEND_INTERVAL);
    }

    socket.drain_tx_completions()?;
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
    raw.trim().parse().map_err(|error| format!("{error}").into())
}

fn interface_mtu(iface: &str) -> Result<u32, BoxError> {
    let raw = fs::read_to_string(format!("/sys/class/net/{iface}/mtu"))?;
    raw.trim().parse().map_err(Into::into)
}
