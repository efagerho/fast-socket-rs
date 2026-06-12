use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::Args as ClapArgs;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

// Common `--count` / `--duration-ms` stopping criteria for benchmark loops.
// Flatten this with `#[command(flatten)]` to share the contract across
// binaries. `keep_running` returns `true` while both limits (if set) are still
// satisfied; if neither is set, the loop runs until the caller stops it.
//
// Plain `//` comments here so clap doesn't inherit them into the parent
// command's `--help` when this struct is flattened.
#[derive(Clone, Copy, Debug, Default, ClapArgs)]
pub struct RunLimit {
    /// Stop after this many packets.
    #[arg(long)]
    pub count: Option<u64>,
    /// Stop after this many milliseconds of wall clock.
    #[arg(long = "duration-ms", value_parser = parse_duration_ms)]
    pub duration: Option<Duration>,
}

impl RunLimit {
    pub fn keep_running(self, completed: u64, started: Instant) -> bool {
        if self.count.is_some_and(|count| completed >= count) {
            return false;
        }
        if self
            .duration
            .is_some_and(|duration| started.elapsed() >= duration)
        {
            return false;
        }
        true
    }
}

fn parse_duration_ms(value: &str) -> Result<Duration, std::num::ParseIntError> {
    Ok(Duration::from_millis(value.parse()?))
}

#[derive(Debug)]
pub struct Progress {
    name: &'static str,
    started: Instant,
    last: Instant,
    last_count: u64,
    ticks_since_clock_check: u32,
}

impl Progress {
    /// Sample `Instant::now()` (a vdso `clock_gettime` call) at most once
    /// per this many ticks. At benchmark-scale call rates a per-tick clock
    /// sample showed up as ~7% of total CPU in single-thread profiles; the
    /// display gate only needs second-resolution timing, so reading the
    /// clock every few thousand ticks is plenty.
    const TICKS_BETWEEN_CLOCK_CHECKS: u32 = 1024;

    pub fn new(name: &'static str) -> Self {
        let now = Instant::now();
        Self {
            name,
            started: now,
            last: now,
            last_count: 0,
            ticks_since_clock_check: 0,
        }
    }

    pub fn tick(&mut self, count: u64) {
        // Throttle the clock sample: most ticks are no-ops after the
        // 1-second display gate below, so paying for a `clock_gettime`
        // every tick is pure overhead.
        self.ticks_since_clock_check = self.ticks_since_clock_check.wrapping_add(1);
        if self.ticks_since_clock_check < Self::TICKS_BETWEEN_CLOCK_CHECKS {
            return;
        }
        self.ticks_since_clock_check = 0;

        // Sample `now` once so the rate window matches the interval between
        // successive `self.last` values.
        let now = Instant::now();
        let elapsed = now.duration_since(self.last);
        if elapsed < Duration::from_secs(1) {
            return;
        }
        let rate = (count - self.last_count) as f64 / elapsed.as_secs_f64();
        eprintln!("{}: {count} packets ({rate:.0} packets/s)", self.name);
        self.last = now;
        self.last_count = count;
    }

    pub fn finish(&self, count: u64) {
        let elapsed = self.started.elapsed();
        let rate = if elapsed.is_zero() {
            0.0
        } else {
            count as f64 / elapsed.as_secs_f64()
        };
        println!(
            "{}: {count} packets in {elapsed:?} ({rate:.0} packets/s)",
            self.name
        );
    }
}

pub fn payload(len: usize) -> Vec<u8> {
    let mut payload = vec![0u8; len];
    for (index, byte) in payload.iter_mut().enumerate() {
        *byte = index as u8;
    }
    payload
}

const FIRST_DYNAMIC_PORT: u16 = 49152;

/// Returns a UDP source port derived from the current process ID, suitable for
/// example/benchmark binaries that need a non-conflicting ephemeral port. Not
/// collision-free across processes whose PIDs hash to the same slot.
pub fn dynamic_source_port() -> u16 {
    let range = u32::from(u16::MAX) - u32::from(FIRST_DYNAMIC_PORT) + 1;
    FIRST_DYNAMIC_PORT + (std::process::id() % range) as u16
}

pub fn write_sequence(payload: &mut [u8], sequence: u64) {
    let bytes = sequence.to_be_bytes();
    let len = payload.len().min(bytes.len());
    payload[..len].copy_from_slice(&bytes[..len]);
}

static SHUTDOWN_SIGNALS: AtomicUsize = AtomicUsize::new(0);

#[cfg(target_os = "linux")]
pub fn install_shutdown_signal_handlers() -> Result<(), BoxError> {
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = handle_shutdown_signal as *const () as usize;
        action.sa_flags = 0;
        libc::sigemptyset(&mut action.sa_mask);
        for signal in [libc::SIGINT, libc::SIGTERM] {
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn install_shutdown_signal_handlers() -> Result<(), BoxError> {
    Ok(())
}

#[inline]
pub fn shutdown_requested() -> bool {
    SHUTDOWN_SIGNALS.load(Ordering::Relaxed) > 0
}

/// Number of signal presses after which `handle_shutdown_signal` calls
/// `libc::_exit`. The first `FORCE_EXIT_PRESSES - 1` presses only set the
/// `SHUTDOWN_SIGNALS` flag so `shutdown_requested()` returns true and worker
/// loops can finish in-flight work and run their destructors. After that many
/// presses the handler force-exits unconditionally.
///
/// `libc::_exit` is the only async-signal-safe terminator available here — it
/// skips Rust stack destructors (so `XdpProgramHandle::drop` does not run),
/// but Linux automatically detaches unpinned XDP programs when the loading
/// process dies, so the kernel still cleans up after a forced exit.
const FORCE_EXIT_PRESSES: usize = 3;

extern "C" fn handle_shutdown_signal(signal: libc::c_int) {
    if SHUTDOWN_SIGNALS.fetch_add(1, Ordering::SeqCst) + 1 >= FORCE_EXIT_PRESSES {
        // SAFETY: `_exit` is async-signal-safe and takes only a single int.
        unsafe { libc::_exit(128 + signal) };
    }
}

#[cfg(target_os = "linux")]
pub fn pin_current_thread_to_cpu(cpu: u32) -> Result<(), BoxError> {
    let cpu = usize::try_from(cpu)?;
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        let result = libc::sched_setaffinity(
            0,
            std::mem::size_of::<libc::cpu_set_t>(),
            &set as *const libc::cpu_set_t,
        );
        if result == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error().into())
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn pin_current_thread_to_cpu(cpu: u32) -> Result<(), BoxError> {
    Err(format!("thread pinning is not implemented on this target (requested CPU {cpu})").into())
}

#[cfg(target_os = "linux")]
pub type XdpProgramMap =
    std::collections::BTreeMap<fast_socket_rs::IfIndex, fast_socket_xdp_rs::XdpProgramHandle>;

#[cfg(target_os = "linux")]
pub fn attach_xdp_programs_for_slots(
    slots: &[fast_socket_xdp_rs::XdpQueueSlot],
) -> Result<XdpProgramMap, BoxError> {
    let mut programs = XdpProgramMap::new();
    for slot in slots {
        if programs.contains_key(&slot.ifindex) {
            continue;
        }
        let program = fast_socket_xdp_rs::XdpProgramHandle::load(
            slot.ifindex.get(),
            fast_socket_xdp_rs::AttachMode::Default,
            None,
        )
        .map_err(|error| {
            format!(
                "attach XDP program to {} (if_index {}): {error}",
                slot.iface,
                slot.ifindex.get()
            )
        })?;
        programs.insert(slot.ifindex, program);
    }
    Ok(programs)
}

#[cfg(target_os = "linux")]
pub fn xdp_program_for_slot<'a>(
    programs: &'a XdpProgramMap,
    slot: &fast_socket_xdp_rs::XdpQueueSlot,
) -> Result<&'a fast_socket_xdp_rs::XdpProgramHandle, BoxError> {
    programs.get(&slot.ifindex).ok_or_else(|| {
        format!(
            "no pre-attached XDP program for {} (if_index {})",
            slot.iface,
            slot.ifindex.get()
        )
        .into()
    })
}

/// Builds an [`InterfaceSelector`](fast_socket_xdp_rs::InterfaceSelector) from
/// mutually-exclusive `--ifindex` / `--iface` CLI options.
pub fn interface_selector(
    ifindex: Option<u32>,
    iface: Option<String>,
) -> Result<fast_socket_xdp_rs::InterfaceSelector, BoxError> {
    match (ifindex, iface) {
        (Some(index), None) => Ok(fast_socket_xdp_rs::InterfaceSelector::Index(
            fast_socket_rs::IfIndex::new(index),
        )),
        (None, Some(name)) => Ok(fast_socket_xdp_rs::InterfaceSelector::Name(name)),
        (Some(_), Some(_)) => Err("use only one of --ifindex or --iface".into()),
        (None, None) => Err("missing --ifindex N or --iface NAME".into()),
    }
}

/// Parsed IPv4/UDP header fields for benchmark listener loops.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Ipv4UdpPacket {
    /// Source IPv4 address.
    pub source: Ipv4Addr,
    /// Destination IPv4 address.
    pub destination: Ipv4Addr,
    /// Source UDP port.
    pub source_port: u16,
    /// Destination UDP port.
    pub destination_port: u16,
    /// UDP payload byte offset inside the IP packet.
    pub payload_offset: usize,
    /// IPv4 total length.
    pub total_len: usize,
}

/// Parses an IPv4 UDP datagram.
pub fn parse_ipv4_udp(packet: &[u8]) -> Option<Ipv4UdpPacket> {
    if packet.len() < 28 || packet[0] >> 4 != 4 {
        return None;
    }
    let ihl = usize::from(packet[0] & 0x0f) * 4;
    if ihl < 20 || packet.len() < ihl + 8 {
        return None;
    }
    let total_len = usize::from(u16::from_be_bytes([packet[2], packet[3]]));
    if total_len < ihl + 8 || total_len > packet.len() || packet[9] != 17 {
        return None;
    }
    let fragment = u16::from_be_bytes([packet[6], packet[7]]);
    if fragment & 0x3fff != 0 {
        return None;
    }
    let udp_len = usize::from(u16::from_be_bytes([packet[ihl + 4], packet[ihl + 5]]));
    if udp_len < 8 || ihl + udp_len > total_len {
        return None;
    }
    Some(Ipv4UdpPacket {
        source: Ipv4Addr::new(packet[12], packet[13], packet[14], packet[15]),
        destination: Ipv4Addr::new(packet[16], packet[17], packet[18], packet[19]),
        source_port: u16::from_be_bytes([packet[ihl], packet[ihl + 1]]),
        destination_port: u16::from_be_bytes([packet[ihl + 2], packet[ihl + 3]]),
        payload_offset: ihl + 8,
        total_len,
    })
}

/// Swaps IPv4 and UDP endpoints in-place and returns the new destination IP.
pub fn reflect_ipv4_udp(packet: &mut [u8]) -> Option<Ipv4Addr> {
    let parsed = parse_ipv4_udp(packet)?;
    packet[12..16].copy_from_slice(&parsed.destination.octets());
    packet[16..20].copy_from_slice(&parsed.source.octets());
    let udp = parsed.payload_offset - 8;
    packet[udp..udp + 2].copy_from_slice(&parsed.destination_port.to_be_bytes());
    packet[udp + 2..udp + 4].copy_from_slice(&parsed.source_port.to_be_bytes());
    packet[udp + 6..udp + 8].copy_from_slice(&0u16.to_be_bytes());
    packet[10..12].copy_from_slice(&0u16.to_be_bytes());
    let checksum = ipv4_header_checksum(&packet[..udp]);
    packet[10..12].copy_from_slice(&checksum.to_be_bytes());
    Some(parsed.source)
}

fn ipv4_header_checksum(header: &[u8]) -> u16 {
    let mut sum = 0u32;
    for chunk in header.chunks_exact(2) {
        sum += u32::from(u16::from_be_bytes([chunk[0], chunk[1]]));
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
