use std::net::Ipv4Addr;
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug)]
pub struct Args {
    values: Vec<String>,
}

impl Args {
    pub fn new() -> Self {
        Self {
            values: std::env::args().skip(1).collect(),
        }
    }

    pub fn mode(&mut self, usage: &str) -> Result<String, BoxError> {
        if self.values.is_empty() || self.values[0] == "--help" || self.values[0] == "-h" {
            return Err(usage.to_owned().into());
        }
        Ok(self.values.remove(0))
    }

    pub fn take(&mut self, name: &str) -> Option<String> {
        let position = self.values.iter().position(|value| value == name)?;
        self.values.remove(position);
        if position >= self.values.len() {
            return None;
        }
        Some(self.values.remove(position))
    }

    pub fn flag(&mut self, name: &str) -> bool {
        let Some(position) = self.values.iter().position(|value| value == name) else {
            return false;
        };
        self.values.remove(position);
        true
    }

    pub fn required<T>(&mut self, name: &str) -> Result<T, BoxError>
    where
        T: FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        let value = self
            .take(name)
            .ok_or_else(|| format!("missing required argument {name}"))?;
        Ok(value.parse()?)
    }

    pub fn optional<T>(&mut self, name: &str, default: T) -> Result<T, BoxError>
    where
        T: FromStr,
        T::Err: std::error::Error + Send + Sync + 'static,
    {
        match self.take(name) {
            Some(value) => Ok(value.parse()?),
            None => Ok(default),
        }
    }

    pub fn finish(self) -> Result<(), BoxError> {
        if self.values.is_empty() {
            Ok(())
        } else {
            Err(format!("unexpected arguments: {}", self.values.join(" ")).into())
        }
    }
}

impl Default for Args {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug)]
pub struct RunLimit {
    pub count: Option<u64>,
    pub duration: Option<Duration>,
}

impl RunLimit {
    pub fn from_args(args: &mut Args) -> Result<Self, BoxError> {
        let count = args
            .take("--count")
            .map(|value| value.parse())
            .transpose()?;
        let duration = args
            .take("--duration-ms")
            .map(|value| value.parse::<u64>().map(Duration::from_millis))
            .transpose()?;
        Ok(Self { count, duration })
    }

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

#[derive(Debug)]
pub struct Progress {
    name: &'static str,
    started: Instant,
    last: Instant,
    last_count: u64,
}

impl Progress {
    pub fn new(name: &'static str) -> Self {
        let now = Instant::now();
        Self {
            name,
            started: now,
            last: now,
            last_count: 0,
        }
    }

    pub fn tick(&mut self, count: u64) {
        if self.last.elapsed() < Duration::from_secs(1) {
            return;
        }
        let elapsed = self.last.elapsed().as_secs_f64();
        let rate = (count - self.last_count) as f64 / elapsed;
        eprintln!("{}: {count} packets ({rate:.0} packets/s)", self.name);
        self.last = Instant::now();
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

/// Reads `--timeout-ms`, returning `default_ms` when absent.
pub fn timeout_from_args(args: &mut Args, default_ms: u64) -> Result<Duration, BoxError> {
    let timeout_ms = args.optional("--timeout-ms", default_ms)?;
    Ok(Duration::from_millis(timeout_ms))
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

extern "C" fn handle_shutdown_signal(signal: libc::c_int) {
    if SHUTDOWN_SIGNALS.fetch_add(1, Ordering::SeqCst) > 0 {
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
