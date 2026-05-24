//! XDP/eBPF program loading and XSKMAP management.
//!
//! The embedded program redirects IPv4/IPv6 packets to `XSKMAP[rx_queue_index]`
//! while no UDP ports are bound. When `BOUND_PORTS` and `BOUND_PORT_COUNT` are
//! present and at least one UDP port is bound, it redirects only matching IPv4
//! UDP packets and leaves unrelated traffic on the kernel path. The loader also
//! accepts older quac-compatible objects that have only a UDP `BOUND_PORTS` map.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fmt;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io;
use std::os::fd::{AsRawFd, BorrowedFd};
use std::sync::{Arc, LockResult, Mutex, MutexGuard, OnceLock};

use aya::Ebpf;
use aya::maps::{Array as AyaArray, XskMap};
use aya::programs::{Xdp, XdpFlags};

pub use fast_socket_xdp_ebpf::{
    BOUND_PORT_COUNT_LEN, BOUND_PORTS_LEN, DROP_COUNTERS_LEN, DROP_REASON_UDP_FRAGMENT,
    DROP_REASON_UDP_OPTIONS, MAX_BOUND_PORTS, MAX_QUEUES,
};

/// XDP attach mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttachMode {
    /// Let the kernel choose the mode.
    Default,
    /// Generic SKB-mode XDP.
    Skb,
    /// Native driver-mode XDP.
    Drv,
    /// Hardware-offloaded XDP.
    Hw,
}

impl AttachMode {
    fn flags(self) -> XdpFlags {
        match self {
            Self::Default => XdpFlags::default(),
            Self::Skb => XdpFlags::SKB_MODE,
            Self::Drv => XdpFlags::DRV_MODE,
            Self::Hw => XdpFlags::HW_MODE,
        }
    }
}

/// Loaded XDP program plus userspace-managed redirect maps.
pub struct XdpProgram {
    ebpf: Ebpf,
    if_index: u32,
    program_hash: u64,
    bound_ports_available: bool,
    bound_port_count_available: bool,
    bound_ports: BTreeMap<u16, usize>,
    registered_queues: BTreeSet<u32>,
}

impl XdpProgram {
    fn load_and_attach(
        if_index: u32,
        mode: AttachMode,
        bytes: &[u8],
        program_hash: u64,
    ) -> io::Result<Self> {
        let mut ebpf = Ebpf::load(bytes).map_err(load_err)?;
        let bound_port_maps = validate_bound_port_maps(&mut ebpf)?;
        validate_xskmap(&mut ebpf)?;

        let program_name = if ebpf.program("fast_socket_xdp").is_some() {
            "fast_socket_xdp"
        } else {
            // The checked-in fallback object was copied from quac and still
            // carries this symbol. Accept it so older prebuilt objects work.
            "quac_xdp"
        };
        let program: &mut Xdp = ebpf
            .program_mut(program_name)
            .ok_or_else(|| {
                io::Error::other("eBPF object has no `fast_socket_xdp` or `quac_xdp` XDP program")
            })?
            .try_into()
            .map_err(load_err)?;
        program.load().map_err(load_err)?;
        program
            .attach_to_if_index(if_index, mode.flags())
            .map_err(|error| attach_err(if_index, mode, error))?;

        Ok(Self {
            ebpf,
            if_index,
            program_hash,
            bound_ports_available: bound_port_maps.ports_available,
            bound_port_count_available: bound_port_maps.count_available,
            bound_ports: BTreeMap::new(),
            registered_queues: BTreeSet::new(),
        })
    }

    /// Enables UDP destination-port redirection.
    pub fn bind_port(&mut self, port: u16) -> io::Result<()> {
        if !self.bound_ports_available {
            return Ok(());
        }
        let refcount = self.bound_ports.get(&port).copied().unwrap_or(0);
        if refcount == 0 {
            let map = self
                .ebpf
                .map_mut("BOUND_PORTS")
                .ok_or_else(|| io::Error::other("eBPF object has no `BOUND_PORTS` map"))?;
            let mut ports: AyaArray<_, u8> = AyaArray::try_from(map).map_err(io_err)?;
            ports.set(u32::from(port), 1u8, 0).map_err(io_err)?;
            let new_count = self.bound_ports.len() as u32 + 1;
            if let Err(error) = self.set_bound_port_count(new_count) {
                let map = self
                    .ebpf
                    .map_mut("BOUND_PORTS")
                    .ok_or_else(|| io::Error::other("eBPF object has no `BOUND_PORTS` map"))?;
                let mut ports: AyaArray<_, u8> = AyaArray::try_from(map).map_err(io_err)?;
                let _ = ports.set(u32::from(port), 0u8, 0);
                return Err(error);
            }
        }
        self.bound_ports.insert(port, refcount + 1);
        Ok(())
    }

    /// Disables UDP destination-port redirection when the last user drops it.
    pub fn unbind_port(&mut self, port: u16) -> io::Result<()> {
        if !self.bound_ports_available {
            return Ok(());
        }
        let Some(refcount) = self.bound_ports.get(&port).copied() else {
            return Ok(());
        };
        if refcount > 1 {
            self.bound_ports.insert(port, refcount - 1);
            return Ok(());
        }

        let map = self
            .ebpf
            .map_mut("BOUND_PORTS")
            .ok_or_else(|| io::Error::other("eBPF object has no `BOUND_PORTS` map"))?;
        let mut ports: AyaArray<_, u8> = AyaArray::try_from(map).map_err(io_err)?;
        ports.set(u32::from(port), 0u8, 0).map_err(io_err)?;
        self.bound_ports.remove(&port);
        self.set_bound_port_count(self.bound_ports.len() as u32)?;
        Ok(())
    }

    fn set_bound_port_count(&mut self, count: u32) -> io::Result<()> {
        if !self.bound_port_count_available {
            return Ok(());
        }
        let map = self
            .ebpf
            .map_mut("BOUND_PORT_COUNT")
            .ok_or_else(|| io::Error::other("eBPF object has no `BOUND_PORT_COUNT` map"))?;
        let mut counts: AyaArray<_, u32> = AyaArray::try_from(map).map_err(io_err)?;
        counts.set(0, count, 0).map_err(io_err)
    }

    /// Registers an AF_XDP socket fd in `XSKMAP[queue_id]`.
    pub fn register_socket(&mut self, queue_id: u32, socket_fd: BorrowedFd<'_>) -> io::Result<()> {
        let map = self
            .ebpf
            .map_mut("XSKMAP")
            .ok_or_else(|| io::Error::other("eBPF object has no `XSKMAP` map"))?;
        let mut xskmap: XskMap<_> = XskMap::try_from(map).map_err(io_err)?;
        xskmap
            .set(queue_id, socket_fd.as_raw_fd(), 0)
            .map_err(io_err)?;
        self.registered_queues.insert(queue_id);
        Ok(())
    }

    /// Drops local tracking for a registered queue.
    ///
    /// The kernel removes the XSKMAP fd reference when the AF_XDP socket fd is
    /// closed; aya 0.13 does not expose `XskMap::remove`.
    pub fn unregister_socket(&mut self, queue_id: u32) -> io::Result<()> {
        self.registered_queues.remove(&queue_id);
        Ok(())
    }

    /// Returns the interface index this program is attached to.
    #[must_use]
    pub const fn if_index(&self) -> u32 {
        self.if_index
    }
}

/// Reference-counted handle to an attached XDP program.
///
/// Clone this handle when several AF_XDP queue sockets need to share one
/// interface-level XDP attachment.
pub struct XdpProgramHandle {
    if_index: u32,
    program: Option<Arc<Mutex<XdpProgram>>>,
}

impl XdpProgramHandle {
    /// Gets the already-attached program for an interface or loads a new one.
    pub fn load(if_index: u32, mode: AttachMode, program_bytes: Option<&[u8]>) -> io::Result<Self> {
        let program = get_or_load(if_index, mode, program_bytes)?;
        Ok(Self {
            if_index,
            program: Some(program),
        })
    }

    /// Returns the interface index this program is attached to.
    #[must_use]
    pub const fn if_index(&self) -> u32 {
        self.if_index
    }

    pub(crate) fn lock(&self) -> LockResult<MutexGuard<'_, XdpProgram>> {
        self.program
            .as_ref()
            .expect("XDP program handle used after drop")
            .lock()
    }
}

impl Clone for XdpProgramHandle {
    fn clone(&self) -> Self {
        Self {
            if_index: self.if_index,
            program: Some(Arc::clone(
                self.program
                    .as_ref()
                    .expect("XDP program handle cloned after drop"),
            )),
        }
    }
}

impl fmt::Debug for XdpProgramHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XdpProgramHandle")
            .field("if_index", &self.if_index)
            .finish_non_exhaustive()
    }
}

impl Drop for XdpProgramHandle {
    fn drop(&mut self) {
        if let Some(program) = self.program.take() {
            release(self.if_index, program);
        }
    }
}

static PROGRAMS: OnceLock<Mutex<HashMap<u32, Arc<Mutex<XdpProgram>>>>> = OnceLock::new();

/// Returns the embedded AF_XDP redirect program object bytes.
#[must_use]
pub fn xdp_program_bytes() -> &'static [u8] {
    &fast_socket_xdp_ebpf::FAST_SOCKET_XDP_EBPF_PROGRAM.0
}

/// Alias for callers that want to load the embedded object manually.
#[must_use]
pub fn embedded_program_bytes() -> &'static [u8] {
    xdp_program_bytes()
}

/// Gets the already-attached program for an interface or loads a new one.
pub fn get_or_load(
    if_index: u32,
    mode: AttachMode,
    program_bytes: Option<&[u8]>,
) -> io::Result<Arc<Mutex<XdpProgram>>> {
    let bytes = match program_bytes {
        Some(bytes) => bytes,
        None => xdp_program_bytes(),
    };
    let new_hash = hash_program_bytes(bytes);
    let registry = PROGRAMS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = registry
        .lock()
        .expect("XDP program registry mutex poisoned");

    if let Some(existing) = map.get(&if_index) {
        let existing_hash = existing
            .lock()
            .expect("XDP program mutex poisoned")
            .program_hash;
        if existing_hash != new_hash {
            return Err(io::Error::other(format!(
                "XDP program mismatch on if_index {if_index}: existing hash \
                 {existing_hash:#018x}, supplied hash {new_hash:#018x}"
            )));
        }
        return Ok(Arc::clone(existing));
    }

    let program = XdpProgram::load_and_attach(if_index, mode, bytes, new_hash)?;
    let program = Arc::new(Mutex::new(program));
    map.insert(if_index, Arc::clone(&program));
    Ok(program)
}

/// Releases a program registry reference when the last queue user drops it.
pub fn release(if_index: u32, program: Arc<Mutex<XdpProgram>>) {
    let registry = PROGRAMS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = registry
        .lock()
        .expect("XDP program registry mutex poisoned");
    if Arc::strong_count(&program) == 2 {
        map.remove(&if_index);
    }
    drop(program);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BoundPortMaps {
    ports_available: bool,
    count_available: bool,
}

fn validate_bound_port_maps(ebpf: &mut Ebpf) -> io::Result<BoundPortMaps> {
    let Some(map) = ebpf.map_mut("BOUND_PORTS") else {
        if ebpf.map_mut("BOUND_PORT_COUNT").is_some() {
            return Err(io::Error::other(
                "eBPF object has `BOUND_PORT_COUNT` without `BOUND_PORTS`",
            ));
        }
        return Ok(BoundPortMaps {
            ports_available: false,
            count_available: false,
        });
    };
    let ports: AyaArray<_, u8> = AyaArray::try_from(map).map_err(io_err)?;
    if ports.len() != BOUND_PORTS_LEN {
        return Err(io::Error::other(format!(
            "eBPF map `BOUND_PORTS` has wrong length: expected {BOUND_PORTS_LEN}, got {}",
            ports.len()
        )));
    }

    let count_available = if let Some(map) = ebpf.map_mut("BOUND_PORT_COUNT") {
        let counts: AyaArray<_, u32> = AyaArray::try_from(map).map_err(io_err)?;
        if counts.len() != BOUND_PORT_COUNT_LEN {
            return Err(io::Error::other(format!(
                "eBPF map `BOUND_PORT_COUNT` has wrong length: expected {BOUND_PORT_COUNT_LEN}, got {}",
                counts.len()
            )));
        }
        true
    } else {
        false
    };

    Ok(BoundPortMaps {
        ports_available: true,
        count_available,
    })
}

fn validate_xskmap(ebpf: &mut Ebpf) -> io::Result<()> {
    let map = ebpf
        .map_mut("XSKMAP")
        .ok_or_else(|| io::Error::other("eBPF object missing required map `XSKMAP`"))?;
    let _: XskMap<_> = XskMap::try_from(map).map_err(io_err)?;
    Ok(())
}

fn hash_program_bytes(bytes: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    bytes.hash(&mut hasher);
    hasher.finish()
}

fn io_err<E: std::fmt::Display>(error: E) -> io::Error {
    io::Error::other(error.to_string())
}

fn load_err<E>(error: E) -> io::Error
where
    E: Error + 'static,
{
    let message = error_with_sources(&error);
    let lower = message.to_lowercase();
    if lower.contains("permission denied")
        || lower.contains("operation not permitted")
        || lower.contains("eperm")
        || lower.contains("eacces")
    {
        io::Error::other(format!(
            "{message}\n\
             hint: loading XDP / AF_XDP requires CAP_BPF + CAP_PERFMON \
             (kernel >= 5.8) or CAP_SYS_ADMIN."
        ))
    } else {
        io::Error::other(message)
    }
}

fn attach_err<E>(if_index: u32, mode: AttachMode, error: E) -> io::Error
where
    E: Error + 'static,
{
    let source_message = error_with_sources(&error);
    let mut message = format!(
        "failed to attach XDP program to if_index {if_index} with mode {mode:?}: {source_message}"
    );
    let lower = source_message.to_lowercase();
    if lower.contains("device or resource busy")
        || lower.contains("ebusy")
        || lower.contains("file exists")
        || lower.contains("eexist")
        || lower.contains("`bpf_link_create` failed")
    {
        message.push_str(
            "\nhint: only one XDP program can be attached to an interface at a time. \
             Use one process that opens all needed AF_XDP queues, or detach the existing \
             XDP program from this interface.",
        );
    }
    io::Error::other(message)
}

fn error_with_sources(error: &(dyn Error + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_program_exposes_udp_filter_maps() {
        let mut ebpf = match Ebpf::load(xdp_program_bytes()) {
            Ok(ebpf) => ebpf,
            Err(error) => {
                let message = error_with_sources(&error);
                let lower = message.to_lowercase();
                if lower.contains("operation not permitted")
                    || lower.contains("permission denied")
                    || lower.contains("eperm")
                {
                    return;
                }
                panic!("{message}");
            }
        };

        assert_eq!(
            validate_bound_port_maps(&mut ebpf).unwrap(),
            BoundPortMaps {
                ports_available: true,
                count_available: true,
            }
        );
        validate_xskmap(&mut ebpf).unwrap();
    }
}
