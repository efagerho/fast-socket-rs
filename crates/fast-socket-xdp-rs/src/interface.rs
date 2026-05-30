//! Interface and RX-queue discovery helpers for AF_XDP sockets.
//!
//! AF_XDP binds to a concrete `(ifindex, queue_id)`. For Linux bond masters,
//! that concrete interface is one of the physical slaves, not the bond device
//! itself. These helpers expose a flat queue-slot view where a bond's queues are
//! the concatenation of each active slave's local RX queues in sysfs slave order.

use std::ffi::CString;
use std::fs;
use std::io;
use std::path::Path;

use fast_socket_rs::{IfIndex, NumaNode, QueueId};

/// One AF_XDP-bindable RX queue.
///
/// Construct via [`resolve_xdp_queue_slot`] or
/// [`xdp_queue_slots_for_interface`] — the `flat_index` field is meaningful
/// only when it comes from the system enumeration of the interface's queues,
/// and hand-fabricated slots were the root cause of past per-queue source-
/// port collisions. The `#[non_exhaustive]` attribute prevents direct
/// construction with field-init syntax.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct XdpQueueSlot {
    /// Interface name to bind to. For bonds this is the slave name.
    pub iface: String,
    /// Interface index to pass to AF_XDP.
    pub ifindex: IfIndex,
    /// Queue id local to `iface`.
    pub queue: QueueId,
    /// Position in the flattened queue list for the requested interface.
    pub flat_index: QueueId,
}

impl XdpQueueSlot {
    /// Builds a queue slot directly. Prefer the discovery helpers
    /// ([`resolve_xdp_queue_slot`], [`xdp_queue_slots_for_interface`]); this
    /// constructor exists for tests and for callers that have already done
    /// their own queue enumeration.
    #[must_use]
    pub fn new(iface: String, ifindex: IfIndex, queue: QueueId, flat_index: QueueId) -> Self {
        Self {
            iface,
            ifindex,
            queue,
            flat_index,
        }
    }
}

/// Resolves an interface name to its kernel ifindex.
pub fn if_name_to_index(name: &str) -> io::Result<IfIndex> {
    let name = CString::new(name).map_err(|_| io::Error::other("interface name contains NUL"))?;
    let index = unsafe { libc::if_nametoindex(name.as_ptr()) };
    if index == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(IfIndex::new(index))
    }
}

/// Resolves a kernel ifindex to its interface name.
pub fn if_index_to_name(ifindex: IfIndex) -> io::Result<String> {
    let mut buffer = [0u8; libc::IF_NAMESIZE];
    let result =
        unsafe { libc::if_indextoname(ifindex.get(), buffer.as_mut_ptr().cast::<libc::c_char>()) };
    if result.is_null() {
        return Err(io::Error::last_os_error());
    }
    let name = unsafe { std::ffi::CStr::from_ptr(result) };
    Ok(name.to_string_lossy().into_owned())
}

/// Returns bond slave names from `/sys/class/net/<iface>/bonding/slaves`.
///
/// Returns `Ok(None)` when `iface` exists but is not a bond master.
pub fn bond_slaves(iface: &str) -> io::Result<Option<Vec<String>>> {
    let iface_path = format!("/sys/class/net/{iface}");
    if !Path::new(&iface_path).exists() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("interface {iface} does not exist"),
        ));
    }

    let path = format!("{iface_path}/bonding/slaves");
    match fs::read_to_string(&path) {
        Ok(slaves) => Ok(Some(slaves.split_whitespace().map(String::from).collect())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!("read {path}: {error}"),
        )),
    }
}

/// Returns every AF_XDP-bindable queue for `iface`.
///
/// Non-bond interfaces return their own RX queues. Bond masters return all
/// usable slave RX queues in `/sys/class/net/<bond>/bonding/slaves` order; for
/// active-backup bonds only the current `active_slave` is bindable.
pub fn xdp_queue_slots_for_interface(iface: &str) -> io::Result<Vec<XdpQueueSlot>> {
    let mut slots = Vec::new();
    fill_queue_slots(iface, &mut slots)?;
    for (index, slot) in slots.iter_mut().enumerate() {
        slot.flat_index = QueueId::new(index as u32);
    }
    if slots.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{iface} has no RX queues"),
        ));
    }
    Ok(slots)
}

/// Resolves one flattened queue slot for `iface`.
///
/// For non-bonds, `flat_queue` is the local RX queue id. For bond masters, it
/// is the position in the concatenated slave queue list.
pub fn resolve_xdp_queue_slot(iface: &str, flat_queue: QueueId) -> io::Result<XdpQueueSlot> {
    let slots = xdp_queue_slots_for_interface(iface)?;
    let index = usize::try_from(flat_queue.get()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("queue id {} does not fit usize", flat_queue.get()),
        )
    })?;
    slots.get(index).cloned().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "--queue ({}) exceeds {} AF_XDP queue slots on {iface}",
                flat_queue.get(),
                slots.len()
            ),
        )
    })
}

/// Returns the single CPU handling interrupts for `slot`'s RX queue.
///
/// # Single-CPU requirement
///
/// This function returns an error rather than guessing when one RX queue's
/// IRQ is steered to multiple CPUs. The whole "busy-poll worker pinned per
/// queue" design assumes a 1:1 NIC-queue-to-CPU mapping so cache lines stay
/// hot and the kernel-side soft-IRQ does not race the AF_XDP user worker on
/// a different core. There is intentionally **no escape hatch**:
///
/// - If `irqbalance` is running, stop it (`systemctl stop irqbalance` and
///   `systemctl mask irqbalance`) or configure its policy file to leave the
///   AF_XDP NIC alone.
/// - If a manual `/proc/irq/{N}/smp_affinity[_list]` mask has more than one
///   bit set, write a single CPU id back to it before starting the workers.
///
/// Loosening this contract would couple every consumer to "which of the N
/// CPUs do we pick?" decisions that are workload-specific and easy to get
/// wrong silently. The error message at the caller surfaces exactly what
/// the operator needs to fix.
pub fn cpu_for_xdp_queue(slot: &XdpQueueSlot) -> io::Result<u32> {
    cpu_for_rx_queue(&slot.iface, slot.queue)
}

/// Returns the NUMA node reported for an interface's backing device.
pub fn numa_node_for_interface(iface: &str) -> io::Result<NumaNode> {
    let path = format!("/sys/class/net/{iface}/device/numa_node");
    let raw = fs::read_to_string(&path)
        .map_err(|error| io::Error::new(error.kind(), format!("read {path}: {error}")))?;
    parse_numa_node(raw.trim()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{iface} has unusable NUMA node {:?}: {error}", raw.trim()),
        )
    })
}

fn fill_queue_slots(iface: &str, slots: &mut Vec<XdpQueueSlot>) -> io::Result<()> {
    if let Some(slaves) = bond_slaves(iface)? {
        for slave in xdp_bindable_bond_slaves(iface, slaves)? {
            fill_queue_slots(&slave, slots)?;
        }
        return Ok(());
    }

    let count = rx_queue_count(iface)?;
    let ifindex = if_name_to_index(iface)?;
    for queue in 0..count {
        slots.push(XdpQueueSlot {
            iface: iface.to_owned(),
            ifindex,
            queue: QueueId::new(queue),
            flat_index: QueueId::new(0),
        });
    }
    Ok(())
}

fn xdp_bindable_bond_slaves(iface: &str, slaves: Vec<String>) -> io::Result<Vec<String>> {
    let selected = if bond_mode_is_active_backup(iface)? {
        let active = bond_active_slave(iface)?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("{iface} is active-backup but has no active slave"),
            )
        })?;
        if !slaves.iter().any(|slave| slave == &active) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{iface} active slave {active} is not listed in bonding/slaves"),
            ));
        }
        vec![active]
    } else {
        slaves
    };

    let mut bindable = Vec::new();
    for slave in selected {
        if interface_can_bind_xdp(&slave)? {
            bindable.push(slave);
        }
    }
    if bindable.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{iface} has no active bond slaves with usable RX queues"),
        ));
    }
    Ok(bindable)
}

fn bond_mode_is_active_backup(iface: &str) -> io::Result<bool> {
    let path = format!("/sys/class/net/{iface}/bonding/mode");
    let mode = fs::read_to_string(&path)
        .map_err(|error| io::Error::new(error.kind(), format!("read {path}: {error}")))?;
    Ok(is_active_backup_bond_mode(mode.trim()))
}

fn is_active_backup_bond_mode(mode: &str) -> bool {
    mode.split_whitespace()
        .any(|token| token == "active-backup" || token == "1")
}

fn bond_active_slave(iface: &str) -> io::Result<Option<String>> {
    let path = format!("/sys/class/net/{iface}/bonding/active_slave");
    match fs::read_to_string(&path) {
        Ok(active) => {
            let active = active.trim();
            Ok((!active.is_empty()).then(|| active.to_owned()))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!("read {path}: {error}"),
        )),
    }
}

fn interface_can_bind_xdp(iface: &str) -> io::Result<bool> {
    let path = format!("/sys/class/net/{iface}/operstate");
    match fs::read_to_string(&path) {
        Ok(state) => Ok(!matches!(state.trim(), "down" | "lowerlayerdown")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(io::Error::new(
            error.kind(),
            format!("read {path}: {error}"),
        )),
    }
}

fn rx_queue_count(iface: &str) -> io::Result<u32> {
    let dir = format!("/sys/class/net/{iface}/queues");
    let entries = fs::read_dir(&dir)
        .map_err(|error| io::Error::new(error.kind(), format!("read_dir {dir}: {error}")))?;
    let mut count = 0u32;
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name
            .strip_prefix("rx-")
            .is_some_and(|suffix| suffix.parse::<u32>().is_ok())
        {
            count += 1;
        }
    }
    if count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("{iface} has no rx-* queues"),
        ));
    }
    Ok(count)
}

fn cpu_for_rx_queue(iface: &str, queue: QueueId) -> io::Result<u32> {
    let irq = irq_for_rx_queue(iface, queue)?;
    let path = format!("/proc/irq/{irq}/smp_affinity_list");
    let raw = fs::read_to_string(&path)
        .map_err(|error| io::Error::new(error.kind(), format!("read {path}: {error}")))?;
    parse_single_cpu_affinity_list(raw.trim()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "{iface} rx-{} IRQ {irq} has affinity list {:?} ({error}); \
                 each NIC RX queue must be pinned to exactly one CPU before \
                 xdp-listener starts. Stop irqbalance or configure it to leave \
                 this NIC alone, then write one CPU id to {path}.",
                queue.get(),
                raw.trim()
            ),
        )
    })
}

fn irq_for_rx_queue(iface: &str, queue: QueueId) -> io::Result<u32> {
    // Prefer the structured sysfs path: for any NIC that publishes MSI-X
    // IRQs (every modern multi-queue card does), `/sys/class/net/{iface}/
    // device/msi_irqs/` lists the IRQ numbers and the per-IRQ
    // `/proc/irq/{N}` directory contains an "action" file or per-driver
    // descriptor file naming the queue. If sysfs doesn't yield an answer we
    // fall back to scraping `/proc/interrupts` with a generalized token
    // matcher (no per-vendor special cases).
    if let Some(irq) = irq_from_sysfs(iface, queue)? {
        return Ok(irq);
    }
    irq_from_proc_interrupts(iface, queue)
}

/// Walk `/sys/class/net/{iface}/device/msi_irqs/` (each subentry name is an
/// IRQ number) and for each IRQ check whether `/proc/irq/{irq}/` carries an
/// "actions" entry or a per-driver subdirectory whose name encodes the queue
/// id. Returns `Ok(None)` if the sysfs path is absent (non-PCIe device,
/// virtio without MSI-X) so the caller can fall back.
fn irq_from_sysfs(iface: &str, queue: QueueId) -> io::Result<Option<u32>> {
    let dir = format!("/sys/class/net/{iface}/device/msi_irqs");
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(io::Error::new(error.kind(), format!("read {dir}: {error}")));
        }
    };
    let queue_str = queue.get().to_string();
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str() else {
            continue;
        };
        let Ok(irq) = name.parse::<u32>() else {
            continue;
        };
        if irq_descriptor_names_queue(irq, iface, &queue_str)? {
            return Ok(Some(irq));
        }
    }
    Ok(None)
}

/// `/proc/irq/{irq}/` may contain a file named `actions` (newer kernels) or a
/// subdirectory per registered handler. Either way the names embed the queue
/// id when the driver registered MSI-X per RX queue, so we re-use the same
/// token matcher used for `/proc/interrupts` lines.
fn irq_descriptor_names_queue(irq: u32, iface: &str, queue_str: &str) -> io::Result<bool> {
    let actions_path = format!("/proc/irq/{irq}/actions");
    match fs::read_to_string(&actions_path) {
        Ok(actions) => {
            for token in actions.split(',') {
                if token_names_iface_queue(token.trim(), iface, queue_str) {
                    return Ok(true);
                }
            }
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(io::Error::new(
                error.kind(),
                format!("read {actions_path}: {error}"),
            ));
        }
    }

    let dir = format!("/proc/irq/{irq}");
    let entries = match fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(io::Error::new(error.kind(), format!("read {dir}: {error}")));
        }
    };
    for entry in entries {
        let entry = entry?;
        let file_name = entry.file_name();
        if let Some(name) = file_name.to_str()
            && token_names_iface_queue(name, iface, queue_str)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn irq_from_proc_interrupts(iface: &str, queue: QueueId) -> io::Result<u32> {
    let raw = fs::read_to_string("/proc/interrupts")
        .map_err(|error| io::Error::new(error.kind(), format!("read /proc/interrupts: {error}")))?;
    let queue = queue.get();
    let queue_string = queue.to_string();
    for line in raw.lines() {
        let line = line.trim_start();
        let Some((head, rest)) = line.split_once(':') else {
            continue;
        };
        let Ok(irq) = head.trim().parse::<u32>() else {
            continue;
        };
        for token in rest.split_whitespace() {
            if token_names_iface_queue(token, iface, &queue_string) {
                return Ok(irq);
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("no IRQ in /proc/interrupts matches {iface} rx-{queue}"),
    ))
}

/// Returns true when `token` is the IRQ handler name for `iface` RX queue
/// `queue_str`. Generalized over the per-vendor naming conventions:
/// - `iface-{queue}`            (Intel `ice`, `i40e`)
/// - `iface.{queue}`            (some Broadcom)
/// - `iface@{queue}`            (Mellanox short form)
/// - `*comp{queue}*iface*`      (Mellanox `mlx5_comp42@eth0`)
/// - `bnxt_en_{queue}_iface`    (Broadcom long form)
///
/// One alphanumeric sub-part (split on `-`, `.`, `@`, `_`) must be **exactly**
/// `iface` AND another must be exactly `queue_str` or end in `queue_str` with an
/// alphabetic prefix (e.g., `comp42`). The interface match is on a whole
/// delimiter-split component rather than a substring, so `eth1` does not match a
/// token belonging to `eth10`.
fn token_names_iface_queue(token: &str, iface: &str, queue_str: &str) -> bool {
    let delimiters = ['-', '.', '@', '_'];
    if !token.split(delimiters).any(|part| part == iface) {
        return false;
    }
    token.split(delimiters).any(|part| {
        if part == queue_str {
            return true;
        }
        let Some(prefix) = part.strip_suffix(queue_str) else {
            return false;
        };
        !prefix.is_empty() && prefix.chars().all(|c| c.is_ascii_alphabetic())
    })
}

fn parse_numa_node(raw: &str) -> Result<NumaNode, String> {
    let node: i32 = raw
        .parse()
        .map_err(|error| format!("bad NUMA node value {raw:?}: {error}"))?;
    if node < 0 {
        return Err("kernel reported an unknown NUMA node".to_string());
    }
    let node = u16::try_from(node).map_err(|_| format!("NUMA node {node} does not fit u16"))?;
    Ok(NumaNode::new(node))
}

fn parse_single_cpu_affinity_list(list: &str) -> Result<u32, String> {
    let mut cpu = None;
    for part in list
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        match part.split_once('-') {
            Some((start, end)) => {
                let start: u32 = start
                    .parse()
                    .map_err(|error| format!("bad range start {part:?}: {error}"))?;
                let end: u32 = end
                    .parse()
                    .map_err(|error| format!("bad range end {part:?}: {error}"))?;
                if end < start {
                    return Err(format!("inverted range {part:?}"));
                }
                for candidate in start..=end {
                    if cpu.replace(candidate).is_some() {
                        return Err(format!("affinity covers more than one CPU ({list:?})"));
                    }
                }
            }
            None => {
                let candidate: u32 = part
                    .parse()
                    .map_err(|error| format!("bad CPU id {part:?}: {error}"))?;
                if cpu.replace(candidate).is_some() {
                    return Err(format!("affinity covers more than one CPU ({list:?})"));
                }
            }
        }
    }
    cpu.ok_or_else(|| format!("empty affinity list {list:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn non_bond_loopback_has_no_slaves() {
        assert_eq!(bond_slaves("lo").expect("bond_slaves(lo)"), None);
    }

    #[test]
    fn loopback_queue_slots_resolve() {
        let slots = xdp_queue_slots_for_interface("lo").expect("loopback queue slots");
        assert!(!slots.is_empty());
        assert_eq!(slots[0].iface, "lo");
        assert_eq!(slots[0].flat_index, QueueId::new(0));
    }

    #[test]
    fn loopback_ifindex_resolves_to_name() {
        assert_eq!(
            if_index_to_name(IfIndex::new(1)).expect("if_indextoname(1)"),
            "lo"
        );
    }

    #[test]
    fn single_cpu_affinity_parser_rejects_multi_cpu_masks() {
        assert_eq!(parse_single_cpu_affinity_list("3").unwrap(), 3);
        assert!(parse_single_cpu_affinity_list("0-1").is_err());
        assert!(parse_single_cpu_affinity_list("1,3").is_err());
    }

    #[test]
    fn active_backup_bond_mode_parser_accepts_name_or_number() {
        assert!(is_active_backup_bond_mode("active-backup 1"));
        assert!(is_active_backup_bond_mode("1"));
        assert!(!is_active_backup_bond_mode("802.3ad 4"));
        assert!(!is_active_backup_bond_mode("balance-rr 0"));
    }

    #[test]
    fn numa_node_parser_rejects_unknown_nodes() {
        assert_eq!(parse_numa_node("0").unwrap(), NumaNode::new(0));
        assert_eq!(parse_numa_node("12").unwrap(), NumaNode::new(12));
        assert!(parse_numa_node("-1").is_err());
        assert!(parse_numa_node("not-a-node").is_err());
    }
}
