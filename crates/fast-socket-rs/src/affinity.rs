//! Thread-pinning helpers that place a worker on the core its socket wants.
//!
//! Sockets expose a preferred core through
//! [`UdpSocket::worker_affinity`](crate::UdpSocket::worker_affinity) /
//! [`IpPacketSocket::worker_affinity`](crate::IpPacketSocket::worker_affinity);
//! the free functions here consume that hint so callers stop hand-rolling
//! `sched_setaffinity`. Call them on the worker thread that will own the
//! socket.

use std::io;

use crate::{IpPacketSocket, QueueAffinity, UdpSocket};

/// Result of a thread-pinning request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum PinOutcome {
    /// The thread was pinned to the reported affinity.
    Pinned(QueueAffinity),
    /// The socket reported [`QueueAffinity::Any`]; nothing was pinned.
    NoHint,
}

/// Pins the current thread to the CPU(s) `socket` asks for.
///
/// Call on the worker thread that will own `socket`. Returns
/// [`PinOutcome::NoHint`] when the socket reports
/// [`QueueAffinity::Any`](crate::QueueAffinity::Any).
pub fn pin_current_thread_to_socket<S: UdpSocket>(socket: &S) -> io::Result<PinOutcome> {
    pin_current_thread_to_affinity(socket.worker_affinity())
}

/// Pins the current thread to the CPU(s) an IP-packet `socket` asks for.
///
/// Call on the worker thread that will own `socket`. Returns
/// [`PinOutcome::NoHint`] when the socket reports
/// [`QueueAffinity::Any`](crate::QueueAffinity::Any).
pub fn pin_current_thread_to_ip_packet_socket<S: IpPacketSocket>(
    socket: &S,
) -> io::Result<PinOutcome> {
    pin_current_thread_to_affinity(socket.worker_affinity())
}

/// Pins the current thread to the CPU(s) named by `affinity`.
///
/// [`QueueAffinity::Any`] is a no-op returning [`PinOutcome::NoHint`];
/// [`QueueAffinity::Core`] pins to that core and [`QueueAffinity::Mask`] pins
/// to every set bit. On non-Linux targets a concrete `Core`/`Mask` request is
/// unsupported and returns an error.
pub fn pin_current_thread_to_affinity(affinity: QueueAffinity) -> io::Result<PinOutcome> {
    match affinity {
        QueueAffinity::Any => Ok(PinOutcome::NoHint),
        QueueAffinity::Core(cpu) => {
            pin_current_thread_to_cpu(cpu)?;
            Ok(PinOutcome::Pinned(QueueAffinity::Core(cpu)))
        }
        QueueAffinity::Mask(mask) => {
            pin_current_thread_to_mask(mask)?;
            Ok(PinOutcome::Pinned(QueueAffinity::Mask(mask)))
        }
    }
}

/// Pins the current thread to a single CPU core.
#[cfg(target_os = "linux")]
pub fn pin_current_thread_to_cpu(cpu: u32) -> io::Result<()> {
    let cpu = usize::try_from(cpu)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "CPU index out of range"))?;
    // `CPU_SET` indexes a fixed-size bitmap (`cpu_set_t`, `CPU_SETSIZE` bits);
    // an index at/over that capacity would panic inside `CPU_SET`. Reject it
    // with a clean error instead. Real IRQ CPU ids are far below this.
    if cpu >= libc::CPU_SETSIZE as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "CPU index {cpu} exceeds CPU_SETSIZE ({})",
                libc::CPU_SETSIZE
            ),
        ));
    }
    // SAFETY: `set` is fully initialized by `CPU_ZERO`/`CPU_SET` before it is
    // passed to `sched_setaffinity`, and the size argument matches its type.
    #[allow(unsafe_code)]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

/// Pins the current thread to every CPU in `mask` (bit `n` => CPU `n`).
#[cfg(target_os = "linux")]
fn pin_current_thread_to_mask(mask: u64) -> io::Result<()> {
    if mask == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "empty CPU affinity mask",
        ));
    }
    // SAFETY: `set` is zero-initialized then populated through the documented
    // `CPU_SET` macro before being handed to `sched_setaffinity`.
    #[allow(unsafe_code)]
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_ZERO(&mut set);
        for bit in 0..u64::BITS {
            if mask & (1 << bit) != 0 {
                libc::CPU_SET(bit as usize, &mut set);
            }
        }
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

/// Pins the current thread to a single CPU core.
#[cfg(not(target_os = "linux"))]
pub fn pin_current_thread_to_cpu(cpu: u32) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        format!("thread pinning is not implemented on this target (requested CPU {cpu})"),
    ))
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_to_mask(_mask: u64) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "thread pinning is not implemented on this target",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn any_affinity_is_a_no_op() {
        assert_eq!(
            pin_current_thread_to_affinity(QueueAffinity::Any).unwrap(),
            PinOutcome::NoHint
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn core_affinity_pins_current_thread() {
        // CPU 0 exists on every Linux host the test runs on.
        let outcome = pin_current_thread_to_affinity(QueueAffinity::Core(0)).unwrap();
        assert_eq!(outcome, PinOutcome::Pinned(QueueAffinity::Core(0)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn empty_mask_is_rejected() {
        assert!(pin_current_thread_to_affinity(QueueAffinity::Mask(0)).is_err());
    }
}
