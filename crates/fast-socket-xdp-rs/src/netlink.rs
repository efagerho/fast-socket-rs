//! Netlink route, link, and neighbor table helpers for XDP egress resolution.

use std::collections::HashMap;
use std::io;
use std::mem;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::ptr;
use std::slice;

use fast_socket_rs::{IfIndex, LinkAddr};

const NETLINK_RCVBUF_SIZE: i32 = 1 << 16;
const NLMSG_ALIGNTO: usize = 4;
const NLA_ALIGNTO: usize = 4;
const NLA_HDR_LEN: usize = align_to(mem::size_of::<libc::nlattr>(), NLA_ALIGNTO);

#[repr(C)]
#[allow(non_camel_case_types)]
struct ifinfomsg {
    ifi_family: u8,
    __ifi_pad: u8,
    ifi_type: u16,
    ifi_index: i32,
    ifi_flags: u32,
    ifi_change: u32,
}

#[repr(C)]
#[allow(non_camel_case_types)]
struct ndmsg {
    ndm_family: u8,
    _ndm_pad1: u8,
    _ndm_pad2: u16,
    ndm_ifindex: i32,
    ndm_state: u16,
    _ndm_flags: u8,
    _ndm_type: u8,
}

#[repr(C)]
#[allow(non_camel_case_types)]
struct rtmsg {
    rtm_family: u8,
    rtm_dst_len: u8,
    rtm_src_len: u8,
    rtm_tos: u8,
    rtm_table: u8,
    rtm_protocol: u8,
    rtm_scope: u8,
    rtm_type: u8,
    rtm_flags: u32,
}

#[repr(C)]
struct LinkRequest {
    header: libc::nlmsghdr,
    ifi: ifinfomsg,
}

#[repr(C)]
struct NeighborRequest {
    header: libc::nlmsghdr,
    ndm: ndmsg,
}

#[repr(C)]
struct RouteRequest {
    header: libc::nlmsghdr,
    rtm: rtmsg,
}

/// Netlink route socket.
#[derive(Debug)]
pub struct NetlinkSocket {
    fd: OwnedFd,
}

impl NetlinkSocket {
    /// Opens an unbound route netlink socket.
    pub fn open() -> io::Result<Self> {
        // SAFETY: socket returns an owned fd on success.
        let fd = unsafe { libc::socket(libc::AF_NETLINK, libc::SOCK_RAW, libc::NETLINK_ROUTE) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fd was just returned by socket and is uniquely owned.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };

        let rcvbuf = NETLINK_RCVBUF_SIZE;
        // SAFETY: rcvbuf points to a valid i32 for the duration of setsockopt.
        let _ = unsafe {
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                (&rcvbuf as *const i32).cast(),
                mem::size_of::<i32>() as libc::socklen_t,
            )
        };

        Ok(Self { fd })
    }

    /// Binds a route netlink socket to multicast `groups`.
    pub fn bind(groups: u32) -> io::Result<Self> {
        let socket = Self::open()?;
        // SAFETY: zeroed is a valid baseline for sockaddr_nl; fields are set below.
        let mut addr: libc::sockaddr_nl = unsafe { mem::zeroed() };
        addr.nl_family = libc::AF_NETLINK as libc::sa_family_t;
        addr.nl_pid = 0;
        addr.nl_groups = groups;
        // SAFETY: addr is a valid sockaddr_nl for the duration of bind.
        let rc = unsafe {
            libc::bind(
                socket.fd.as_raw_fd(),
                (&addr as *const libc::sockaddr_nl).cast(),
                mem::size_of::<libc::sockaddr_nl>() as libc::socklen_t,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(socket)
    }

    /// Sends one netlink request buffer.
    pub fn send(&self, message: &[u8]) -> io::Result<()> {
        // SAFETY: message points to initialized bytes and send does not retain it.
        let rc = unsafe {
            libc::send(
                self.fd.as_raw_fd(),
                message.as_ptr().cast(),
                message.len(),
                0,
            )
        };
        if rc < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Receives a full netlink dump response, requiring that every accepted
    /// message belong to dump `expected_seq` and that the kernel terminate
    /// the dump with `NLMSG_DONE`. Messages with a different sequence number
    /// (e.g., unsolicited multicast events delivered on a shared socket) are
    /// silently dropped; an `EOF`-style early termination is reported as an
    /// `Other` error so the caller can distinguish it from a successful dump
    /// that happens to be empty.
    pub fn recv(&self, expected_seq: u32) -> io::Result<Vec<NetlinkMessage>> {
        // 32 KiB matches the kernel's default netlink message size; smaller
        // buffers would force callers with many routes/neighbors to handle
        // truncation explicitly.
        let mut buf = [0u8; 32768];
        let mut messages = Vec::new();

        loop {
            // SAFETY: buf is writable and recv does not retain it.
            let len = unsafe {
                libc::recv(
                    self.fd.as_raw_fd(),
                    buf.as_mut_ptr().cast(),
                    buf.len(),
                    libc::MSG_TRUNC,
                )
            };
            if len < 0 {
                return Err(io::Error::last_os_error());
            }
            if len == 0 {
                return Err(io::Error::other(
                    "netlink dump closed before NLMSG_DONE was received",
                ));
            }
            let len = len as usize;
            if len > buf.len() {
                return Err(io::Error::other("netlink datagram truncated"));
            }

            let mut offset = 0;
            while offset < len {
                let message = NetlinkMessage::read(&buf[offset..len])?;
                let aligned_len = align_to(message.header.nlmsg_len as usize, NLMSG_ALIGNTO);
                offset = offset.saturating_add(aligned_len);

                if message.header.nlmsg_seq != expected_seq {
                    // Multicast events or stale dump replies share this
                    // socket; ignore them so they don't contaminate the
                    // current dump's response set.
                    continue;
                }

                match message.header.nlmsg_type as i32 {
                    libc::NLMSG_DONE => return Ok(messages),
                    libc::NLMSG_ERROR => {
                        if let Some(error) = message.error {
                            if error.error == 0 {
                                continue;
                            }
                            return Err(io::Error::from_raw_os_error(-error.error));
                        }
                    }
                    _ => messages.push(message),
                }
            }
        }
    }

    /// Returns the raw file descriptor.
    #[must_use]
    pub fn as_raw_fd(&self) -> RawFd {
        self.fd.as_raw_fd()
    }
}

/// Parsed netlink message.
#[derive(Debug, Clone)]
pub struct NetlinkMessage {
    /// Netlink message header.
    pub header: libc::nlmsghdr,
    data: Vec<u8>,
    error: Option<libc::nlmsgerr>,
}

impl NetlinkMessage {
    fn read(buf: &[u8]) -> io::Result<Self> {
        if mem::size_of::<libc::nlmsghdr>() > buf.len() {
            return Err(io::Error::other("buffer smaller than nlmsghdr"));
        }
        // SAFETY: nlmsghdr is POD and read_unaligned handles alignment.
        let header = unsafe { ptr::read_unaligned(buf.as_ptr().cast::<libc::nlmsghdr>()) };
        let msg_len = header.nlmsg_len as usize;
        if msg_len < mem::size_of::<libc::nlmsghdr>() || msg_len > buf.len() {
            return Err(io::Error::other("invalid nlmsg_len"));
        }

        let data_offset = align_to(mem::size_of::<libc::nlmsghdr>(), NLMSG_ALIGNTO);
        let (data, error) = if header.nlmsg_type == libc::NLMSG_ERROR as u16 {
            if data_offset + mem::size_of::<libc::nlmsgerr>() > msg_len {
                return Err(io::Error::other("NLMSG_ERROR missing nlmsgerr"));
            }
            (
                Vec::new(),
                // SAFETY: nlmsgerr is POD and read_unaligned handles alignment.
                Some(unsafe {
                    ptr::read_unaligned(buf[data_offset..].as_ptr().cast::<libc::nlmsgerr>())
                }),
            )
        } else {
            (buf[data_offset..msg_len].to_vec(), None)
        };

        Ok(Self {
            header,
            data,
            error,
        })
    }
}

/// Link/interface facts from netlink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LinkEntry {
    /// Interface index.
    pub ifindex: IfIndex,
    /// Master interface index for enslaved links.
    pub master_ifindex: Option<IfIndex>,
    /// Interface MAC address, if netlink exposed it.
    pub mac: Option<LinkAddr>,
    /// Interface MTU, if netlink exposed it.
    pub mtu: Option<u32>,
}

/// Neighbor table entry from netlink ARP/NDP state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NeighborEntry {
    /// Neighbor IP address.
    pub destination: Option<IpAddr>,
    /// Neighbor link-layer address.
    pub lladdr: Option<LinkAddr>,
    /// Interface index owning this neighbor.
    pub ifindex: IfIndex,
    /// NUD state bits.
    pub state: u16,
}

/// Route table entry from netlink.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RouteEntry {
    /// Destination prefix address; `None` means default route.
    pub destination: Option<IpAddr>,
    /// Gateway address when this route uses a next hop.
    pub gateway: Option<IpAddr>,
    /// Output interface index.
    pub out_ifindex: Option<IfIndex>,
    /// Route priority/metric.
    pub priority: Option<u32>,
    /// Route table id.
    pub table: Option<u32>,
    /// Address family.
    pub family: u8,
    /// Destination prefix length.
    pub dst_len: u8,
}

/// Dumps link/interface information through netlink.
pub fn netlink_get_links(family: u8) -> io::Result<Vec<LinkEntry>> {
    let socket = NetlinkSocket::open()?;
    // SAFETY: LinkRequest is a plain C request struct.
    let mut request = unsafe { mem::zeroed::<LinkRequest>() };
    let len = mem::size_of::<libc::nlmsghdr>() + mem::size_of::<ifinfomsg>();
    request.header = libc::nlmsghdr {
        nlmsg_len: len as u32,
        nlmsg_type: libc::RTM_GETLINK,
        nlmsg_flags: (libc::NLM_F_REQUEST | libc::NLM_F_DUMP) as u16,
        nlmsg_seq: 1,
        nlmsg_pid: 0,
    };
    request.ifi.ifi_family = family;
    socket.send(&bytes_of(&request)[..len])?;

    let mut links = Vec::new();
    for message in socket.recv(1)? {
        if message.header.nlmsg_type == libc::RTM_NEWLINK
            && let Some(link) = parse_link(&message)
        {
            links.push(link);
        }
    }
    Ok(links)
}

/// Dumps neighbor entries through netlink.
pub fn netlink_get_neighbors(
    ifindex: Option<IfIndex>,
    family: u8,
) -> io::Result<Vec<NeighborEntry>> {
    let socket = NetlinkSocket::open()?;
    // SAFETY: NeighborRequest is a plain C request struct.
    let mut request = unsafe { mem::zeroed::<NeighborRequest>() };
    let len = mem::size_of::<libc::nlmsghdr>() + mem::size_of::<ndmsg>();
    request.header = libc::nlmsghdr {
        nlmsg_len: len as u32,
        nlmsg_type: libc::RTM_GETNEIGH,
        nlmsg_flags: (libc::NLM_F_REQUEST | libc::NLM_F_DUMP) as u16,
        nlmsg_seq: 1,
        nlmsg_pid: 0,
    };
    request.ndm.ndm_family = family;
    request.ndm.ndm_ifindex = ifindex.map_or(0, |idx| idx.get() as i32);
    socket.send(&bytes_of(&request)[..len])?;

    let mut neighbors = Vec::new();
    for message in socket.recv(1)? {
        if message.header.nlmsg_type == libc::RTM_NEWNEIGH
            && let Some(neighbor) = parse_neighbor(&message, ifindex)
        {
            neighbors.push(neighbor);
        }
    }
    Ok(neighbors)
}

/// Dumps route entries for one table through netlink.
pub fn netlink_get_routes(family: u8, table: u32) -> io::Result<Vec<RouteEntry>> {
    let socket = NetlinkSocket::open()?;
    // SAFETY: RouteRequest is a plain C request struct.
    let mut request = unsafe { mem::zeroed::<RouteRequest>() };
    let base_len = mem::size_of::<libc::nlmsghdr>() + mem::size_of::<rtmsg>();
    request.header = libc::nlmsghdr {
        nlmsg_len: base_len as u32,
        nlmsg_type: libc::RTM_GETROUTE,
        nlmsg_flags: (libc::NLM_F_REQUEST | libc::NLM_F_DUMP) as u16,
        nlmsg_seq: 1,
        nlmsg_pid: 0,
    };
    request.rtm.rtm_family = family;

    let mut buffer = bytes_of(&request)[..base_len].to_vec();
    push_nlattr(&mut buffer, libc::RTA_TABLE, &table);
    refresh_nlmsg_len(&mut buffer)?;
    socket.send(&buffer)?;

    // When the caller requests a table > 255, only honor RTA_TABLE matches;
    // `rtm_table` truncates and `parse_route`'s fallback would otherwise
    // misclassify those entries as belonging to a different table.
    let require_rta_table = table > u32::from(u8::MAX);

    let mut routes = Vec::new();
    for message in socket.recv(1)? {
        if message.header.nlmsg_type != libc::RTM_NEWROUTE {
            continue;
        }
        let Some(route) = parse_route(&message) else {
            continue;
        };
        if require_rta_table && !route_table_from_attr(&message) {
            continue;
        }
        if route.table == Some(table) {
            routes.push(route);
        }
    }
    Ok(routes)
}

fn route_table_from_attr(message: &NetlinkMessage) -> bool {
    if message.data.len() < mem::size_of::<rtmsg>() {
        return false;
    }
    let Ok(attrs) = parse_attrs(&message.data[mem::size_of::<rtmsg>()..]) else {
        return false;
    };
    attrs.contains_key(&libc::RTA_TABLE)
}

fn parse_link(message: &NetlinkMessage) -> Option<LinkEntry> {
    if message.data.len() < mem::size_of::<ifinfomsg>() {
        return None;
    }
    // SAFETY: checked length and unaligned reads are allowed.
    let info = unsafe { ptr::read_unaligned(message.data.as_ptr().cast::<ifinfomsg>()) };
    let attrs = parse_attrs(&message.data[mem::size_of::<ifinfomsg>()..]).ok()?;
    let mac = attrs.get(&libc::IFLA_ADDRESS).and_then(|attr| {
        let bytes = attr.data.get(..6)?;
        let mut mac = [0u8; 6];
        mac.copy_from_slice(bytes);
        Some(LinkAddr::new(mac))
    });
    let mtu = attrs
        .get(&libc::IFLA_MTU)
        .and_then(|attr| read_u32_ne(attr.data));
    let master_ifindex = attrs
        .get(&libc::IFLA_MASTER)
        .and_then(|attr| read_u32_ne(attr.data))
        .and_then(IfIndex::try_new);
    Some(LinkEntry {
        // `ifi_index` from the kernel is non-zero for every real interface;
        // skip any entry that violates that invariant rather than panicking
        // inside the strict `IfIndex::new`.
        ifindex: IfIndex::try_new(info.ifi_index as u32)?,
        master_ifindex,
        mac,
        mtu,
    })
}

fn parse_neighbor(
    message: &NetlinkMessage,
    filter_ifindex: Option<IfIndex>,
) -> Option<NeighborEntry> {
    if message.data.len() < mem::size_of::<ndmsg>() {
        return None;
    }
    // SAFETY: checked length and unaligned reads are allowed.
    let ndm = unsafe { ptr::read_unaligned(message.data.as_ptr().cast::<ndmsg>()) };
    // Some neighbor entries (e.g., a half-finalized FAILED entry) can carry
    // `ndm_ifindex == 0`. Skip them instead of panicking.
    let ifindex = IfIndex::try_new(ndm.ndm_ifindex as u32)?;
    if filter_ifindex.is_some_and(|filter| filter != ifindex) {
        return None;
    }
    let attrs = parse_attrs(&message.data[mem::size_of::<ndmsg>()..]).ok()?;
    let destination = attrs
        .get(&libc::NDA_DST)
        .and_then(|attr| parse_ip_address(attr.data, ndm.ndm_family));
    let lladdr = attrs.get(&libc::NDA_LLADDR).and_then(|attr| {
        let bytes = attr.data.get(..6)?;
        let mut mac = [0u8; 6];
        mac.copy_from_slice(bytes);
        Some(LinkAddr::new(mac))
    });
    Some(NeighborEntry {
        destination,
        lladdr,
        ifindex,
        state: ndm.ndm_state,
    })
}

fn parse_route(message: &NetlinkMessage) -> Option<RouteEntry> {
    if message.data.len() < mem::size_of::<rtmsg>() {
        return None;
    }
    // SAFETY: checked length and unaligned reads are allowed.
    let rtm = unsafe { ptr::read_unaligned(message.data.as_ptr().cast::<rtmsg>()) };
    let attrs = parse_attrs(&message.data[mem::size_of::<rtmsg>()..]).ok()?;
    let destination = attrs
        .get(&libc::RTA_DST)
        .and_then(|attr| parse_ip_address(attr.data, rtm.rtm_family));
    let gateway = attrs
        .get(&libc::RTA_GATEWAY)
        .and_then(|attr| parse_ip_address(attr.data, rtm.rtm_family));
    let out_ifindex = attrs
        .get(&libc::RTA_OIF)
        .and_then(|attr| read_u32_ne(attr.data))
        .and_then(IfIndex::try_new);
    let priority = attrs
        .get(&libc::RTA_PRIORITY)
        .and_then(|attr| read_u32_ne(attr.data));
    // Prefer the 32-bit RTA_TABLE attribute when present; only fall back to
    // the 8-bit `rtm_table` field when the kernel did not emit it. The
    // fallback can never represent table ids > 255 (RT_TABLE_LOCAL etc.
    // legitimately exceed that range), so consumers that filter by a
    // > 255 table id must require the attribute.
    let table = attrs
        .get(&libc::RTA_TABLE)
        .and_then(|attr| read_u32_ne(attr.data))
        .or(Some(u32::from(rtm.rtm_table)));
    Some(RouteEntry {
        destination,
        gateway,
        out_ifindex,
        priority,
        table,
        family: rtm.rtm_family,
        dst_len: rtm.rtm_dst_len,
    })
}

#[derive(Clone, Copy)]
struct NlAttr<'a> {
    data: &'a [u8],
}

fn parse_attrs(buf: &[u8]) -> io::Result<HashMap<u16, NlAttr<'_>>> {
    let mut attrs = HashMap::new();
    let mut offset = 0;
    while offset < buf.len() {
        if buf.len() - offset < NLA_HDR_LEN {
            return Err(io::Error::other("short netlink attribute header"));
        }
        // SAFETY: checked length and unaligned reads are allowed.
        let attr = unsafe { ptr::read_unaligned(buf[offset..].as_ptr().cast::<libc::nlattr>()) };
        let len = attr.nla_len as usize;
        if len < NLA_HDR_LEN || offset + len > buf.len() {
            return Err(io::Error::other("invalid netlink attribute length"));
        }
        attrs.insert(
            attr.nla_type & libc::NLA_TYPE_MASK as u16,
            NlAttr {
                data: &buf[offset + NLA_HDR_LEN..offset + len],
            },
        );
        offset = offset.saturating_add(align_to(len, NLA_ALIGNTO));
    }
    Ok(attrs)
}

fn parse_ip_address(data: &[u8], family: u8) -> Option<IpAddr> {
    match family as i32 {
        libc::AF_INET if data.len() == 4 => Some(IpAddr::V4(Ipv4Addr::new(
            data[0], data[1], data[2], data[3],
        ))),
        libc::AF_INET6 if data.len() == 16 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(data);
            Some(IpAddr::V6(Ipv6Addr::from(octets)))
        }
        _ => None,
    }
}

fn read_u32_ne(data: &[u8]) -> Option<u32> {
    let data = data.get(..4)?;
    Some(u32::from_ne_bytes([data[0], data[1], data[2], data[3]]))
}

fn push_nlattr<T>(buffer: &mut Vec<u8>, attr_type: u16, value: &T) {
    let attr_len = NLA_HDR_LEN + mem::size_of::<T>();
    let aligned_len = align_to(attr_len, NLA_ALIGNTO);
    let attr = libc::nlattr {
        nla_len: attr_len as u16,
        nla_type: attr_type,
    };
    buffer.extend_from_slice(bytes_of(&attr));
    buffer.extend_from_slice(bytes_of(value));
    buffer.resize(buffer.len() + aligned_len - attr_len, 0);
}

fn refresh_nlmsg_len(buffer: &mut [u8]) -> io::Result<()> {
    let len: u32 = buffer
        .len()
        .try_into()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    let Some(header_len) = buffer.get_mut(..mem::size_of::<u32>()) else {
        return Err(io::Error::other("buffer smaller than nlmsghdr"));
    };
    header_len.copy_from_slice(&len.to_ne_bytes());
    Ok(())
}

fn bytes_of<T>(value: &T) -> &[u8] {
    let size = mem::size_of::<T>();
    // SAFETY: value is valid for size_of::<T>() bytes for this borrow.
    unsafe { slice::from_raw_parts(slice::from_ref(value).as_ptr().cast(), size) }
}

const fn align_to(value: usize, align: usize) -> usize {
    (value + (align - 1)) & !(align - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refresh_nlmsg_len_accounts_for_appended_attrs() {
        let mut request = unsafe { mem::zeroed::<RouteRequest>() };
        let base_len = mem::size_of::<libc::nlmsghdr>() + mem::size_of::<rtmsg>();
        request.header.nlmsg_len = base_len as u32;
        let mut buffer = bytes_of(&request)[..base_len].to_vec();

        push_nlattr(&mut buffer, libc::RTA_TABLE, &254u32);
        refresh_nlmsg_len(&mut buffer).unwrap();

        let actual = u32::from_ne_bytes(buffer[..4].try_into().unwrap());
        assert_eq!(actual as usize, buffer.len());
        assert!(buffer.len() > base_len);
    }
}
