use anyhow::{bail, Context, Result};
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;
use tokio::net::{lookup_host, TcpListener, TcpStream, UdpSocket};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddrFamily {
    V4,
    V6,
}

impl AddrFamily {
    pub fn from_host(host: &str) -> Option<Self> {
        if let Ok(ip) = host.parse::<IpAddr>() {
            return Some(match ip {
                IpAddr::V4(_) => AddrFamily::V4,
                IpAddr::V6(_) => AddrFamily::V6,
            });
        }
        None
    }

    pub fn matches(self, addr: SocketAddr) -> bool {
        match self {
            AddrFamily::V4 => addr.is_ipv4(),
            AddrFamily::V6 => addr.is_ipv6(),
        }
    }
}

/// Split `host:port`, supporting `[ipv6]:port`.
pub fn parse_host_port(s: &str) -> Result<(String, u16)> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty address");
    }
    if let Some(rest) = s.strip_prefix('[') {
        let close = rest
            .find(']')
            .ok_or_else(|| anyhow::anyhow!("missing ']' in IPv6 address {s:?}"))?;
        let host = rest[..close].to_string();
        let after = &rest[close + 1..];
        let port_str = after
            .strip_prefix(':')
            .ok_or_else(|| anyhow::anyhow!("missing port in {s:?}"))?;
        let port: u16 = port_str
            .parse()
            .with_context(|| format!("invalid port in {s:?}"))?;
        if host.is_empty() {
            bail!("empty host in {s:?}");
        }
        return Ok((host, port));
    }
    match s.rsplit_once(':') {
        Some((host, port_str))
            if !host.is_empty() && !port_str.is_empty() && !host.contains(':') =>
        {
            let port: u16 = port_str
                .parse()
                .with_context(|| format!("invalid port in {s:?}"))?;
            Ok((host.to_string(), port))
        }
        _ => bail!("address {s:?} must be host:port or [ipv6]:port"),
    }
}

pub fn is_unspecified_host(host: &str) -> bool {
    match host.parse::<IpAddr>() {
        Ok(ip) => ip.is_unspecified(),
        Err(_) => host == "*" || host.eq_ignore_ascii_case("unspecified"),
    }
}

pub fn join_host_port(host: &str, port: u16) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

pub fn is_loopback_host(host: &str) -> bool {
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or(false)
}

/// Local unicast addresses, excluding unspecified / loopback / link-local.
pub fn local_unicast_ips() -> Vec<IpAddr> {
    use std::ptr;
    let mut ifa: *mut libc::ifaddrs = ptr::null_mut();
    let rc = unsafe { libc::getifaddrs(&mut ifa) };
    if rc != 0 || ifa.is_null() {
        return Vec::new();
    }
    let mut out = Vec::new();
    unsafe {
        let mut cur = ifa;
        while !cur.is_null() {
            let addr = (*cur).ifa_addr;
            if !addr.is_null() {
                match (*addr).sa_family as i32 {
                    libc::AF_INET => {
                        let sin = &*(addr as *const libc::sockaddr_in);
                        let ip = Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr));
                        if !ip.is_loopback() && !ip.is_unspecified() && !ip.is_link_local() {
                            out.push(IpAddr::V4(ip));
                        }
                    }
                    libc::AF_INET6 => {
                        let sin6 = &*(addr as *const libc::sockaddr_in6);
                        let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
                        // Keep unique-local (fd00::/8) — podman IPv6 networks use it.
                        if !ip.is_loopback() && !ip.is_unspecified() && !ip.is_unicast_link_local()
                        {
                            out.push(IpAddr::V6(ip));
                        }
                    }
                    _ => {}
                }
            }
            cur = (*cur).ifa_next;
        }
        libc::freeifaddrs(ifa);
    }
    out
}

pub async fn resolve_one(host: &str, port: u16, family: Option<AddrFamily>) -> Result<SocketAddr> {
    let query = join_host_port(host, port);
    let addrs: Vec<SocketAddr> = lookup_host(&query)
        .await
        .with_context(|| format!("DNS lookup for {query}"))?
        .collect();
    pick_addr(&addrs, family)
        .with_context(|| format!("no usable address for {query} (family={family:?})"))
}

pub fn pick_addr(addrs: &[SocketAddr], family: Option<AddrFamily>) -> Result<SocketAddr> {
    let filtered: Vec<SocketAddr> = match family {
        Some(fam) => addrs.iter().copied().filter(|a| fam.matches(*a)).collect(),
        None => addrs.to_vec(),
    };
    if filtered.is_empty() {
        bail!("no matching addresses");
    }
    if let Some(a) = filtered
        .iter()
        .find(|a| a.is_ipv4() && !a.ip().is_loopback())
    {
        return Ok(*a);
    }
    if let Some(a) = filtered
        .iter()
        .find(|a| a.is_ipv6() && !a.ip().is_loopback())
    {
        return Ok(*a);
    }
    if let Some(a) = filtered.iter().find(|a| a.is_ipv4()) {
        return Ok(*a);
    }
    Ok(filtered[0])
}

const SOCK_BUF: usize = 4 * 1024 * 1024;

fn apply_buf_sizes(sock: &Socket) {
    let _ = sock.set_recv_buffer_size(SOCK_BUF);
    let _ = sock.set_send_buffer_size(SOCK_BUF);
}

pub fn tcp_listen(addr: SocketAddr) -> Result<TcpListener> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock = Socket::new(domain, Type::STREAM, Some(Protocol::TCP))
        .context("create tcp listen socket")?;
    sock.set_reuse_address(true)?;
    if addr.is_ipv6() && addr.ip().is_unspecified() {
        let _ = sock.set_only_v6(false);
    }
    apply_buf_sizes(&sock);
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())
        .with_context(|| format!("bind tcp {addr}"))?;
    sock.listen(1024)?;
    let std_listener: std::net::TcpListener = sock.into();
    TcpListener::from_std(std_listener).context("tcp listener from_std")
}

pub fn udp_bind(addr: SocketAddr) -> Result<UdpSocket> {
    let domain = if addr.is_ipv4() {
        Domain::IPV4
    } else {
        Domain::IPV6
    };
    let sock =
        Socket::new(domain, Type::DGRAM, Some(Protocol::UDP)).context("create udp socket")?;
    sock.set_reuse_address(true)?;
    if addr.is_ipv6() && addr.ip().is_unspecified() {
        let _ = sock.set_only_v6(false);
    }
    apply_buf_sizes(&sock);
    sock.set_nonblocking(true)?;
    sock.bind(&addr.into())
        .with_context(|| format!("bind udp {addr}"))?;
    let std_sock: std::net::UdpSocket = sock.into();
    UdpSocket::from_std(std_sock).context("udp socket from_std")
}

pub async fn resolve_retry(
    host: &str,
    port: u16,
    family: Option<AddrFamily>,
    deadline: Duration,
    cancel: &CancellationToken,
) -> Result<SocketAddr> {
    let start = std::time::Instant::now();
    let mut delay = Duration::from_millis(20);
    loop {
        if cancel.is_cancelled() {
            bail!("resolve {host} cancelled");
        }
        match resolve_one(host, port, family).await {
            Ok(a) => return Ok(a),
            Err(e) => {
                if start.elapsed() >= deadline {
                    return Err(e)
                        .with_context(|| format!("giving up DNS for {host} after {deadline:?}"));
                }
                tokio::select! {
                    _ = cancel.cancelled() => bail!("resolve {host} cancelled"),
                    _ = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(Duration::from_millis(400));
            }
        }
    }
}

pub async fn connect_tcp_retry(
    host: &str,
    port: u16,
    deadline: Duration,
    cancel: &CancellationToken,
) -> Result<TcpStream> {
    let dest = join_host_port(host, port);
    connect_tcp_dest(&dest, deadline, cancel).await
}

pub async fn connect_tcp_addr_retry(
    addr: SocketAddr,
    deadline: Duration,
    cancel: &CancellationToken,
) -> Result<TcpStream> {
    connect_tcp_dest(&addr.to_string(), deadline, cancel).await
}

async fn connect_tcp_dest(
    dest: &str,
    deadline: Duration,
    cancel: &CancellationToken,
) -> Result<TcpStream> {
    let start = std::time::Instant::now();
    let mut delay = Duration::from_millis(20);
    loop {
        if cancel.is_cancelled() {
            bail!("connect to {dest} cancelled");
        }
        match TcpStream::connect(dest).await {
            Ok(s) => {
                let _ = s.set_nodelay(true);
                return Ok(s);
            }
            Err(e) => {
                if start.elapsed() >= deadline {
                    return Err(e).with_context(|| {
                        format!("giving up connect to {dest} after {deadline:?}")
                    });
                }
                tokio::select! {
                    _ = cancel.cancelled() => bail!("connect to {dest} cancelled"),
                    _ = tokio::time::sleep(delay) => {}
                }
                delay = (delay * 2).min(Duration::from_millis(400));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_v4() {
        let (h, p) = parse_host_port("10.1.2.3:9100").unwrap();
        assert_eq!(h, "10.1.2.3");
        assert_eq!(p, 9100);
    }

    #[test]
    fn parse_v6() {
        let (h, p) = parse_host_port("[2001:db8::1]:9100").unwrap();
        assert_eq!(h, "2001:db8::1");
        assert_eq!(p, 9100);
    }

    #[test]
    fn parse_hostname() {
        let (h, p) = parse_host_port("srv1.lab:7777").unwrap();
        assert_eq!(h, "srv1.lab");
        assert_eq!(p, 7777);
    }

    #[test]
    fn parse_rejects_bare_v6() {
        assert!(parse_host_port("2001:db8::1:9100").is_err());
    }

    #[test]
    fn unspecified_detection() {
        assert!(is_unspecified_host("0.0.0.0"));
        assert!(is_unspecified_host("::"));
        assert!(is_unspecified_host("::0"));
        assert!(!is_unspecified_host("127.0.0.1"));
        assert!(!is_unspecified_host("::1"));
        assert!(!is_unspecified_host("srv1"));
    }

    #[test]
    fn join_v6() {
        assert_eq!(join_host_port("2001:db8::1", 9), "[2001:db8::1]:9");
        assert_eq!(join_host_port("srv1", 9), "srv1:9");
    }

    #[test]
    fn pick_prefers_v4_when_unspecified_family() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        let addrs = [
            SocketAddr::from((Ipv6Addr::LOCALHOST, 9)),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
        ];
        let a = pick_addr(&addrs, None).unwrap();
        assert!(a.is_ipv4());
        let a = pick_addr(&addrs, Some(AddrFamily::V6)).unwrap();
        assert!(a.is_ipv6());
    }

    #[test]
    fn pick_skips_loopback_when_real_v4_exists() {
        use std::net::{Ipv4Addr, Ipv6Addr};
        let addrs = [
            SocketAddr::from((Ipv4Addr::LOCALHOST, 9)),
            SocketAddr::from((Ipv4Addr::new(10, 0, 0, 5), 9)),
            SocketAddr::from((Ipv6Addr::LOCALHOST, 9)),
        ];
        let a = pick_addr(&addrs, None).unwrap();
        assert_eq!(a.ip(), IpAddr::V4(Ipv4Addr::new(10, 0, 0, 5)));
    }

    #[test]
    fn loopback_host_detection() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("srv1"));
    }
}
