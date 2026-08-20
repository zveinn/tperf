use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::Path;

use crate::netutil::{is_unspecified_host, parse_host_port, AddrFamily};

pub const DEFAULT_CLIENT_ADDR: &str = "0.0.0.0:7777";
pub const DEFAULT_PAYLOAD_SIZE: u64 = 100 * 1024 * 1024;
pub const DEFAULT_WORKERS: u32 = 10;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Network {
    #[default]
    Tcp,
    Udp,
}

impl std::fmt::Display for Network {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Network::Tcp => write!(f, "tcp"),
            Network::Udp => write!(f, "udp"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum TestType {
    #[default]
    Pairs,
    Mesh,
}

impl std::fmt::Display for TestType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestType::Pairs => write!(f, "pairs"),
            TestType::Mesh => write!(f, "mesh"),
        }
    }
}

/// Canonical in-memory / on-wire configuration.
///
/// `hostlist` is always the expanded list of hosts. `hostlist_raw` preserves
/// the values as written in the config file (ellipsis patterns included).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    #[serde(default = "default_client_addr")]
    pub client_addr: String,
    pub test_addr: String,
    #[serde(default)]
    pub network: Network,
    #[serde(default = "default_payload")]
    pub payload_size: u64,
    #[serde(default = "default_workers")]
    pub workers: u32,
    #[serde(rename = "type", default)]
    pub test_type: TestType,
    pub hostlist: Vec<String>,
    #[serde(default)]
    pub hostlist_raw: Vec<String>,
    pub tag: String,
}

fn default_client_addr() -> String {
    DEFAULT_CLIENT_ADDR.to_string()
}
fn default_payload() -> u64 {
    DEFAULT_PAYLOAD_SIZE
}
fn default_workers() -> u32 {
    DEFAULT_WORKERS
}

#[derive(Debug, Deserialize)]
struct ConfigFile {
    #[serde(default = "default_client_addr")]
    client_addr: String,
    test_addr: String,
    #[serde(default)]
    network: Network,
    #[serde(default = "default_payload_file", deserialize_with = "deser_payload")]
    payload_size: u64,
    #[serde(default = "default_workers")]
    workers: u32,
    #[serde(rename = "type", default)]
    test_type: TestType,
    #[serde(deserialize_with = "deser_hostlist")]
    hostlist: Vec<String>,
    tag: String,
}

fn default_payload_file() -> u64 {
    DEFAULT_PAYLOAD_SIZE
}

fn deser_payload<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = u64;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a byte count or size string like 100MiB")
        }
        fn visit_u64<E: serde::de::Error>(self, v: u64) -> std::result::Result<u64, E> {
            Ok(v)
        }
        fn visit_i64<E: serde::de::Error>(self, v: i64) -> std::result::Result<u64, E> {
            if v < 0 {
                return Err(E::custom("payload_size must be >= 0"));
            }
            Ok(v as u64)
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<u64, E> {
            parse_size(v).map_err(E::custom)
        }
    }
    deserializer.deserialize_any(V)
}

fn deser_hostlist<'de, D>(deserializer: D) -> std::result::Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct V;
    impl<'de> serde::de::Visitor<'de> for V {
        type Value = Vec<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "a host string or array of host strings")
        }
        fn visit_str<E: serde::de::Error>(self, v: &str) -> std::result::Result<Vec<String>, E> {
            Ok(split_hosts(v))
        }
        fn visit_seq<A>(self, mut seq: A) -> std::result::Result<Vec<String>, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut out = Vec::new();
            while let Some(s) = seq.next_element::<String>()? {
                out.extend(split_hosts(&s));
            }
            Ok(out)
        }
    }
    deserializer.deserialize_any(V)
}

fn split_hosts(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect()
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        Self::from_toml(&text)
    }

    pub fn from_toml(text: &str) -> Result<Self> {
        let f: ConfigFile = toml::from_str(text).context("parsing config toml")?;
        let hostlist_raw = f.hostlist;
        let hostlist = expand_hostlist(&hostlist_raw)?;
        let cfg = Config {
            client_addr: f.client_addr,
            test_addr: f.test_addr,
            network: f.network,
            payload_size: f.payload_size,
            workers: f.workers,
            test_type: f.test_type,
            hostlist,
            hostlist_raw,
            tag: f.tag,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<()> {
        if self.tag.is_empty()
            || self.tag.contains('/')
            || self.tag.contains('\\')
            || self.tag.contains('\0')
            || self.tag == "."
            || self.tag == ".."
        {
            bail!(
                "invalid tag {:?}: used as sqlite filename, must be a plain name",
                self.tag
            );
        }
        if self.workers == 0 {
            bail!("workers must be >= 1");
        }
        if self.payload_size == 0 {
            bail!("payload_size must be >= 1");
        }
        if self.hostlist.is_empty() {
            bail!("hostlist is empty after expansion");
        }
        if self.hostlist.len() < 2 {
            bail!(
                "hostlist must contain at least 2 hosts, got {}",
                self.hostlist.len()
            );
        }
        let (c_host, c_port) = parse_host_port(&self.client_addr)
            .with_context(|| format!("client_addr {:?}", self.client_addr))?;
        if c_port == 0 {
            bail!("client_addr port must be non-zero");
        }
        let _ = c_host;

        let (t_host, t_port) = parse_host_port(&self.test_addr)
            .with_context(|| format!("test_addr {:?}", self.test_addr))?;
        if t_host.is_empty() {
            bail!("test_addr has no host (a host/ip is required; 0.0.0.0 is not accepted)");
        }
        if t_port == 0 {
            bail!("test_addr port must be non-zero");
        }
        if is_unspecified_host(&t_host) {
            bail!("test_addr must not use an unspecified address (0.0.0.0 / ::)");
        }
        Ok(())
    }

    pub fn control_port(&self) -> Result<u16> {
        Ok(parse_host_port(&self.client_addr)?.1)
    }

    pub fn test_port(&self) -> Result<u16> {
        Ok(parse_host_port(&self.test_addr)?.1)
    }

    pub fn test_family(&self) -> Result<Option<AddrFamily>> {
        let (host, _) = parse_host_port(&self.test_addr)?;
        Ok(AddrFamily::from_host(&host))
    }

    /// One sqlite file per run: `<tag>.db` in the client's cwd.
    pub fn db_path(&self) -> std::path::PathBuf {
        std::path::PathBuf::from(db_filename(&self.tag))
    }
}

/// Always `[tag].db`. A trailing `.db` on the tag is not doubled.
pub fn db_filename(tag: &str) -> String {
    let stem = tag
        .strip_suffix(".db")
        .or_else(|| tag.strip_suffix(".DB"))
        .unwrap_or(tag);
    format!("{stem}.db")
}

/// Parse sizes like `100MiB`, `64KiB`, `1GB`, or a raw integer.
pub fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty size");
    }
    if let Ok(n) = s.parse::<u64>() {
        return Ok(n);
    }
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() && (bytes[i].is_ascii_digit() || bytes[i] == b'.') {
        i += 1;
    }
    if i == 0 {
        bail!("invalid size {s:?}");
    }
    let num: f64 = s[..i]
        .parse()
        .with_context(|| format!("invalid size number in {s:?}"))?;
    if num < 0.0 {
        bail!("size must be non-negative");
    }
    let unit = s[i..].trim().to_ascii_lowercase();
    let mul: f64 = match unit.as_str() {
        "" | "b" => 1.0,
        "k" | "kb" => 1000.0,
        "ki" | "kib" => 1024.0,
        "m" | "mb" => 1000.0 * 1000.0,
        "mi" | "mib" => 1024.0 * 1024.0,
        "g" | "gb" => 1000.0 * 1000.0 * 1000.0,
        "gi" | "gib" => 1024.0 * 1024.0 * 1024.0,
        other => bail!("unknown size unit {other:?} in {s:?}"),
    };
    let v = num * mul;
    if !v.is_finite() || v > u64::MAX as f64 {
        bail!("size {s:?} is out of range");
    }
    Ok(v.round() as u64)
}

/// Expand a list of host patterns. Each item may contain `{start..end}` ranges
/// anywhere in the string (zero-padded if the start token has leading zeros).
pub fn expand_hostlist(items: &[String]) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for item in items {
        for h in expand_pattern(item)? {
            if !seen.insert(h.clone()) {
                bail!("duplicate host {h} after expanding hostlist");
            }
            out.push(h);
        }
    }
    Ok(out)
}

pub fn expand_pattern(pat: &str) -> Result<Vec<String>> {
    match find_range(pat)? {
        None => Ok(vec![pat.to_string()]),
        Some((lo, hi, start, end)) => {
            let spec = &pat[lo + 1..hi];
            let (a_str, b_str) = spec
                .split_once("..")
                .ok_or_else(|| anyhow::anyhow!("invalid ellipsis in {pat:?}"))?;
            let a: i64 = a_str
                .parse()
                .with_context(|| format!("ellipsis start in {pat:?}"))?;
            let b: i64 = b_str
                .parse()
                .with_context(|| format!("ellipsis end in {pat:?}"))?;
            let width = if a_str.len() > 1 && a_str.starts_with('0') {
                a_str.len()
            } else {
                0
            };
            let nums: Vec<i64> = if a <= b {
                (a..=b).collect()
            } else {
                (b..=a).rev().collect()
            };
            if nums.len() > 10_000 {
                bail!(
                    "ellipsis in {pat:?} expands to too many hosts ({})",
                    nums.len()
                );
            }
            let mut out = Vec::new();
            for n in nums {
                let mid = if width > 0 {
                    format!("{n:0width$}")
                } else {
                    n.to_string()
                };
                let replaced = format!("{start}{mid}{end}");
                out.extend(expand_pattern(&replaced)?);
            }
            Ok(out)
        }
    }
}

/// Returns (brace_open_idx, brace_close_idx, prefix, suffix).
fn find_range(s: &str) -> Result<Option<(usize, usize, String, String)>> {
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'{' {
            i += 1;
            continue;
        }
        if let Some(rel) = bytes[i + 1..].iter().position(|&c| c == b'}') {
            let close = i + 1 + rel;
            let inner = &s[i + 1..close];
            if is_range_inner(inner) {
                return Ok(Some((
                    i,
                    close,
                    s[..i].to_string(),
                    s[close + 1..].to_string(),
                )));
            }
            i += 1;
        } else {
            bail!("unterminated '{{' in host pattern {s:?}");
        }
    }
    Ok(None)
}

fn is_range_inner(inner: &str) -> bool {
    let Some((a, b)) = inner.split_once("..") else {
        return false;
    };
    if a.is_empty() || b.is_empty() {
        return false;
    }
    a.chars().all(|c| c.is_ascii_digit()) && b.chars().all(|c| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_simple() {
        let got = expand_pattern("srv{1..3}.example.com").unwrap();
        assert_eq!(
            got,
            vec!["srv1.example.com", "srv2.example.com", "srv3.example.com"]
        );
    }

    #[test]
    fn expand_padded() {
        let got = expand_pattern("n{01..03}").unwrap();
        assert_eq!(got, vec!["n01", "n02", "n03"]);
    }

    #[test]
    fn expand_in_middle() {
        let got = expand_pattern("rack-a{8..10}-x").unwrap();
        assert_eq!(got, vec!["rack-a8-x", "rack-a9-x", "rack-a10-x"]);
    }

    #[test]
    fn expand_nested_ranges() {
        let got = expand_pattern("r{1..2}h{1..2}").unwrap();
        assert_eq!(got, vec!["r1h1", "r1h2", "r2h1", "r2h2"]);
    }

    #[test]
    fn expand_reverse() {
        let got = expand_pattern("x{3..1}").unwrap();
        assert_eq!(got, vec!["x3", "x2", "x1"]);
    }

    #[test]
    fn expand_list_dedup() {
        let err = expand_hostlist(&["a{1..2}".into(), "a1".into()]).unwrap_err();
        assert!(err.to_string().contains("duplicate"));
    }

    #[test]
    fn parse_sizes() {
        assert_eq!(parse_size("100").unwrap(), 100);
        assert_eq!(parse_size("100MiB").unwrap(), 100 * 1024 * 1024);
        assert_eq!(parse_size("1KiB").unwrap(), 1024);
        assert_eq!(parse_size("1.5KiB").unwrap(), 1536);
        assert_eq!(parse_size("2MB").unwrap(), 2_000_000);
    }

    #[test]
    fn toml_roundtrip_defaults() {
        let t = r#"
test_addr = "10.1.2.3:9100"
hostlist = ["srv{1..2}.lab"]
tag = "run1"
"#;
        let c = Config::from_toml(t).unwrap();
        assert_eq!(c.client_addr, DEFAULT_CLIENT_ADDR);
        assert_eq!(c.network, Network::Tcp);
        assert_eq!(c.payload_size, DEFAULT_PAYLOAD_SIZE);
        assert_eq!(c.workers, DEFAULT_WORKERS);
        assert_eq!(c.test_type, TestType::Pairs);
        assert_eq!(c.hostlist, vec!["srv1.lab", "srv2.lab"]);
        assert_eq!(c.hostlist_raw, vec!["srv{1..2}.lab"]);
        assert_eq!(c.tag, "run1");
        assert_eq!(c.db_path(), std::path::PathBuf::from("run1.db"));
    }

    #[test]
    fn db_filename_adds_db_suffix() {
        assert_eq!(db_filename("live"), "live.db");
        assert_eq!(db_filename("live.db"), "live.db");
        assert_eq!(db_filename("run.sqlite"), "run.sqlite.db");
    }

    #[test]
    fn toml_rejects_unspecified_test_addr() {
        let t = r#"
test_addr = "0.0.0.0:9100"
hostlist = ["a", "b"]
tag = "x"
"#;
        let err = Config::from_toml(t).unwrap_err();
        assert!(err.to_string().contains("unspecified"));
    }

    #[test]
    fn toml_rejects_v6_unspecified_test_addr() {
        let t = r#"
test_addr = "[::]:9100"
hostlist = ["a", "b"]
tag = "x"
"#;
        let err = Config::from_toml(t).unwrap_err();
        assert!(err.to_string().contains("unspecified"));
    }

    #[test]
    fn toml_payload_string_and_type() {
        let t = r#"
test_addr = "host:9100"
network = "udp"
payload_size = "64KiB"
workers = 4
type = "mesh"
hostlist = "n{1..3},edge"
tag = "t"
"#;
        let c = Config::from_toml(t).unwrap();
        assert_eq!(c.network, Network::Udp);
        assert_eq!(c.payload_size, 64 * 1024);
        assert_eq!(c.test_type, TestType::Mesh);
        assert_eq!(c.hostlist, vec!["n1", "n2", "n3", "edge"]);
    }

    #[test]
    fn invalid_tag() {
        let t = r#"
test_addr = "h:1"
hostlist = ["a", "b"]
tag = "foo/bar"
"#;
        assert!(Config::from_toml(t).is_err());
    }
}
