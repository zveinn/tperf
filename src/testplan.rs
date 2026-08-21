use std::net::SocketAddr;

use crate::proto::{Assignment, Target};

/// Round-robin 1-factorization: every host is paired with every other host
/// across rounds. Within a round, pairs are disjoint (a host is in at most
/// one pair). Odd-sized lists get a bye each round.
///
/// For n hosts (n even): n-1 rounds of n/2 pairs.
/// For n odd: n rounds of (n-1)/2 pairs.
pub fn round_robin_rounds(hosts: &[String]) -> Vec<Vec<(String, String)>> {
    if hosts.len() < 2 {
        return Vec::new();
    }
    let mut arr: Vec<Option<String>> = hosts.iter().cloned().map(Some).collect();
    if arr.len() % 2 == 1 {
        arr.push(None);
    }
    let n = arr.len();
    let mut rounds = Vec::with_capacity(n - 1);
    for _ in 0..(n - 1) {
        let mut pairs = Vec::new();
        for i in 0..(n / 2) {
            if let (Some(a), Some(b)) = (&arr[i], &arr[n - 1 - i]) {
                pairs.push((a.clone(), b.clone()));
            }
        }
        rounds.push(pairs);
        // Keep arr[0] fixed; rotate the rest clockwise.
        if n > 2 {
            let last = arr.pop().expect("n > 2");
            arr.insert(1, last);
        }
    }
    rounds
}

pub fn assignments_mesh(hosts: &[String]) -> Vec<Assignment> {
    hosts
        .iter()
        .enumerate()
        .map(|(i, name)| Assignment {
            self_name: name.clone(),
            self_id: i as u32,
            bind: String::new(),
            targets: hosts
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(j, n)| Target {
                    name: n.clone(),
                    id: j as u32,
                    addr: String::new(),
                })
                .collect(),
        })
        .collect()
}

pub fn assignments_for_pairs(hosts: &[String], pairs: &[(String, String)]) -> Vec<Assignment> {
    let mut out: Vec<Assignment> = hosts
        .iter()
        .enumerate()
        .map(|(i, name)| Assignment {
            self_name: name.clone(),
            self_id: i as u32,
            bind: String::new(),
            targets: Vec::new(),
        })
        .collect();
    for (a, b) in pairs {
        let ia = hosts.iter().position(|h| h == a).expect("pair host a");
        let ib = hosts.iter().position(|h| h == b).expect("pair host b");
        out[ia].targets.push(Target {
            name: b.clone(),
            id: ib as u32,
            addr: String::new(),
        });
        out[ib].targets.push(Target {
            name: a.clone(),
            id: ia as u32,
            addr: String::new(),
        });
    }
    out
}

pub fn fill_resolved(asg: &mut [Assignment], resolved: &[SocketAddr]) {
    for a in asg {
        a.bind = resolved[a.self_id as usize].to_string();
        for t in &mut a.targets {
            t.addr = resolved[t.id as usize].to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn names(n: usize) -> Vec<String> {
        (1..=n).map(|i| format!("srv{i}")).collect()
    }

    fn undirected(pairs: &[(String, String)]) -> HashSet<(String, String)> {
        pairs
            .iter()
            .map(|(a, b)| {
                if a <= b {
                    (a.clone(), b.clone())
                } else {
                    (b.clone(), a.clone())
                }
            })
            .collect()
    }

    #[test]
    fn four_hosts_match_user_example() {
        let hosts = names(4);
        let rounds = round_robin_rounds(&hosts);
        assert_eq!(rounds.len(), 3);
        assert!(rounds.iter().all(|r| r.len() == 2));

        let mut all = HashSet::new();
        for r in &rounds {
            let mut used = HashSet::new();
            for (a, b) in r {
                assert!(used.insert(a), "host {a} twice in one round");
                assert!(used.insert(b), "host {b} twice in one round");
                assert!(a != b);
            }
            let u = undirected(r);
            assert_eq!(u.len(), r.len(), "duplicate pair in a round");
            for p in u {
                assert!(all.insert(p.clone()), "pair {:?} in two rounds", p);
            }
        }
        assert_eq!(all.len(), 6, "C(4,2)=6 undirected pairs");
    }

    #[test]
    fn five_hosts_bye_each_round() {
        let hosts = names(5);
        let rounds = round_robin_rounds(&hosts);
        assert_eq!(rounds.len(), 5);
        assert!(rounds.iter().all(|r| r.len() == 2));
        let mut all = HashSet::new();
        for r in &rounds {
            let mut used = HashSet::new();
            for (a, b) in r {
                assert!(used.insert(a));
                assert!(used.insert(b));
            }
            for p in undirected(r) {
                assert!(all.insert(p.clone()), "duplicate {p:?}");
            }
        }
        assert_eq!(all.len(), 10, "C(5,2)=10");
    }

    #[test]
    fn forty_hosts_full_cover() {
        let hosts = names(40);
        let rounds = round_robin_rounds(&hosts);
        assert_eq!(rounds.len(), 39);
        assert!(rounds.iter().all(|r| r.len() == 20));
        let mut all = HashSet::new();
        for r in &rounds {
            for p in undirected(r) {
                all.insert(p);
            }
        }
        assert_eq!(all.len(), 40 * 39 / 2);
    }

    #[test]
    fn pair_assignment_is_bidirectional() {
        let hosts = names(4);
        let pairs = vec![
            ("srv1".into(), "srv2".into()),
            ("srv3".into(), "srv4".into()),
        ];
        let asg = assignments_for_pairs(&hosts, &pairs);
        assert_eq!(asg[0].targets[0].name, "srv2");
        assert_eq!(asg[1].targets[0].name, "srv1");
        assert_eq!(asg[2].targets[0].name, "srv4");
        assert_eq!(asg[3].targets[0].name, "srv3");
    }

    #[test]
    fn mesh_all_to_all() {
        let hosts = names(3);
        let asg = assignments_mesh(&hosts);
        assert_eq!(asg[0].targets.len(), 2);
        assert_eq!(asg[1].targets.len(), 2);
        assert_eq!(asg[2].targets.len(), 2);
        let names: Vec<_> = asg[1].targets.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["srv1", "srv3"]);
    }
}
