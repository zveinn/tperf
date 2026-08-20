use crate::proto::{Assignment, Target};

/// Pair hosts in list order: (0,1), (2,3), ... The last host is idle if the
/// count is odd. No pairs overlap.
pub fn pair_hosts(hosts: &[String]) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut i = 0;
    while i + 1 < hosts.len() {
        pairs.push((hosts[i].clone(), hosts[i + 1].clone()));
        i += 2;
    }
    pairs
}

pub fn assignments(hosts: &[String], mesh: bool) -> Vec<Assignment> {
    if mesh {
        return hosts
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
            .collect();
    }

    let pairs = pair_hosts(hosts);
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
        let ia = hosts.iter().position(|h| h == &a).expect("pair host a");
        let ib = hosts.iter().position(|h| h == &b).expect("pair host b");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pairs_even() {
        let hosts = ["a".into(), "b".into(), "c".into(), "d".into()];
        let p = pair_hosts(&hosts);
        assert_eq!(p, vec![("a".into(), "b".into()), ("c".into(), "d".into())]);
        let asg = assignments(&hosts, false);
        assert_eq!(asg[0].targets.len(), 1);
        assert_eq!(asg[0].targets[0].name, "b");
        assert_eq!(asg[1].targets[0].name, "a");
        assert_eq!(asg[2].targets[0].name, "d");
        assert_eq!(asg[3].targets[0].name, "c");
    }

    #[test]
    fn pairs_odd_bye() {
        let hosts = ["a".into(), "b".into(), "c".into()];
        let p = pair_hosts(&hosts);
        assert_eq!(p, vec![("a".into(), "b".into())]);
        let asg = assignments(&hosts, false);
        assert!(asg[2].targets.is_empty());
    }

    #[test]
    fn mesh_all_to_all() {
        let hosts = ["a".into(), "b".into(), "c".into()];
        let asg = assignments(&hosts, true);
        assert_eq!(asg[0].targets.len(), 2);
        assert_eq!(asg[1].targets.len(), 2);
        assert_eq!(asg[2].targets.len(), 2);
        let names: Vec<_> = asg[1].targets.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["a", "c"]);
    }
}
