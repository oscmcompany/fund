//! What `host/provision-host` and `host/run-host` answer for each role before either reaches AWS.

use std::path::PathBuf;
use std::process::Command;

use fund::common::storage::Host;
use strum::IntoEnumIterator;

fn script(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("host")
        .join(name)
}

/// The provisioner's exit code and output for `role` under a malformed profile, which stops it before any AWS call.
fn provision(role: &str) -> (i32, String) {
    let output = Command::new("bash")
        .arg(script("provision-host"))
        .args(["--role", role, "--profile", "not-a-profile"])
        .output()
        .unwrap();
    (
        output.status.code().unwrap(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

#[test]
fn test_each_role_is_answered_before_any_aws_call() {
    let answers: Vec<(String, i32, String)> = ["archiver", "trader", "researcher", "accountant"]
        .into_iter()
        .map(|role| {
            let (code, output) = provision(role);
            (
                role.to_string(),
                code,
                output.lines().next().unwrap_or("").to_string(),
            )
        })
        .collect();
    assert_eq!(
        answers,
        [
            (
                "archiver".to_string(),
                1,
                "Error: --profile must be production or development/<name>; got \"not-a-profile\""
                    .to_string()
            ),
            (
                "trader".to_string(),
                1,
                "Error: the trader role is not provisioned by this script yet".to_string()
            ),
            (
                "researcher".to_string(),
                0,
                "The researcher role is reserved: studies run on demand and nothing provisions a box for them yet"
                    .to_string()
            ),
            (
                "accountant".to_string(),
                1,
                "Error: --role must be archiver, trader or researcher; got \"accountant\"".to_string()
            ),
        ]
    );
}

#[test]
fn test_every_host_is_a_role_of_both_scripts() {
    let run_host = std::fs::read_to_string(script("run-host")).unwrap();
    for host in Host::iter() {
        let (_, output) = provision(&host.to_string());
        assert!(!output.contains("--role must be"), "{host}: {output}");
        assert!(
            run_host.contains(&format!("\n  {host})")),
            "{host} has no arm in run-host"
        );
    }
}

#[test]
fn test_the_archiver_grant_names_each_writable_prefix() {
    let provision_host = std::fs::read_to_string(script("provision-host")).unwrap();
    for prefix in Host::Archiver.writable_prefixes() {
        let templated = prefix.replace("producer=archiver", "producer=${ROLE}");
        assert!(
            provision_host.contains(&format!("/{templated}*\"")),
            "{prefix} is not granted"
        );
    }
}
