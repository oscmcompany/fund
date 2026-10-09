//! What `host/provision-host` and `host/run-role` answer for each role before either reaches AWS or builds.

use std::path::PathBuf;
use std::process::Command;

use fund::common::storage::Host;
use strum::IntoEnumIterator;

fn script(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("host")
        .join(name)
}

/// The exit code and output of `name` run with `arguments`, with no `FUND_PROFILE` in its environment.
fn run(name: &str, arguments: &[&str]) -> (i32, String) {
    let output = Command::new("bash")
        .arg(script(name))
        .args(arguments)
        .env_remove("FUND_PROFILE")
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

/// The provisioner's answer for `role` under a malformed profile, which stops it before any AWS call.
fn provision(role: &str) -> (i32, String) {
    run(
        "provision-host",
        &["--role", role, "--profile", "not-a-profile"],
    )
}

/// The role runner's answer for `role` without a profile, which stops it before any build.
fn run_role(role: &str) -> (i32, String) {
    run("run-role", &[role])
}

/// Each role beside its exit code and first line of output from `answer`.
fn answers(answer: fn(&str) -> (i32, String)) -> Vec<(String, i32, String)> {
    ["archiver", "trader", "researcher", "accountant"]
        .into_iter()
        .map(|role| {
            let (code, output) = answer(role);
            (
                role.to_string(),
                code,
                output.lines().next().unwrap_or("").to_string(),
            )
        })
        .collect()
}

#[test]
fn test_each_role_is_answered_before_any_aws_call() {
    assert_eq!(
        answers(provision),
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
fn test_each_role_is_answered_before_any_build() {
    assert_eq!(
        answers(run_role),
        [
            (
                "archiver".to_string(),
                1,
                "Error: FUND_PROFILE is not set; it names the profile whose secrets the archiver reads"
                    .to_string()
            ),
            (
                "trader".to_string(),
                1,
                "Error: the trader role is not built yet".to_string()
            ),
            (
                "researcher".to_string(),
                0,
                "The researcher role is reserved: studies run on demand and nothing schedules one yet"
                    .to_string()
            ),
            (
                "accountant".to_string(),
                1,
                "Error: the role is \"accountant\", not archiver, trader or researcher".to_string()
            ),
        ]
    );
}

#[test]
fn test_every_host_is_a_role_of_both_scripts() {
    for host in Host::iter() {
        let (_, provisioned) = provision(&host.to_string());
        assert!(
            !provisioned.contains("--role must be"),
            "{host}: {provisioned}"
        );
        let (_, ran) = run_role(&host.to_string());
        assert!(
            !ran.contains("not archiver, trader or researcher"),
            "{host}: {ran}"
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
