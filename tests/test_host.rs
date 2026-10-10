//! What `host/provision-host` and `host/run-role` answer for each role, and what the provisioner writes, run whole
//! against a scripted `aws` in place of AWS.

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
                "Error: --profile must be production or development/<name>; got \"not-a-profile\""
                    .to_string()
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
                "Error: FUND_PROFILE is not set; it names the profile whose secrets the trader reads"
                    .to_string()
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

/// Stands in for the `aws` CLI: answers each call from `STUB_*`, saves every policy and schedule target it is handed
/// under `STUB_DIRECTORY`, and logs each call's service and operation to `calls`.
const STUB: &str = r#"#!/usr/bin/env bash
echo "$1 $2" >> "$STUB_DIRECTORY/calls"
value_of() {
  local flag="$1"; shift
  while [[ $# -gt 0 ]]; do [[ "$1" == "$flag" ]] && { echo "$2"; return; }; shift; done
}
case "$1 $2" in
  "sts get-caller-identity") echo 123456789012 ;;
  "iam get-role") [[ -n "${STUB_ROLE_OWNER+set}" ]] || exit 254 ;;
  "iam list-role-tags") echo "${STUB_ROLE_OWNER}" ;;
  "iam get-instance-profile" | "scheduler get-schedule") exit 254 ;;
  "ec2 describe-vpcs") echo vpc-1 ;;
  "ec2 describe-security-groups") echo None ;;
  "ec2 create-security-group") echo sg-1 ;;
  "ssm get-parameter") echo ami-1 ;;
  "ec2 run-instances") echo i-0new ;;
  "ec2 describe-instances")
    case "$*" in
      *State.Name*) echo "${STUB_STATE:-stopped}" ;;
      *tag:fund-role*) echo "${STUB_INSTANCE:-}" ;;
      *) echo sg-1 ;;
    esac ;;
  "iam put-role-policy")
    value_of --policy-document "$@" > "$STUB_DIRECTORY/policy-$(value_of --policy-name "$@").json" ;;
  "scheduler create-schedule" | "scheduler update-schedule")
    value_of --target "$@" > "$STUB_DIRECTORY/target-$(value_of --name "$@").json" ;;
  *) echo None ;;
esac
"#;

/// A run of the provisioner for the archiver under `profile` with `--apply`, the stub answering from `environment`.
struct Provisioned {
    code: i32,
    output: String,
    directory: PathBuf,
}

impl Provisioned {
    fn run(profile: &str, environment: &[(&str, &str)]) -> Self {
        Self::run_role("archiver", profile, environment)
    }

    fn run_role(role: &str, profile: &str, environment: &[(&str, &str)]) -> Self {
        let directory =
            std::env::temp_dir().join(format!("provision-host-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&directory).unwrap();
        for (name, body) in [
            ("aws", STUB),
            ("uuidgen", "#!/usr/bin/env bash\necho token\n"),
        ] {
            let path = directory.join(name);
            std::fs::write(&path, body).unwrap();
            let mut permissions = std::fs::metadata(&path).unwrap().permissions();
            std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
            std::fs::set_permissions(&path, permissions).unwrap();
        }
        let path = format!("{}:{}", directory.display(), std::env::var("PATH").unwrap());
        let output = Command::new("bash")
            .arg(script("provision-host"))
            .args(["--role", role, "--profile", profile, "--apply"])
            .env("PATH", path)
            .env("STUB_DIRECTORY", &directory)
            .env("BOOTSTRAP_POLL_SECONDS", "0")
            .envs(environment.iter().copied())
            .output()
            .unwrap();
        Self {
            code: output.status.code().unwrap(),
            output: format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            ),
            directory,
        }
    }

    fn calls(&self) -> Vec<String> {
        std::fs::read_to_string(self.directory.join("calls"))
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect()
    }

    fn json(&self, name: &str) -> serde_json::Value {
        let text = std::fs::read_to_string(self.directory.join(name)).unwrap();
        serde_json::from_str(&text).unwrap_or_else(|error| panic!("{name}: {error}: {text}"))
    }
}

impl Drop for Provisioned {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

/// Whether `policy` holds an `Allow` statement granting `action` on exactly `resource`.
fn allows(policy: &serde_json::Value, action: &str, resource: &str) -> bool {
    policy["Statement"]
        .as_array()
        .unwrap()
        .iter()
        .any(|statement| {
            let listed = |field: &str, wanted: &str| match &statement[field] {
                serde_json::Value::String(value) => value == wanted,
                serde_json::Value::Array(values) => values.iter().any(|value| value == wanted),
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Object(_) => false,
            };
            statement["Effect"] == "Allow"
                && listed("Action", action)
                && listed("Resource", resource)
        })
}

#[test]
fn test_a_new_archiver_is_granted_its_prefixes_and_scheduled_once_bootstrapped() {
    let provisioned = Provisioned::run("production", &[]);
    assert_eq!(provisioned.code, 0, "{}", provisioned.output);

    let mut written: Vec<String> = std::fs::read_dir(&provisioned.directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.ends_with(".json"))
        .collect();
    written.sort();
    assert_eq!(
        written,
        [
            "policy-archiver.json",
            "policy-start-and-stop.json",
            "target-fund-archiver-production-start.json",
            "target-fund-archiver-production-stop.json",
        ]
    );

    let grant = provisioned.json("policy-archiver.json");
    for (bucket, prefix) in [
        ("oscm-fund-archive", "data/equity/"),
        ("oscm-fund-production", "records/journal/producer=archiver/"),
        ("oscm-fund-production", "records/logs/producer=archiver/"),
    ] {
        let resource = format!("arn:aws:s3:::{bucket}/{prefix}*");
        assert!(
            allows(&grant, "s3:PutObject", &resource),
            "{resource} is not writable: {grant}"
        );
    }
    let mut granted: Vec<String> = Host::Archiver.writable_prefixes();
    granted.sort();
    assert_eq!(
        granted,
        [
            "data/equity/",
            "records/journal/producer=archiver/",
            "records/logs/producer=archiver/",
        ]
    );

    let secrets =
        "arn:aws:secretsmanager:us-east-1:123456789012:secret:secretspec/fund/production/*";
    assert!(
        allows(&grant, "secretsmanager:GetSecretValue", secrets),
        "{grant}"
    );
    assert!(
        allows(&grant, "secretsmanager:BatchGetSecretValue", "*"),
        "{grant}"
    );

    let scheduler = provisioned.json("policy-start-and-stop.json");
    let instance = "arn:aws:ec2:us-east-1:123456789012:instance/i-0new";
    assert!(
        allows(&scheduler, "ec2:StartInstances", instance),
        "{scheduler}"
    );
    assert!(
        allows(&scheduler, "ec2:StopInstances", instance),
        "{scheduler}"
    );
    for (schedule, action) in [("start", "startInstances"), ("stop", "stopInstances")] {
        let target = provisioned.json(&format!("target-fund-archiver-production-{schedule}.json"));
        assert_eq!(
            target["Arn"],
            format!("arn:aws:scheduler:::aws-sdk:ec2:{action}")
        );
        let input: serde_json::Value =
            serde_json::from_str(target["Input"].as_str().unwrap()).unwrap();
        assert_eq!(input, serde_json::json!({"InstanceIds": ["i-0new"]}));
    }

    let calls = provisioned.calls();
    let granted_at = calls
        .iter()
        .position(|call| call == "iam put-role-policy")
        .unwrap();
    let launched = calls
        .iter()
        .position(|call| call == "ec2 run-instances")
        .unwrap();
    assert!(
        granted_at < launched,
        "the bootstrap ran without its grant: {calls:?}"
    );
    let bootstrapped = calls
        .iter()
        .position(|call| call == "ec2 create-tags")
        .unwrap();
    let scheduled = calls
        .iter()
        .position(|call| call == "scheduler create-schedule")
        .unwrap();
    assert!(bootstrapped < scheduled, "{calls:?}");
}

#[test]
fn test_an_untagged_role_is_tagged_and_an_owned_one_is_left_alone() {
    let untagged = Provisioned::run("production", &[("STUB_ROLE_OWNER", "")]);
    let owned = Provisioned::run("production", &[("STUB_ROLE_OWNER", "production")]);
    for (provisioned, tagged) in [(&untagged, true), (&owned, false)] {
        assert_eq!(provisioned.code, 0, "{}", provisioned.output);
        let calls = provisioned.calls();
        assert!(
            calls
                .iter()
                .any(|call| call == "iam update-assume-role-policy"),
            "{calls:?}"
        );
        assert_eq!(
            calls.iter().any(|call| call == "iam tag-role"),
            tagged,
            "{calls:?}"
        );
    }
}

#[test]
fn test_a_refused_box_is_never_scheduled_and_only_a_bootstrapping_one_is_granted() {
    let unfinished = Provisioned::run(
        "production",
        &[(
            "STUB_INSTANCE",
            "i-0old r6i.2xlarge arn:aws:iam::1:instance-profile/fund-archiver-production required enabled None",
        )],
    );
    let still_running = Provisioned::run(
        "production",
        &[("STUB_STATE", "running"), ("BOOTSTRAP_POLLS", "2")],
    );
    let collided = Provisioned::run("development/a.b", &[("STUB_ROLE_OWNER", "development/a-b")]);
    // A box still bootstrapping holds its grant already, since its bootstrap enters devenv and reads secrets.
    let every_write = [
        "iam put-role-policy",
        "iam tag-role",
        "ec2 create-tags",
        "iam update-assume-role-policy",
        "scheduler create-schedule",
    ];
    for (provisioned, refusal, forbidden) in [
        (
            &unfinished,
            "never finished its bootstrap",
            &every_write[..],
        ),
        (
            &still_running,
            "its bootstrap failed or is still running",
            &["ec2 create-tags", "scheduler create-schedule"][..],
        ),
        (
            &collided,
            "belongs to profile development/a-b",
            &every_write[..],
        ),
    ] {
        assert_eq!(provisioned.code, 1, "{}", provisioned.output);
        assert!(
            provisioned.output.contains(refusal),
            "{}",
            provisioned.output
        );
        let calls = provisioned.calls();
        assert!(!calls.is_empty());
        for write in forbidden {
            assert!(
                !calls.iter().any(|call| call == write),
                "{write} ran: {calls:?}"
            );
        }
    }
}

#[test]
fn test_a_new_trader_reads_its_playbook_and_writes_only_its_own_records() {
    let provisioned = Provisioned::run_role("trader", "development/a.b", &[]);
    assert_eq!(provisioned.code, 0, "{}", provisioned.output);
    let grant = provisioned.json("policy-trader.json");
    let records = "arn:aws:s3:::oscm-fund-development-a-b";
    let mut granted: Vec<(String, String, String)> = grant["Statement"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|statement| {
            let listed = |field: &str| match &statement[field] {
                serde_json::Value::Array(values) => values
                    .iter()
                    .map(|value| value.as_str().unwrap().to_string())
                    .collect(),
                serde_json::Value::String(value) => vec![value.clone()],
                serde_json::Value::Null
                | serde_json::Value::Bool(_)
                | serde_json::Value::Number(_)
                | serde_json::Value::Object(_) => panic!("{field} is not a string or a list"),
            };
            let condition = statement["Condition"].to_string();
            listed("Action")
                .into_iter()
                .filter(|action| action.starts_with("s3:"))
                .flat_map(|action| {
                    listed("Resource")
                        .into_iter()
                        .map(move |resource| (action.clone(), resource))
                })
                .map(move |(action, resource)| (action, resource, condition.clone()))
                .collect::<Vec<_>>()
        })
        .collect();
    granted.sort();
    let unconditioned = "null".to_string();
    assert_eq!(
        granted,
        [
            (
                "s3:GetObject".to_string(),
                "arn:aws:s3:::oscm-fund-archive/data/equity/*".to_string(),
                unconditioned.clone()
            ),
            (
                "s3:GetObject".to_string(),
                format!("{records}/configuration/playbook.toml"),
                unconditioned.clone()
            ),
            (
                "s3:GetObject".to_string(),
                format!("{records}/records/journal/producer=trader/*"),
                unconditioned.clone()
            ),
            (
                "s3:GetObject".to_string(),
                format!("{records}/records/logs/producer=trader/*"),
                unconditioned.clone()
            ),
            (
                "s3:ListBucket".to_string(),
                "arn:aws:s3:::oscm-fund-archive".to_string(),
                r#"{"StringLike":{"s3:prefix":["data/equity/*"]}}"#.to_string()
            ),
            (
                "s3:ListBucket".to_string(),
                records.to_string(),
                unconditioned.clone()
            ),
            (
                "s3:PutObject".to_string(),
                format!("{records}/records/journal/producer=trader/*"),
                unconditioned.clone()
            ),
            (
                "s3:PutObject".to_string(),
                format!("{records}/records/logs/producer=trader/*"),
                unconditioned
            ),
        ]
    );
    let start = provisioned.json("target-fund-trader-development-a-b-start.json");
    assert_eq!(
        start["Arn"],
        "arn:aws:scheduler:::aws-sdk:ec2:startInstances"
    );
    assert!(
        provisioned.output.contains("cron(30 12 ? * MON-FRI *)")
            && provisioned.output.contains("cron(30 21 ? * MON-FRI *)"),
        "{}",
        provisioned.output
    );
}
