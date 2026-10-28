//! What the `host/` scripts answer for each role and command, and what the provisioner writes, run whole against a
//! scripted `aws` in place of AWS.

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
                1,
                "Error: --profile must be production or development/<name>; got \"not-a-profile\""
                    .to_string()
            ),
            (
                "accountant".to_string(),
                1,
                "Error: --role must be archiver, trader or researcher; got \"accountant\""
                    .to_string()
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
                1,
                "Error: FUND_PROFILE is not set; it names the profile whose secrets the researcher reads"
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
fn test_an_unheld_researcher_runs_nothing_and_builds_nothing() {
    let directory = std::env::temp_dir().join(format!("run-role-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let cargo = directory.join("cargo");
    std::fs::write(
        &cargo,
        "#!/usr/bin/env bash\necho \"cargo was called\"\nexit 99\n",
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&cargo).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    std::fs::set_permissions(&cargo, permissions).unwrap();
    let output = Command::new("bash")
        .arg(script("run-role"))
        .arg("researcher")
        .env(
            "PATH",
            format!("{}:{}", directory.display(), std::env::var("PATH").unwrap()),
        )
        .env("FUND_PROFILE", "development/a.b")
        .output()
        .unwrap();
    std::fs::remove_dir_all(&directory).unwrap();
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "The researcher runs only what host/researcher sends it; nothing to run\n"
    );
}

#[test]
fn test_a_researcher_command_is_refused_before_any_aws_call() {
    let directory = std::env::temp_dir().join(format!("researcher-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    let aws = directory.join("aws");
    std::fs::write(
        &aws,
        "#!/usr/bin/env bash\necho \"aws was called\"\nexit 99\n",
    )
    .unwrap();
    let mut permissions = std::fs::metadata(&aws).unwrap().permissions();
    std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
    std::fs::set_permissions(&aws, permissions).unwrap();
    let path = format!("{}:{}", directory.display(), std::env::var("PATH").unwrap());
    let usage = "Usage: researcher <command>";
    let study = |study: &str| {
        format!(
            "Error: the study is \"{study}\", not a name of lowercase letters, digits and underscores"
        )
    };
    let hours =
        |hours: &str| format!("Error: the hours are \"{hours}\", not a whole number from 1 to 24");
    let profile = |profile: &str| {
        format!("Error: FUND_PROFILE must be production or development/<name>; got \"{profile}\"")
    };
    for (arguments, fund_profile, refusal) in [
        (vec![], "development/a.b", usage.to_string()),
        (vec!["launch"], "development/a.b", usage.to_string()),
        (vec!["start", "extra"], "development/a.b", usage.to_string()),
        (vec!["run"], "development/a.b", usage.to_string()),
        (
            vec!["run", "../trader"],
            "development/a.b",
            study("../trader"),
        ),
        (vec!["log", "Study"], "development/a.b", study("Study")),
        (vec!["detach", "0", "probe"], "development/a.b", hours("0")),
        (
            vec!["detach", "25", "probe"],
            "development/a.b",
            hours("25"),
        ),
        (vec!["detach", "1"], "development/a.b", usage.to_string()),
        (vec!["detach", "1", "a b"], "development/a.b", study("a b")),
        (vec!["push"], "", profile("")),
        (vec!["push"], "development", profile("development")),
        (vec!["stop"], "staging", profile("staging")),
    ] {
        let output = Command::new("bash")
            .arg(script("researcher"))
            .args(&arguments)
            .env("PATH", &path)
            .env("FUND_PROFILE", fund_profile)
            .output()
            .unwrap();
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.status.code(), Some(1), "{arguments:?}");
        assert_eq!(text.lines().next(), Some(refusal.as_str()), "{arguments:?}");
    }
    std::fs::remove_dir_all(&directory).unwrap();
}

/// A temporary home for `researcher`, where `ssh` runs the command it is handed with the upload on its input, so a
/// push really unpacks into `working-tree`, and `devenv`, `secretspec`, `cargo`, `sudo` and `flock` are stubs; `cargo`
/// and a failing `sudo` write their arguments to `cargo` and `sudoed`, `flock -n <fd>` takes a real lock through Perl
/// since macOS has no `flock`, and `ssh` rewrites the tree's `.upload` before any command but a push while the file
/// `replace` exists.
fn researcher_home() -> PathBuf {
    let directory = std::env::temp_dir().join(format!("researcher-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&directory).unwrap();
    for (name, stub) in [
        (
            "aws",
            "#!/usr/bin/env bash\n[[ \"$1 $2\" != \"ec2 describe-instances\" ]] || echo i-0123abcd\n",
        ),
        (
            "ssh",
            "#!/usr/bin/env bash\ncd \"$HOME\"\nif [[ -e replace && \"${@: -1}\" != *\"> .upload\"* ]]; then echo other > working-tree/.upload; fi\nexec bash -c \"${@: -1}\"\n",
        ),
        (
            "ssh-keygen",
            "#!/usr/bin/env bash\ntouch \"${@: -1}\" \"${@: -1}.pub\"\n",
        ),
        (
            "sudo",
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$HOME/sudoed\"\nexit 1\n",
        ),
        (
            "flock",
            "#!/usr/bin/env bash\n[[ \"$1 $#\" == \"-n 2\" ]] || exit 2\nexec perl -MFcntl=:flock -e 'open(my $lock, \">&=\", $ARGV[0]) or exit 2; flock($lock, LOCK_EX | LOCK_NB) or exit 1' \"$2\"\n",
        ),
        (
            ".nix-profile/bin/devenv",
            "#!/usr/bin/env bash\nshift 2\nexec \"$@\"\n",
        ),
        (
            ".nix-profile/bin/secretspec",
            "#!/usr/bin/env bash\nwhile [[ \"$1\" != -- ]]; do shift; done\nshift\nUNDER_SECRETSPEC=yes exec \"$@\"\n",
        ),
        (
            ".nix-profile/bin/cargo",
            "#!/usr/bin/env bash\nprintf '%s\\n' \"$@\" > \"$HOME/cargo\"\n",
        ),
    ] {
        let path = directory.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, stub).unwrap();
        let mut permissions = std::fs::metadata(&path).unwrap().permissions();
        std::os::unix::fs::PermissionsExt::set_mode(&mut permissions, 0o755);
        std::fs::set_permissions(&path, permissions).unwrap();
    }
    directory
}

/// Held by each test that pushes, since a push tars the repository while `job_run` adds a job to it and removes it.
static PUSHING: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Runs `researcher` with `arguments` from the repository against the stubs in `home`.
fn researcher(home: &PathBuf, arguments: &[&str]) -> std::process::Output {
    Command::new("bash")
        .arg(script("researcher"))
        .args(arguments)
        .env(
            "PATH",
            format!("{}:{}", home.display(), std::env::var("PATH").unwrap()),
        )
        .env("HOME", home)
        .env("FUND_PROFILE", "development/a.b")
        .output()
        .unwrap()
}

/// What a throwaway `jobs/<name>.sh` pushed and run by `researcher run` with `arguments` saw: whether it ran under
/// secretspec, whether the working tree's lock was held, then its argument count and arguments.
fn job_run(arguments: &[&str]) -> Vec<String> {
    let _pushing = PUSHING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let home = researcher_home();
    let job = format!("probe_{}", uuid::Uuid::new_v4().simple());
    let body = format!(
        "printf '%s\\n' \"${{UNDER_SECRETSPEC:-no}}\" \"$({TRY_LOCK} \"$HOME/working-tree.lock\")\" \"$#\" \"$@\" > \"$HOME/job\"\n"
    );
    let jobs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("jobs");
    std::fs::create_dir_all(&jobs).unwrap();
    std::fs::write(jobs.join(format!("{job}.sh")), body).unwrap();
    let output = researcher(&home, &[&["run", job.as_str()], arguments].concat());
    std::fs::remove_file(jobs.join(format!("{job}.sh"))).unwrap();
    let seen = std::fs::read_to_string(home.join("job"));
    let uploaded = home.join(format!("working-tree/jobs/{job}.sh")).exists();
    std::fs::remove_dir_all(&home).unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(uploaded, "the job was not unpacked into the working tree");
    seen.unwrap().lines().map(str::to_string).collect()
}

#[test]
fn test_a_job_runs_under_secretspec_with_exactly_the_arguments_it_was_given() {
    assert_eq!(job_run(&[]), ["yes", "held", "0"]);
    assert_eq!(
        job_run(&["a b", "it's", ""]),
        ["yes", "held", "3", "a b", "it's", ""]
    );
}

#[test]
fn test_a_study_receives_its_arguments_after_cargo_has_read_its_own() {
    let _pushing = PUSHING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let home = researcher_home();
    let output = researcher(&home, &["run", "probe", "--help", "a b"]);
    let cargo = std::fs::read_to_string(home.join("cargo"));
    let key = home.join(".ssh/fund-researcher").exists();
    std::fs::remove_dir_all(&home).unwrap();
    assert_eq!(
        output.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(key, "no key was generated in a home without ~/.ssh");
    assert_eq!(
        cargo.unwrap().lines().collect::<Vec<_>>(),
        [
            "run",
            "-q",
            "--profile",
            "study",
            "-p",
            "studies",
            "--bin",
            "probe",
            "--",
            "--help",
            "a b"
        ]
    );
}

/// A shell command that prints `held` when another open file holds the lock on the file it is given, else `free`.
const TRY_LOCK: &str = "perl -MFcntl=:flock -e 'open(my $lock, \">>\", $ARGV[0]) or die; print flock($lock, LOCK_EX | LOCK_NB) ? \"free\" : \"held\"'";

#[test]
fn test_a_push_refuses_while_a_study_holds_the_working_tree_and_proceeds_once_it_ends() {
    let _pushing = PUSHING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let home = researcher_home();
    std::fs::create_dir_all(home.join("working-tree")).unwrap();
    std::fs::write(home.join("working-tree/running"), "").unwrap();
    let mut study = Command::new("perl")
        .args([
            "-MFcntl=:flock",
            "-e",
            "open(my $lock, '>>', $ARGV[0]) or die; flock($lock, LOCK_EX) or die; $| = 1; print \"held\\n\"; <STDIN>",
        ])
        .arg(home.join("working-tree.lock"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let mut held = String::new();
    std::io::BufRead::read_line(
        &mut std::io::BufReader::new(study.stdout.take().unwrap()),
        &mut held,
    )
    .unwrap();
    assert_eq!(held, "held\n");
    let refused = researcher(&home, &["push"]);
    let kept = home.join("working-tree/running").exists();
    drop(study.stdin.take());
    study.wait().unwrap();
    let pushed = researcher(&home, &["push"]);
    let replaced = !home.join("working-tree/running").exists();
    std::fs::remove_dir_all(&home).unwrap();
    assert_eq!(refused.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&refused.stderr).lines().next(),
        Some("Error: a study is still running in ~/working-tree; wait for it or stop the box")
    );
    assert!(kept, "the running study's files were replaced");
    assert_eq!(
        pushed.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&pushed.stderr)
    );
    assert!(replaced, "the push after the study ended left the old tree");
}

#[test]
fn test_a_run_refuses_a_tree_another_push_replaced_before_it_ran() {
    let _pushing = PUSHING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let home = researcher_home();
    std::fs::write(home.join("replace"), "").unwrap();
    let output = researcher(&home, &["run", "probe"]);
    let ran = home.join("cargo").exists();
    std::fs::remove_dir_all(&home).unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).lines().last(),
        Some("Error: another push replaced this upload before it could run")
    );
    assert!(!ran, "the study ran from another push's files");
}

#[test]
fn test_a_detach_refuses_a_tree_another_push_replaced_before_it_started() {
    let _pushing = PUSHING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let home = researcher_home();
    std::fs::write(home.join("replace"), "").unwrap();
    let output = researcher(&home, &["detach", "1", "probe"]);
    let started = home.join("runs").exists() || home.join("sudoed").exists();
    std::fs::remove_dir_all(&home).unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).lines().last(),
        Some("Error: another push replaced this upload before it could run")
    );
    assert!(
        !started,
        "the detached run started from another push's files"
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

/// Stands in for the `aws` CLI: answers each call from `STUB_*`, saves every policy, schedule target and lifecycle
/// configuration it is handed under `STUB_DIRECTORY`, and logs each call's service and operation to `calls`.
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
  "iam get-instance-profile") exit 254 ;;
  "scheduler get-schedule")
    case "${STUB_SCHEDULE:-}" in
      "") echo "An error occurred (ResourceNotFoundException) when calling the GetSchedule operation" >&2; exit 254 ;;
      denied) echo "An error occurred (AccessDeniedException) when calling the GetSchedule operation" >&2; exit 254 ;;
    esac ;;
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
  "s3api put-bucket-lifecycle-configuration")
    value_of --lifecycle-configuration "$@" > "$STUB_DIRECTORY/lifecycle-$(value_of --bucket "$@").json"
    value_of --transition-default-minimum-object-size "$@" > "$STUB_DIRECTORY/lifecycle-$(value_of --bucket "$@").size" ;;
  "scheduler create-schedule" | "scheduler update-schedule")
    value_of --target "$@" > "$STUB_DIRECTORY/target-$(value_of --name "$@").json"
    echo "$(value_of --schedule-expression "$@") $(value_of --schedule-expression-timezone "$@")" \
      > "$STUB_DIRECTORY/schedule-$(value_of --name "$@").txt" ;;
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

    /// The expression and timezone the schedule `name` was last written with, space-separated.
    fn schedule(&self, name: &str) -> String {
        let path = self.directory.join(format!("schedule-{name}.txt"));
        std::fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("{name}: {error}"))
            .trim_end()
            .to_string()
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

/// Every resource an unconditional `Allow` statement grants `action` on, its action listed outright or matched by a `*` wildcard.
fn allowed_resources(policy: &serde_json::Value, action: &str) -> Vec<String> {
    let strings = |value: &serde_json::Value| -> Vec<String> {
        match value {
            serde_json::Value::String(value) => vec![value.clone()],
            serde_json::Value::Array(values) => values
                .iter()
                .filter_map(|value| value.as_str().map(String::from))
                .collect(),
            serde_json::Value::Null
            | serde_json::Value::Bool(_)
            | serde_json::Value::Number(_)
            | serde_json::Value::Object(_) => Vec::new(),
        }
    };
    policy["Statement"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|statement| statement["Effect"] == "Allow" && statement["Condition"].is_null())
        .filter(|statement| {
            strings(&statement["Action"]).iter().any(|pattern| {
                wildcard_matches(&pattern.to_ascii_lowercase(), &action.to_ascii_lowercase())
            })
        })
        .flat_map(|statement| strings(&statement["Resource"]))
        .collect()
}

/// Whether IAM's `*` wildcard `pattern` matches `value`, case-sensitively as resource ARNs are.
fn wildcard_matches(pattern: &str, value: &str) -> bool {
    let pieces: Vec<&str> = pattern.split('*').collect();
    let (first, wildcarded) = pieces.split_first().unwrap();
    let Some(mut rest) = value.strip_prefix(first) else {
        return false;
    };
    match wildcarded.split_last() {
        None => rest.is_empty(),
        Some((last, middle)) => {
            for piece in middle {
                match rest.find(piece) {
                    Some(index) => rest = &rest[index + piece.len()..],
                    None => return false,
                }
            }
            rest.ends_with(last)
        }
    }
}

#[test]
fn test_an_iam_wildcard_matches_the_actions_it_names() {
    for (pattern, matches) in [
        ("s3:DeleteObject", true),
        ("s3:*Object", true),
        ("s3:*", true),
        ("*", true),
        ("S3:deleteobject", false),
        ("*Object", true),
        ("s3:**Object", true),
        ("s3:DeleteObject*", true),
        ("s3:*e*e*Object", true),
        ("s3:Delete*teObject", false),
        ("s3:DeleteObj*Object", false),
        ("s3:DeleteObjec", false),
        ("s3:Delete*Version", false),
        ("s3:DeleteObjectVersion", false),
        ("s3:Get*", false),
    ] {
        assert_eq!(
            wildcard_matches(pattern, "s3:DeleteObject"),
            matches,
            "{pattern}"
        );
    }
}

/// Whether a `Deny` statement listing `action` refuses `resource` because it falls outside the statement's `NotResource`.
/// Models only a single `NotResource` string ending in `*`, matched as a prefix; any other shape refuses nothing.
fn denies_outside(policy: &serde_json::Value, action: &str, resource: &str) -> bool {
    policy["Statement"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|statement| statement["Effect"] == "Deny")
        .any(|statement| {
            let listed = statement["Action"]
                .as_array()
                .is_some_and(|actions| actions.iter().any(|value| value == action));
            let outside = statement["NotResource"]
                .as_str()
                .and_then(|pattern| pattern.strip_suffix('*'))
                .is_some_and(|prefix| !resource.starts_with(prefix));
            listed && outside
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
            "lifecycle-oscm-fund-archive.json",
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
    // The heal deletes a session's parsed key before re-fetching it, and nothing may delete the raw tee.
    let parsed = "arn:aws:s3:::oscm-fund-archive/data/equity/stage=parsed/*";
    assert!(allows(&grant, "s3:DeleteObject", parsed), "{grant}");
    assert_eq!(
        allowed_resources(&grant, "s3:DeleteObject"),
        [parsed],
        "{grant}"
    );
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

    let secrets = "arn:aws:ssm:us-east-1:123456789012:parameter/secretspec/fund/production/*";
    assert!(allows(&grant, "ssm:GetParameter", secrets), "{grant}");
    assert!(allows(&grant, "ssm:GetParameters", secrets), "{grant}");
    let parameters = "arn:aws:ssm:us-east-1:123456789012:parameter/secretspec/fund/";
    for action in [
        "ssm:GetParameter",
        "ssm:GetParameters",
        "ssm:GetParametersByPath",
        "ssm:GetParameterHistory",
    ] {
        assert!(
            !denies_outside(
                &grant,
                action,
                &format!("{parameters}production/ALPACA_API_SECRET")
            ),
            "{grant}"
        );
        assert!(
            denies_outside(
                &grant,
                action,
                &format!("{parameters}development/john.forstmeier/ALPACA_API_SECRET")
            ),
            "{grant}"
        );
    }

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
    assert_eq!(
        provisioned.schedule("fund-archiver-production-start"),
        "cron(0 3 * * ? *) America/New_York"
    );
    assert_eq!(
        provisioned.schedule("fund-archiver-production-stop"),
        "cron(0 8 * * ? *) America/New_York"
    );

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
fn test_only_production_provisions_an_archiver() {
    let development = Provisioned::run("development/a.b", &[]);
    assert_eq!(development.code, 1, "{}", development.output);
    assert_eq!(
        development.output,
        "Error: --role archiver writes the archive bucket, which only --profile production may; got \"development/a.b\"\n"
    );
    assert_eq!(development.calls(), Vec::<String>::new());
    let researcher = Provisioned::run_role("researcher", "development/a.b", &[]);
    assert!(!researcher.calls().is_empty(), "{}", researcher.output);
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
    let collided = Provisioned::run_role(
        "trader",
        "development/a.b",
        &[("STUB_ROLE_OWNER", "development/a-b")],
    );
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

/// Every `(action, resource, condition)` an `Allow` statement of `policy` grants on S3, sorted, the condition as JSON.
fn s3_grants(policy: &serde_json::Value) -> Vec<(String, String, String)> {
    let mut granted: Vec<(String, String, String)> = policy["Statement"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|statement| statement["Effect"] == "Allow")
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
    granted
}

#[test]
fn test_a_new_researcher_reads_the_archive_writes_its_records_and_scratch_and_is_never_scheduled() {
    let provisioned = Provisioned::run_role("researcher", "development/john.forstmeier", &[]);
    assert_eq!(provisioned.code, 0, "{}", provisioned.output);
    let mut written: Vec<String> = std::fs::read_dir(&provisioned.directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name.ends_with(".json"))
        .collect();
    written.sort();
    assert_eq!(written, ["policy-researcher.json"]);

    let records = "arn:aws:s3:::oscm-fund-development-john-forstmeier";
    let grant = |action: &str, resource: String| (action.to_string(), resource, "null".to_string());
    assert_eq!(
        s3_grants(&provisioned.json("policy-researcher.json")),
        [
            grant("s3:AbortMultipartUpload", format!("{records}/scratch/*")),
            grant("s3:DeleteObject", format!("{records}/scratch/*")),
            grant(
                "s3:GetObject",
                "arn:aws:s3:::oscm-fund-archive/data/*".to_string()
            ),
            grant(
                "s3:GetObject",
                format!("{records}/records/journal/producer=researcher/*")
            ),
            grant(
                "s3:GetObject",
                format!("{records}/records/logs/producer=researcher/*")
            ),
            grant(
                "s3:GetObject",
                format!("{records}/records/sources/producer=researcher/*")
            ),
            grant("s3:GetObject", format!("{records}/scratch/*")),
            grant(
                "s3:ListBucket",
                "arn:aws:s3:::oscm-fund-archive".to_string()
            ),
            grant("s3:ListBucket", records.to_string()),
            grant(
                "s3:PutObject",
                format!("{records}/records/journal/producer=researcher/*")
            ),
            grant(
                "s3:PutObject",
                format!("{records}/records/logs/producer=researcher/*")
            ),
            grant(
                "s3:PutObject",
                format!("{records}/records/sources/producer=researcher/*")
            ),
            grant("s3:PutObject", format!("{records}/scratch/*")),
        ]
    );
    let policy = provisioned.json("policy-researcher.json");
    for prefix in Host::Researcher.writable_prefixes() {
        let resource = format!("{records}/{prefix}*");
        assert!(
            allows(&policy, "s3:PutObject", &resource),
            "{resource} is not writable: {policy}"
        );
    }

    let calls = provisioned.calls();
    assert!(
        calls.iter().any(|call| call == "ec2 run-instances"),
        "{calls:?}"
    );
    for call in &calls {
        assert!(
            !call.starts_with("scheduler ") || call == "scheduler get-schedule",
            "{call}: {calls:?}"
        );
    }
    assert!(
        provisioned.output.contains("Start:    on demand"),
        "{}",
        provisioned.output
    );

    let stale = Provisioned::run_role("researcher", "production", &[("STUB_SCHEDULE", "found")]);
    assert_eq!(stale.code, 0, "{}", stale.output);
    let deleted = stale
        .calls()
        .iter()
        .filter(|call| *call == "scheduler delete-schedule")
        .count();
    assert_eq!(deleted, 2, "{:?}", stale.calls());
}

#[test]
fn test_an_unreadable_schedule_stops_the_researcher_before_any_delete() {
    let denied = Provisioned::run_role("researcher", "production", &[("STUB_SCHEDULE", "denied")]);
    assert_eq!(denied.code, 1, "{}", denied.output);
    assert!(
        denied.output.contains(
            "Error: could not read schedule fund-researcher-production-start: An error occurred \
             (AccessDeniedException)"
        ),
        "{}",
        denied.output
    );
    assert!(
        !denied
            .calls()
            .iter()
            .any(|call| call == "scheduler delete-schedule"),
        "{:?}",
        denied.calls()
    );
}

#[test]
fn test_a_new_trader_reads_its_playbook_and_writes_only_its_own_records() {
    let provisioned = Provisioned::run_role("trader", "development/a.b", &[]);
    assert_eq!(provisioned.code, 0, "{}", provisioned.output);
    let grant = provisioned.json("policy-trader.json");
    let records = "arn:aws:s3:::oscm-fund-development-a-b";
    let unconditioned = "null".to_string();
    assert_eq!(
        s3_grants(&grant),
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
    assert_eq!(
        provisioned.schedule("fund-trader-development-a-b-start"),
        "cron(30 8 ? * MON-FRI *) America/New_York"
    );
    assert_eq!(
        provisioned.schedule("fund-trader-development-a-b-stop"),
        "cron(30 16 ? * MON-FRI *) America/New_York"
    );
}

/// Each S3 operation the crate calls on an `s3_client` field, by file: a call on it that names a bucket next.
fn s3_operations(directory: &std::path::Path) -> Vec<(String, String)> {
    struct Calls(Vec<String>);
    impl<'a> syn::visit::Visit<'a> for Calls {
        fn visit_expr_method_call(&mut self, call: &'a syn::ExprMethodCall) {
            if call.method == "bucket"
                && let syn::Expr::MethodCall(operation) = &*call.receiver
                && let syn::Expr::Field(field) = &*operation.receiver
                && matches!(&field.member, syn::Member::Named(name) if name == "s3_client")
            {
                self.0.push(operation.method.to_string());
            }
            syn::visit::visit_expr_method_call(self, call);
        }
    }
    let mut operations = Vec::new();
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            operations.extend(s3_operations(&path));
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            let file = syn::parse_file(&std::fs::read_to_string(&path).unwrap()).unwrap();
            let mut calls = Calls(Vec::new());
            syn::visit::Visit::visit_file(&mut calls, &file);
            let relative = path.strip_prefix(env!("CARGO_MANIFEST_DIR")).unwrap();
            operations.extend(
                calls
                    .0
                    .into_iter()
                    .map(|operation| (relative.display().to_string(), operation)),
            );
        }
    }
    operations
}

/// Each distinct `(file, operation)` the crate's S3 clients call on our own buckets.
fn our_s3_operations() -> Vec<(String, String)> {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut operations: Vec<(String, String)> = s3_operations(&source);
    operations.sort();
    operations.dedup();
    operations
}

#[test]
fn test_every_s3_operation_the_archive_calls_is_granted_to_the_archiver() {
    let operations = our_s3_operations();
    let mut found: Vec<&str> = operations
        .iter()
        .map(|(_, operation)| operation.as_str())
        .collect();
    found.sort();
    found.dedup();
    assert_eq!(
        found,
        [
            "abort_multipart_upload",
            "complete_multipart_upload",
            "create_multipart_upload",
            "delete_object",
            "get_object",
            "head_object",
            "list_objects_v2",
            "put_object",
            "upload_part",
        ]
    );

    let provisioned = Provisioned::run("production", &[]);
    assert_eq!(provisioned.code, 0, "{}", provisioned.output);
    let grant = provisioned.json("policy-archiver.json");
    assert_eq!(
        ungranted(&grant, &operations, archiver_reach),
        Vec::<String>::new()
    );
}

#[test]
fn test_every_s3_operation_a_study_can_call_is_granted_to_the_researcher() {
    let operations = our_s3_operations();
    assert!(!operations.is_empty());
    let provisioned = Provisioned::run_role("researcher", "production", &[]);
    assert_eq!(provisioned.code, 0, "{}", provisioned.output);
    let grant = provisioned.json("policy-researcher.json");
    assert_eq!(
        ungranted(&grant, &operations, researcher_reach),
        Vec::<String>::new()
    );
    // Of everything the archiver does to the archive, the researcher may only read and list.
    let refused = ungranted(&grant, &operations, archiver_reach);
    for (file, operation) in &operations {
        let (action, _) = archiver_reach(file, operation);
        assert_eq!(
            refused.contains(&format!("{file} {operation}")),
            !["s3:GetObject", "s3:ListBucket"].contains(&action),
            "{file} {operation}: {grant}"
        );
    }
}

const ARCHIVE_RAW: &str = "arn:aws:s3:::oscm-fund-archive/data/equity/stage=raw/key";
const ARCHIVE_PARSED: &str = "arn:aws:s3:::oscm-fund-archive/data/equity/stage=parsed/key";
const ARCHIVE_BUCKET: &str = "arn:aws:s3:::oscm-fund-archive";
const RESEARCHER_JOURNAL: &str =
    "arn:aws:s3:::oscm-fund-production/records/journal/producer=researcher/key";
const PRODUCTION_SCRATCH: &str = "arn:aws:s3:::oscm-fund-production/scratch/key";
const PRODUCTION_PROFILE_BUCKET: &str = "arn:aws:s3:::oscm-fund-production";

/// The IAM action `operation` needs and every resource the archiver reaches with it; the heal deletes only parsed keys.
fn archiver_reach(file: &str, operation: &str) -> (&'static str, &'static [&'static str]) {
    match operation {
        "get_object" | "head_object" => ("s3:GetObject", &[ARCHIVE_RAW, ARCHIVE_PARSED]),
        "put_object" | "create_multipart_upload" | "upload_part" | "complete_multipart_upload" => {
            ("s3:PutObject", &[ARCHIVE_RAW, ARCHIVE_PARSED])
        }
        "abort_multipart_upload" => ("s3:AbortMultipartUpload", &[ARCHIVE_RAW, ARCHIVE_PARSED]),
        "delete_object" => ("s3:DeleteObject", &[ARCHIVE_PARSED]),
        "list_objects_v2" => ("s3:ListBucket", &[ARCHIVE_BUCKET]),
        unmapped => panic!("{file} calls `{unmapped}`, which needs its IAM action named here"),
    }
}

/// The IAM action `operation` needs and every resource a production study reaches with it: it reads the archive and
/// writes only its journal and the scratch prefix.
fn researcher_reach(file: &str, operation: &str) -> (&'static str, &'static [&'static str]) {
    match operation {
        "get_object" | "head_object" => (
            "s3:GetObject",
            &[
                ARCHIVE_RAW,
                ARCHIVE_PARSED,
                RESEARCHER_JOURNAL,
                PRODUCTION_SCRATCH,
            ],
        ),
        "put_object" | "create_multipart_upload" | "upload_part" | "complete_multipart_upload" => {
            ("s3:PutObject", &[RESEARCHER_JOURNAL, PRODUCTION_SCRATCH])
        }
        "abort_multipart_upload" => ("s3:AbortMultipartUpload", &[PRODUCTION_SCRATCH]),
        "delete_object" => ("s3:DeleteObject", &[PRODUCTION_SCRATCH]),
        "list_objects_v2" => (
            "s3:ListBucket",
            &[ARCHIVE_BUCKET, PRODUCTION_PROFILE_BUCKET],
        ),
        unmapped => panic!("{file} calls `{unmapped}`, which needs its IAM action named here"),
    }
}

/// Each `(file, operation)` whose IAM action `policy` does not grant on every resource `reach` names for it.
fn ungranted(
    policy: &serde_json::Value,
    operations: &[(String, String)],
    reach: fn(&str, &str) -> (&'static str, &'static [&'static str]),
) -> Vec<String> {
    operations
        .iter()
        .filter(|(file, operation)| {
            let (action, resources) = reach(file, operation);
            let granted = allowed_resources(policy, action);
            !resources.iter().all(|resource| {
                granted
                    .iter()
                    .any(|pattern| wildcard_matches(pattern, resource))
            })
        })
        .map(|(file, operation)| format!("{file} {operation}"))
        .collect()
}

#[test]
fn test_a_grant_on_another_prefix_leaves_the_operation_ungranted() {
    let operations = [(
        "src/storage/raw.rs".to_string(),
        "abort_multipart_upload".to_string(),
    )];
    let policy = |resource: &str| serde_json::json!({"Statement": [{"Effect": "Allow", "Action": ["s3:AbortMultipartUpload"], "Resource": resource}]});
    assert_eq!(
        ungranted(
            &policy("arn:aws:s3:::oscm-fund-archive/data/equity/*"),
            &operations,
            archiver_reach
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        ungranted(
            &policy("arn:aws:s3:::oscm-fund-archive/data/equity/stage=parsed/*"),
            &operations,
            archiver_reach
        ),
        ["src/storage/raw.rs abort_multipart_upload"]
    );
}

#[test]
fn test_a_conditioned_grant_leaves_the_operation_ungranted() {
    let operations = [("src/storage.rs".to_string(), "list_objects_v2".to_string())];
    let statement = serde_json::json!({"Effect": "Allow", "Action": "S3:listbucket", "Resource": "arn:aws:s3:::oscm-fund-archive"});
    let mut conditioned = statement.clone();
    conditioned["Condition"] = serde_json::json!({"StringLike": {"s3:prefix": ["records/*"]}});
    assert_eq!(
        ungranted(
            &serde_json::json!({"Statement": [statement]}),
            &operations,
            archiver_reach
        ),
        Vec::<String>::new()
    );
    assert_eq!(
        ungranted(
            &serde_json::json!({"Statement": [conditioned]}),
            &operations,
            archiver_reach
        ),
        ["src/storage.rs list_objects_v2"]
    );
}

#[test]
fn test_the_archive_backstop_covers_exactly_the_keys_written_in_deep_archive() {
    use fund::common::storage::{
        JournalKey, Key, LogsKey, Provider, QuotesKey, ReferenceKey, ReferenceTable, Service,
        StorageClass, TradesKey,
    };
    use fund::common::{market::record::BarInterval, time::SessionDate};

    let provisioned = Provisioned::run("production", &[]);
    assert_eq!(provisioned.code, 0, "{}", provisioned.output);
    let rules = provisioned.json("lifecycle-oscm-fund-archive.json")["Rules"].clone();
    let mut identifiers: Vec<&str> = rules
        .as_array()
        .unwrap()
        .iter()
        .map(|rule| rule["ID"].as_str().unwrap())
        .collect();
    identifiers.sort();
    assert_eq!(
        identifiers,
        [
            "abort-incomplete-multipart-uploads",
            "raw-to-deep-archive-backstop"
        ]
    );
    let rule = |identifier: &str| {
        rules
            .as_array()
            .unwrap()
            .iter()
            .find(|rule| rule["ID"] == identifier)
            .unwrap()
            .clone()
    };
    let backstop = rule("raw-to-deep-archive-backstop");
    assert_eq!(backstop["Status"], "Enabled");
    assert_eq!(backstop["Transitions"][0]["StorageClass"], "DEEP_ARCHIVE");
    assert_eq!(backstop["Transitions"][0]["Days"], 1);
    assert_eq!(
        std::fs::read_to_string(
            provisioned
                .directory
                .join("lifecycle-oscm-fund-archive.size")
        )
        .unwrap()
        .trim(),
        "varies_by_storage_class"
    );
    let abort = rule("abort-incomplete-multipart-uploads");
    assert_eq!(abort["Status"], "Enabled");
    assert_eq!(abort["Filter"]["Prefix"], "");
    assert_eq!(
        abort["AbortIncompleteMultipartUpload"]["DaysAfterInitiation"],
        7
    );

    let prefix = backstop["Filter"]["Prefix"].as_str().unwrap();
    let session = SessionDate::from_date(chrono::NaiveDate::from_ymd_opt(2026, 10, 14).unwrap());
    let mut keys: Vec<Key> = vec![
        JournalKey::new(Host::Archiver, session).into(),
        LogsKey::new(
            Host::Archiver,
            Service::new("archive_nightly").unwrap(),
            session,
        )
        .into(),
    ];
    for provider in Provider::iter() {
        keys.push(Key::RawQuotes { provider, session });
        keys.push(Key::RawTrades { provider, session });
        keys.push(QuotesKey::new(provider, BarInterval::OneDay, session).into());
        keys.push(TradesKey::new(provider, BarInterval::OneDay, session).into());
        for table in ReferenceTable::iter() {
            keys.push(Key::RawReference(ReferenceKey::new(
                provider, table, session,
            )));
            keys.push(Key::Reference(ReferenceKey::new(provider, table, session)));
        }
    }
    // No wildcard, so a new variant fails to compile here; the set proves every variant above was built.
    let variants: std::collections::BTreeSet<&str> = keys
        .iter()
        .map(|key| match key {
            Key::Quotes(..) => "quotes",
            Key::Trades(..) => "trades",
            Key::Reference(..) => "reference",
            Key::RawQuotes { .. } => "raw_quotes",
            Key::RawTrades { .. } => "raw_trades",
            Key::RawReference(..) => "raw_reference",
            Key::Journal(..) => "journal",
            Key::Logs(..) => "logs",
        })
        .collect();
    assert_eq!(
        variants.into_iter().collect::<Vec<_>>(),
        [
            "journal",
            "logs",
            "quotes",
            "raw_quotes",
            "raw_reference",
            "raw_trades",
            "reference",
            "trades"
        ]
    );
    let deep = keys
        .iter()
        .filter(|key| key.storage_class() == StorageClass::DeepArchive)
        .count();
    assert!(deep > 0 && deep < keys.len(), "{deep} of {}", keys.len());
    for key in &keys {
        assert_eq!(
            key.path().starts_with(prefix),
            key.storage_class() == StorageClass::DeepArchive,
            "{} under {prefix}",
            key.path()
        );
    }
}

#[test]
fn test_only_the_archiver_sets_the_archive_lifecycle() {
    let provisioned = Provisioned::run_role("trader", "production", &[]);
    assert_eq!(provisioned.code, 0, "{}", provisioned.output);
    assert!(
        !provisioned
            .calls()
            .contains(&"s3api put-bucket-lifecycle-configuration".to_string()),
        "{:?}",
        provisioned.calls()
    );
}
