//! Each profile's records bucket is named from the profile itself, hyphens only, so no profile writes another's.

/// Every `[profiles.<name>]` section with its records bucket default, if it has one.
fn records_buckets(manifest: &str) -> Vec<(String, Option<String>)> {
    let mut sections: Vec<(String, Option<String>)> = Vec::new();
    for line in manifest.lines() {
        if let Some(profile) = line
            .strip_prefix("[profiles.")
            .and_then(|rest| rest.strip_suffix(']'))
        {
            sections.push((profile.trim_matches('"').to_string(), None));
        } else if line.starts_with("AWS_S3_RECORDS_BUCKET_NAME") {
            let (_, bucket) = sections.last_mut().expect("a key sits inside a profile");
            *bucket = line
                .split("default = \"")
                .nth(1)
                .and_then(|rest| rest.split('"').next())
                .map(str::to_string);
        }
    }
    sections
}

#[test]
fn test_each_profile_names_its_own_records_bucket() {
    let manifest = std::fs::read_to_string("secretspec.toml").unwrap();
    let buckets = records_buckets(&manifest);
    assert!(!buckets.is_empty(), "no profiles read");
    for (profile, bucket) in buckets {
        let expected = match profile.as_str() {
            // Credential-less, so it has no bucket of its own to write.
            "development" => None,
            _ => Some(format!("oscm-fund-{}", profile.replace(['/', '.'], "-"))),
        };
        assert_eq!(bucket, expected, "{profile}");
    }
}

#[test]
fn test_the_named_profiles_read_their_literal_buckets() {
    let manifest = std::fs::read_to_string("secretspec.toml").unwrap();
    let buckets = records_buckets(&manifest);
    let find = |profile: &str| {
        buckets
            .iter()
            .find(|(name, _)| name == profile)
            .and_then(|(_, bucket)| bucket.clone())
    };
    assert_eq!(find("production").as_deref(), Some("oscm-fund-production"));
    assert_eq!(
        find("development/john.forstmeier").as_deref(),
        Some("oscm-fund-development-john-forstmeier")
    );
}
