//! The trading schedules in `schema.sql`, read as text so no database or `pg_cron` is needed.
//! Each gated job fires on a UTC cron expression and gates on the Eastern wall clock, and the
//! invariant checked here is that its Eastern firing times are identical on every day of the year.

use std::collections::BTreeMap;

use chrono::{Datelike, NaiveDate, NaiveTime, TimeZone, Utc};
use fund::data::calendar::eastern_datetime;

/// Compiled in, so the test cannot run against a copy that has drifted from the working tree.
const SCHEMA: &str = include_str!("../schema.sql");

/// The year walked. Any full year exercises both transitions; 2026 springs forward on 8 March and
/// falls back on 1 November, and both are Sundays, so the weekday sets on either side of each are
/// what the assertions actually compare.
const YEAR: i32 = 2026;

/// Jobs that carry no Eastern gate, and why that is correct for each.
///
/// Listed explicitly so a *new* ungated job fails rather than being quietly tolerated. Housekeeping
/// is the only justification: it deletes on an age predicate, so the hour it runs at changes
/// nothing about the outcome.
const UNGATED_JOBS: &[&str] = &["cron-run-details-cleanup"];

/// One scheduled job as it appears in the schema.
#[derive(Debug)]
struct ScheduledJob {
    name: String,
    expression: String,
    body: String,
}

/// A parsed five-field cron expression. Only the forms the schema actually uses are accepted.
#[derive(Debug)]
struct CronExpression {
    minutes: Vec<u32>,
    hours: Vec<u32>,
    days_of_week: Vec<u32>,
}

/// One `(now() AT TIME ZONE 'America/New_York')::time OP TIME 'HH:MM'` comparison.
#[derive(Debug)]
struct GateBound {
    operator: String,
    boundary: NaiveTime,
}

fn time(text: &str) -> NaiveTime {
    NaiveTime::parse_from_str(text, "%H:%M").expect("a valid HH:MM literal")
}

/// The Eastern times each gated job is intended to fire at.
///
/// Declared here rather than derived from the schema: a test that recomputed the answer from the
/// same expression it is checking would agree with any edit, including a wrong one. These are the
/// times the comments in `schema.sql` promise.
fn expected_eastern_firings(job_name: &str) -> Option<Vec<NaiveTime>> {
    let single = |value: &str| Some(vec![time(value)]);
    match job_name {
        "portfolio-liquidation-requested" => single("15:45"),
        "account-sync-requested" => single("16:15"),
        "market-data-sync-requested" => single("16:30"),
        // Every five minutes from the open through 15:40, the last pass before liquidation.
        "portfolio-evaluation-requested" => Some(
            (0..)
                .map(|step| time("09:30") + chrono::Duration::minutes(step * 5))
                .take_while(|moment| *moment <= time("15:40"))
                .collect(),
        ),
        _ => None,
    }
}

/// Reads the quoted string starting at the next `'`, returning it and the remainder.
fn take_quoted<'a>(text: &'a str, context: &str) -> (String, &'a str) {
    let start = text
        .find('\'')
        .unwrap_or_else(|| panic!("{context}: expected a quoted argument"));
    let after_open = &text[start + 1..];
    let end = after_open
        .find('\'')
        .unwrap_or_else(|| panic!("{context}: unterminated quoted argument"));
    let value = &after_open[..end];
    assert!(
        !after_open[end..].starts_with("''"),
        "{context}: doubled quotes are not handled by this parser"
    );
    (value.to_string(), &after_open[end + 1..])
}

/// Reads the `$$`-delimited body, returning it and the remainder.
fn take_dollar_quoted<'a>(text: &'a str, context: &str) -> (String, &'a str) {
    let start = text
        .find("$$")
        .unwrap_or_else(|| panic!("{context}: expected a $$-quoted command"));
    let after_open = &text[start + 2..];
    let end = after_open
        .find("$$")
        .unwrap_or_else(|| panic!("{context}: unterminated $$-quoted command"));
    (after_open[..end].to_string(), &after_open[end + 2..])
}

/// Every `cron.schedule(...)` call in the schema, in file order.
///
/// `cron.unschedule(` does not match: the literal searched for is `cron.schedule(`, and the longer
/// name breaks it at `cron.un`.
fn parse_jobs(schema: &str) -> Vec<ScheduledJob> {
    const MARKER: &str = "cron.schedule(";
    let mut jobs = Vec::new();
    let mut remainder = schema;

    while let Some(index) = remainder.find(MARKER) {
        let after_marker = &remainder[index + MARKER.len()..];
        let (name, after_name) = take_quoted(after_marker, "cron.schedule name");
        let context = format!("job '{name}'");
        let (expression, after_expression) = take_quoted(after_name, &context);
        let (body, rest) = take_dollar_quoted(after_expression, &context);
        jobs.push(ScheduledJob {
            name,
            expression,
            body,
        });
        remainder = rest;
    }

    jobs
}

/// Expands one cron field. Accepts `*`, `*/N`, `A-B`, `A-B/N`, `A,B,...`, and a bare `N`.
///
/// Panics on anything else. A field this does not understand must fail loudly: silently treating an
/// unknown form as "matches nothing" would make the job appear never to fire, and the assertions
/// below would then report a schedule problem that is really a parser problem.
fn parse_field(specification: &str, minimum: u32, maximum: u32, context: &str) -> Vec<u32> {
    let mut values = Vec::new();

    for term in specification.split(',') {
        let (range_part, step) = match term.split_once('/') {
            Some((range_part, step_text)) => {
                let step: u32 = step_text
                    .parse()
                    .unwrap_or_else(|_| panic!("{context}: unparseable step in '{term}'"));
                assert!(step > 0, "{context}: zero step in '{term}'");
                (range_part, step)
            }
            None => (term, 1),
        };

        let (first, last) = if range_part == "*" {
            (minimum, maximum)
        } else if let Some((low, high)) = range_part.split_once('-') {
            (parse_bound(low, context), parse_bound(high, context))
        } else {
            let single = parse_bound(range_part, context);
            (single, single)
        };

        assert!(
            first >= minimum && last <= maximum && first <= last,
            "{context}: '{term}' is outside {minimum}..={maximum}"
        );
        values.extend((first..=last).step_by(step as usize));
    }

    values.sort_unstable();
    values.dedup();
    assert!(!values.is_empty(), "{context}: matched no values");
    values
}

fn parse_bound(text: &str, context: &str) -> u32 {
    text.trim()
        .parse()
        .unwrap_or_else(|_| panic!("{context}: unparseable cron value '{text}'"))
}

/// Parses the five-field expression, requiring day-of-month and month to be unrestricted.
///
/// Restricting either would bring in cron's day-of-month/day-of-week OR semantics, which this
/// parser does not model — so it is refused rather than misread.
fn parse_expression(expression: &str, job_name: &str) -> CronExpression {
    let fields: Vec<&str> = expression.split_whitespace().collect();
    assert_eq!(
        fields.len(),
        5,
        "job '{job_name}': expected five cron fields in '{expression}'"
    );

    for (field, label) in [(fields[2], "day-of-month"), (fields[3], "month")] {
        assert_eq!(
            field, "*",
            "job '{job_name}': a restricted {label} field is not modelled by this test"
        );
    }

    CronExpression {
        minutes: parse_field(fields[0], 0, 59, job_name),
        hours: parse_field(fields[1], 0, 23, job_name),
        // Cron accepts both 0 and 7 for Sunday; normalized so the comparison below is a plain
        // `contains` against `num_days_from_sunday`.
        days_of_week: parse_field(fields[4], 0, 7, job_name)
            .into_iter()
            .map(|day| day % 7)
            .collect(),
    }
}

/// Every Eastern time comparison in the job body.
///
/// The connectors between them are not parsed, and [`gate_admits`] applies every bound with `AND`.
/// A gate joined with `OR` would therefore be modelled as its own intersection: PostgreSQL could
/// admit both candidate UTC firings while the once-per-session assertion still passed, which is the
/// one failure this file exists to catch. Disjunction is refused rather than misread, on the same
/// principle as [`parse_field`].
fn parse_gate(body: &str, job_name: &str) -> Vec<GateBound> {
    const MARKER: &str = "(now() AT TIME ZONE 'America/New_York')::time";

    let predicate = body
        .split_once("WHERE")
        .map_or("", |(_, predicate)| predicate);
    assert!(
        !predicate
            .split(|character: char| !character.is_ascii_alphabetic())
            .any(|token| token.eq_ignore_ascii_case("or")),
        "job '{job_name}': disjunctive gates are not modelled by this test"
    );

    let mut bounds = Vec::new();

    for segment in body.split(MARKER).skip(1) {
        let trimmed = segment.trim_start();
        let operator = ["<=", ">=", "<", ">", "="]
            .into_iter()
            .find(|candidate| trimmed.starts_with(candidate))
            .unwrap_or_else(|| {
                panic!("job '{job_name}': unrecognized comparison after the Eastern time cast")
            });

        let after_operator = trimmed[operator.len()..].trim_start();
        let literal = after_operator.strip_prefix("TIME '").unwrap_or_else(|| {
            panic!("job '{job_name}': expected a TIME literal after {operator}")
        });
        let end = literal
            .find('\'')
            .unwrap_or_else(|| panic!("job '{job_name}': unterminated TIME literal"));

        bounds.push(GateBound {
            operator: operator.to_string(),
            boundary: time(&literal[..end]),
        });
    }

    bounds
}

fn gate_admits(bounds: &[GateBound], moment: NaiveTime) -> bool {
    bounds.iter().all(|bound| match bound.operator.as_str() {
        ">=" => moment >= bound.boundary,
        ">" => moment > bound.boundary,
        "<=" => moment <= bound.boundary,
        "<" => moment < bound.boundary,
        "=" => moment == bound.boundary,
        other => panic!("unhandled comparison '{other}'"),
    })
}

/// Walks a year and collects, per Eastern date, the Eastern times the job actually fires and passes
/// its gate.
///
/// The UTC-to-Eastern conversion is the same `eastern_datetime` production uses, so this checks the
/// schedule against the conversion the application will really perform rather than a second
/// implementation that could drift from it.
fn firings_by_eastern_date(
    expression: &CronExpression,
    gate: &[GateBound],
) -> BTreeMap<NaiveDate, Vec<NaiveTime>> {
    let mut by_eastern_date: BTreeMap<NaiveDate, Vec<NaiveTime>> = BTreeMap::new();
    let mut date = NaiveDate::from_ymd_opt(YEAR, 1, 1).expect("a valid start of year");
    let end = NaiveDate::from_ymd_opt(YEAR, 12, 31).expect("a valid end of year");

    while date <= end {
        if expression
            .days_of_week
            .contains(&date.weekday().num_days_from_sunday())
        {
            for &hour in &expression.hours {
                for &minute in &expression.minutes {
                    let instant = Utc
                        .with_ymd_and_hms(date.year(), date.month(), date.day(), hour, minute, 0)
                        .single()
                        .expect("every UTC wall-clock time exists");
                    let eastern = eastern_datetime(instant);
                    if gate_admits(gate, eastern.time()) {
                        by_eastern_date
                            .entry(eastern.date())
                            .or_default()
                            .push(eastern.time());
                    }
                }
            }
        }
        date = date
            .succ_opt()
            .expect("the year ends before the epoch does");
    }

    for times in by_eastern_date.values_mut() {
        times.sort_unstable();
    }
    by_eastern_date
}

/// Every date in the year the expression's day-of-week field admits.
///
/// A schedule must produce a firing on all of them. Comparing only the days that *did* fire would
/// let a job that goes silent for half the year pass: dropping one of the two candidate UTC hours
/// leaves the surviving hour correct in one daylight-saving regime and outside the gate in the
/// other, so the missing days vanish from the map rather than disagreeing with it.
fn expected_eastern_dates(expression: &CronExpression) -> Vec<NaiveDate> {
    let mut dates = Vec::new();
    let mut date = NaiveDate::from_ymd_opt(YEAR, 1, 1).expect("a valid start of year");
    let end = NaiveDate::from_ymd_opt(YEAR, 12, 31).expect("a valid end of year");

    while date <= end {
        if expression
            .days_of_week
            .contains(&date.weekday().num_days_from_sunday())
        {
            dates.push(date);
        }
        date = date
            .succ_opt()
            .expect("the year ends before the epoch does");
    }
    dates
}

/// The whole point: a gated job fires at the same Eastern times on every trading weekday of the
/// year, and those times are the ones the schema's comments promise.
#[test]
fn test_every_gated_schedule_keeps_the_same_eastern_clock_all_year() {
    let jobs = parse_jobs(SCHEMA);
    assert_eq!(
        jobs.len(),
        5,
        "four trading jobs and the cron cleanup must all be found"
    );

    let mut checked = 0;
    for job in &jobs {
        if UNGATED_JOBS.contains(&job.name.as_str()) {
            continue;
        }

        let expected = expected_eastern_firings(&job.name).unwrap_or_else(|| {
            panic!(
                "job '{}' is neither listed as ungated nor given an expected Eastern schedule; \
                 add it to one of the two tables in this file",
                job.name
            )
        });

        let expression = parse_expression(&job.expression, &job.name);
        let gate = parse_gate(&job.body, &job.name);
        assert!(
            !gate.is_empty(),
            "job '{}' has no Eastern gate but is not listed as ungated",
            job.name
        );

        let by_eastern_date = firings_by_eastern_date(&expression, &gate);

        // Checked before the per-day comparison below, because a day with no firing at all is
        // absent from the map rather than present with the wrong times — so comparing only what is
        // there would silently accept a job that stops firing for half the year.
        assert_eq!(
            by_eastern_date.keys().copied().collect::<Vec<NaiveDate>>(),
            expected_eastern_dates(&expression),
            "job '{}' does not fire on every day its schedule admits",
            job.name
        );

        for (eastern_date, times) in &by_eastern_date {
            assert_eq!(
                times, &expected,
                "job '{}' fires at a different Eastern schedule on {eastern_date}",
                job.name
            );
        }

        checked += 1;
    }

    assert_eq!(
        checked,
        jobs.len() - UNGATED_JOBS.len(),
        "every gated job must have been checked"
    );
}

/// A gated job must fire on trading weekdays and never on a weekend.
#[test]
fn test_gated_schedules_never_fire_on_a_weekend() {
    let jobs = parse_jobs(SCHEMA);
    let mut checked = 0;
    for job in &jobs {
        if UNGATED_JOBS.contains(&job.name.as_str()) {
            continue;
        }
        let expression = parse_expression(&job.expression, &job.name);
        let gate = parse_gate(&job.body, &job.name);
        let by_eastern_date = firings_by_eastern_date(&expression, &gate);

        // Checked before the weekend filter below, which finds nothing in a map that is empty for
        // the opposite reason: a job that fires on no day at all also fires on no weekend.
        assert_eq!(
            by_eastern_date.keys().copied().collect::<Vec<NaiveDate>>(),
            expected_eastern_dates(&expression),
            "job '{}' does not fire on every day its schedule admits",
            job.name
        );

        let weekend_dates: Vec<NaiveDate> = by_eastern_date
            .into_keys()
            .filter(|eastern_date| {
                matches!(
                    eastern_date.weekday(),
                    chrono::Weekday::Sat | chrono::Weekday::Sun
                )
            })
            .collect();

        assert!(
            weekend_dates.is_empty(),
            "job '{}' fires on {weekend_dates:?}, which are not trading days",
            job.name
        );

        checked += 1;
    }

    assert_eq!(
        checked,
        jobs.len() - UNGATED_JOBS.len(),
        "every gated job must have been checked"
    );
}

/// The specific defect the gate exists to prevent: a job firing twice on the same Eastern day.
///
/// Each once-daily job is scheduled across two UTC hours so that one of them is correct in either
/// daylight-saving regime. Widen a gate past sixty minutes and both firings pass, and the job runs
/// twice — the failure the comment in `schema.sql` warns about, asserted here rather than trusted.
#[test]
fn test_a_once_daily_job_fires_exactly_once_per_session() {
    let once_daily = [
        "portfolio-liquidation-requested",
        "account-sync-requested",
        "market-data-sync-requested",
    ];

    let mut checked = 0;
    for job in parse_jobs(SCHEMA) {
        if !once_daily.contains(&job.name.as_str()) {
            continue;
        }
        let expression = parse_expression(&job.expression, &job.name);
        let gate = parse_gate(&job.body, &job.name);

        // The precondition that makes this meaningful: the expression really does fire more than
        // once a day, so passing is the gate's doing and not the expression's.
        assert!(
            expression.hours.len() * expression.minutes.len() > 1,
            "job '{}' fires once before gating, so this proves nothing",
            job.name
        );

        let by_eastern_date = firings_by_eastern_date(&expression, &gate);

        // Checked before the per-day comparison below, because a day with no firing at all is
        // absent from the map rather than present with the wrong count.
        assert_eq!(
            by_eastern_date.keys().copied().collect::<Vec<NaiveDate>>(),
            expected_eastern_dates(&expression),
            "job '{}' does not fire on every day its schedule admits",
            job.name
        );

        for (eastern_date, times) in by_eastern_date {
            assert_eq!(
                times.len(),
                1,
                "job '{}' passed its gate {} times on {eastern_date}",
                job.name,
                times.len()
            );
        }

        checked += 1;
    }

    assert_eq!(
        checked,
        once_daily.len(),
        "every once-daily job named above must have been found in the schema"
    );
}

/// The parser refuses what it does not model, rather than reading it as "never fires".
#[test]
fn test_the_parser_rejects_syntax_it_does_not_model() {
    assert!(
        std::panic::catch_unwind(|| parse_expression("0 13 1 * 1-5", "restricted-day")).is_err(),
        "a restricted day-of-month must be refused"
    );
    assert!(
        std::panic::catch_unwind(|| parse_field("13~14", 0, 23, "job")).is_err(),
        "an unrecognized field form must be refused"
    );
    assert!(
        std::panic::catch_unwind(|| parse_field("61", 0, 59, "job")).is_err(),
        "an out-of-range value must be refused"
    );
    assert!(
        std::panic::catch_unwind(|| parse_expression("0 13 * *", "too-few")).is_err(),
        "an expression with the wrong field count must be refused"
    );
}

/// The parser reads the schema's real forms correctly.
#[test]
fn test_the_parser_reads_the_forms_the_schema_uses() {
    assert_eq!(parse_field("0", 0, 59, "job"), vec![0]);
    assert_eq!(parse_field("13,14", 0, 23, "job"), vec![13, 14]);
    assert_eq!(
        parse_field("13-20", 0, 23, "job"),
        vec![13, 14, 15, 16, 17, 18, 19, 20]
    );
    assert_eq!(
        parse_field("*/5", 0, 59, "job"),
        vec![0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
    );
    // Sunday is expressible as either 0 or 7, and both must normalize to the same day.
    assert_eq!(parse_expression("0 3 * * 0", "job").days_of_week, vec![0]);
    assert_eq!(parse_expression("0 3 * * 7", "job").days_of_week, vec![0]);
}

/// Gate parsing, including that an ungated body yields no bounds rather than a spurious one.
#[test]
fn test_gate_parsing_reads_bounds_and_their_inclusivity() {
    let body = "SELECT emit_event('x', '{}'::jsonb) \
                WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '09:00' \
                  AND (now() AT TIME ZONE 'America/New_York')::time < TIME '09:20'";
    let gate = parse_gate(body, "job");
    assert_eq!(gate.len(), 2);

    assert!(!gate_admits(&gate, time("08:59")));
    assert!(
        gate_admits(&gate, time("09:00")),
        "the lower bound is inclusive"
    );
    assert!(gate_admits(&gate, time("09:19")));
    assert!(
        !gate_admits(&gate, time("09:20")),
        "the upper bound is exclusive"
    );

    assert!(parse_gate("DELETE FROM cron.job_run_details", "job").is_empty());
}

/// A disjunctive gate must be refused, not silently read as a conjunction.
///
/// `gate_admits` applies every bound with `AND`. Modelling an `OR` gate that way describes a
/// narrower window than PostgreSQL would evaluate, so both candidate UTC firings could pass in
/// production while `test_a_once_daily_job_fires_exactly_once_per_session` still saw one.
#[test]
fn test_the_gate_parser_refuses_disjunction() {
    let conjunctive = "SELECT emit_event('x', '{}'::jsonb) \
                       WHERE (now() AT TIME ZONE 'America/New_York')::time >= TIME '09:00' \
                         AND (now() AT TIME ZONE 'America/New_York')::time < TIME '09:20'";
    let disjunctive = conjunctive.replace(" AND ", " OR ");

    // The precondition: the conjunctive form parses, so the rejection below is the OR and not
    // some other difference between the two strings.
    assert_eq!(parse_gate(conjunctive, "job").len(), 2);

    assert!(
        std::panic::catch_unwind(|| parse_gate(&disjunctive, "job")).is_err(),
        "an OR-joined gate must be refused"
    );
}

/// The archiver's trigger, which lives in a provisioning script rather than in `schema.sql`.
///
/// Compiled in for the same reason as `SCHEMA`: a copy that drifted from the working tree would
/// otherwise pass.
const PROVISION_ARCHIVER: &str = include_str!("../tools/provision-archiver");

/// The `SCHEDULE_EXPRESSION` literal the provisioning script assigns.
fn archiver_expression() -> String {
    let line = PROVISION_ARCHIVER
        .lines()
        .find(|line| line.starts_with("SCHEDULE_EXPRESSION="))
        .expect("provision-archiver must assign SCHEDULE_EXPRESSION");
    line.trim_start_matches("SCHEDULE_EXPRESSION=")
        .trim_matches('"')
        .to_string()
}

/// The `(minute, hour)` an EventBridge `cron(M H * * ? *)` expression fires at.
///
/// Only the once-daily form is modelled, because that is the only form the archiver uses; anything
/// else panics rather than being read as midnight.
fn archiver_firing() -> (u32, u32) {
    let expression = archiver_expression();
    let fields: Vec<&str> = expression
        .strip_prefix("cron(")
        .and_then(|rest| rest.strip_suffix(')'))
        .unwrap_or_else(|| panic!("{expression} is not a cron(...) expression"))
        .split_whitespace()
        .collect();
    assert_eq!(fields.len(), 6, "EventBridge cron takes six fields");
    assert_eq!(
        &fields[2..],
        &["*", "*", "?", "*"],
        "only a daily every-day trigger is modelled"
    );
    (
        fields[0].parse().expect("a literal minute"),
        fields[1].parse().expect("a literal hour"),
    )
}

#[test]
fn test_the_archiver_fires_after_eastern_midnight_all_year() {
    // Pinned to the literal rather than read back from the script, so an edit to the trigger has to
    // be made here too and cannot pass by agreeing with itself.
    assert_eq!(
        archiver_expression(),
        "cron(0 7 * * ? *)",
        "the archiver's trigger changed; confirm it still clears Eastern midnight"
    );

    // The hour comes from the expression, not from a literal here. An earlier draft hardcoded 7 in
    // the loop, so the walk below agreed with any trigger the pin was edited to accept.
    let (minute, hour) = archiver_firing();

    // The job folds "the previous Eastern date". That phrase only names one session if the trigger
    // lands after Eastern midnight on the same calendar date it fires on in UTC -- at 03:00Z the
    // EDT half of the year would fire at 23:00 the day before and silently mean a different session.
    let mut date = NaiveDate::from_ymd_opt(YEAR, 1, 1).expect("1 January is a date");
    let end = NaiveDate::from_ymd_opt(YEAR, 12, 31).expect("31 December is a date");
    while date <= end {
        let instant = Utc
            .with_ymd_and_hms(date.year(), date.month(), date.day(), hour, minute, 0)
            .single()
            .expect("the trigger instant is unambiguous in UTC");
        let eastern = eastern_datetime(instant);
        assert_eq!(
            eastern.date(),
            date,
            "{hour:02}:{minute:02}Z on {date} lands on a different Eastern date"
        );
        assert!(
            eastern.time() >= time("01:00"),
            "{hour:02}:{minute:02}Z on {date} lands at {} Eastern, too close to midnight",
            eastern.time()
        );
        date = date.succ_opt().expect("the year has a next day");
    }
}
