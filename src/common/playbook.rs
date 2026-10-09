//! The playbook: which strategy trades each stretch of the Eastern regular session, read from a private TOML file, and
//! the strategy that plays it, rolling from one stretch's target into the next over a declared window.

use std::collections::BTreeSet;
use std::num::NonZeroU64;

use chrono::{NaiveTime, TimeDelta};
use serde::{Deserialize, Serialize};

use crate::common::book::Book;
use crate::common::journal::RunId;
use crate::common::market::state::MarketState;
use crate::common::market::{Shares, Symbol};
use crate::common::strategy::noise::Noise;
use crate::common::strategy::{Progress, Strategy, Target, roll};
use crate::common::time::{REGULAR_CLOSE, REGULAR_OPEN, eastern_time};

/// The strategy an entry names, with the settings it is built from; the universe comes from the trader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Choice {
    Flat,
    /// Holds `shares` of each symbol its seeded coin draws.
    Noise {
        shares: Shares,
        seed: u64,
    },
}

/// A `Choice` as written, before its shares are proven to fit a `Shares`.
#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ChoiceFields {
    Flat,
    Noise { shares: NonZeroU64, seed: u64 },
}

/// One stretch of the session, from `from` until `until` Eastern, and the strategy that trades it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    from: NaiveTime,
    until: NaiveTime,
    choice: Choice,
    note: String,
    runs: Vec<RunId>,
}

impl Entry {
    pub fn from(&self) -> NaiveTime {
        self.from
    }

    pub fn until(&self) -> NaiveTime {
        self.until
    }

    pub fn choice(&self) -> Choice {
        self.choice
    }

    /// Why the entry trades what it does.
    pub fn note(&self) -> &str {
        &self.note
    }

    /// The experiments the note cites, if any.
    pub fn runs(&self) -> &[RunId] {
        &self.runs
    }
}

/// Entries covering the regular session from the open to the 16:00 close exactly once, in order, and how long a
/// switch between two of them takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Playbook {
    roll_off: TimeDelta,
    entries: Vec<Entry>,
}

/// Why a playbook was refused, with the times or values that refused it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlaybookRefusal {
    /// The text is not TOML of the playbook's shape.
    #[error("the playbook is unreadable: {cause}")]
    Unreadable { cause: String },
    /// A time is not written `HH:MM`.
    #[error("{raw:?} is not a time written HH:MM")]
    Time { raw: String },
    #[error("the playbook has no entries")]
    Empty,
    /// An entry ends at or before it starts.
    #[error("an entry from {from} ends at {until}")]
    Backward { from: NaiveTime, until: NaiveTime },
    /// No entry trades from `from` until `until`.
    #[error("no entry trades from {from} until {until}")]
    Uncovered { from: NaiveTime, until: NaiveTime },
    /// Two entries both trade at `at`.
    #[error("two entries trade at {at}")]
    Overlapping { at: NaiveTime },
    /// An entry reaches outside the regular session, at `at`.
    #[error("an entry reaches outside the session at {at}")]
    OutsideTheSession { at: NaiveTime },
    #[error("the entry from {from} has no note")]
    BlankNote { from: NaiveTime },
    /// More whole shares than a `Shares` holds.
    #[error("the entry from {from} holds {shares} shares, more than a position can")]
    TooManyShares { from: NaiveTime, shares: NonZeroU64 },
    /// An entry rolled into ends before its roll-off does, so the next switch would jump from a half-rolled target.
    #[error("the entry from {from} until {until} ends before its {} minute roll-off", .roll_off.num_minutes())]
    ShorterThanTheRollOff {
        from: NaiveTime,
        until: NaiveTime,
        roll_off: TimeDelta,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PlaybookFields {
    roll_off_minutes: u32,
    entries: Vec<EntryFields>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EntryFields {
    from: String,
    until: String,
    strategy: ChoiceFields,
    note: String,
    #[serde(default)]
    runs: Vec<RunId>,
}

impl Playbook {
    /// Reads a playbook from its TOML text, refused unless its entries cover the regular session exactly once.
    pub fn parse(text: &str) -> Result<Self, PlaybookRefusal> {
        let fields: PlaybookFields =
            toml::from_str(text).map_err(|error| PlaybookRefusal::Unreadable {
                cause: error.to_string(),
            })?;
        let mut entries = fields
            .entries
            .into_iter()
            .map(Entry::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(|entry| entry.from);
        let roll_off = TimeDelta::minutes(i64::from(fields.roll_off_minutes));
        if let Some(short) = entries
            .iter()
            .skip(1)
            .find(|entry| entry.until - entry.from < roll_off)
        {
            return Err(PlaybookRefusal::ShorterThanTheRollOff {
                from: short.from,
                until: short.until,
                roll_off,
            });
        }
        let mut covered = REGULAR_OPEN;
        for entry in &entries {
            if entry.from < REGULAR_OPEN {
                return Err(PlaybookRefusal::OutsideTheSession { at: entry.from });
            }
            if entry.from > covered {
                return Err(PlaybookRefusal::Uncovered {
                    from: covered,
                    until: entry.from,
                });
            }
            if entry.from < covered {
                return Err(PlaybookRefusal::Overlapping { at: entry.from });
            }
            covered = entry.until;
        }
        match (entries.is_empty(), covered.cmp(&REGULAR_CLOSE)) {
            (true, _) => Err(PlaybookRefusal::Empty),
            (false, std::cmp::Ordering::Less) => Err(PlaybookRefusal::Uncovered {
                from: covered,
                until: REGULAR_CLOSE,
            }),
            (false, std::cmp::Ordering::Greater) => {
                Err(PlaybookRefusal::OutsideTheSession { at: covered })
            }
            (false, std::cmp::Ordering::Equal) => Ok(Self { roll_off, entries }),
        }
    }

    pub fn roll_off(&self) -> TimeDelta {
        self.roll_off
    }

    /// In session order.
    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    /// The strategy that trades this playbook over `universe`.
    pub fn play(&self, universe: &BTreeSet<Symbol>) -> Played<Chosen> {
        Played {
            roll_off: self.roll_off,
            stretches: self
                .entries
                .iter()
                .map(|entry| {
                    let strategy = match entry.choice {
                        Choice::Flat => Chosen::Flat,
                        Choice::Noise { shares, seed } => {
                            Chosen::Noise(Noise::new(universe.clone(), shares, seed))
                        }
                    };
                    (entry.from, strategy)
                })
                .collect(),
        }
    }
}

impl TryFrom<EntryFields> for Entry {
    type Error = PlaybookRefusal;

    fn try_from(fields: EntryFields) -> Result<Self, Self::Error> {
        let (from, until) = (time(&fields.from)?, time(&fields.until)?);
        if until <= from {
            return Err(PlaybookRefusal::Backward { from, until });
        }
        if fields.note.trim().is_empty() {
            return Err(PlaybookRefusal::BlankNote { from });
        }
        let choice = match fields.strategy {
            ChoiceFields::Flat => Choice::Flat,
            ChoiceFields::Noise { shares, seed } => Choice::Noise {
                shares: Shares::whole(shares.get())
                    .map_err(|_| PlaybookRefusal::TooManyShares { from, shares })?,
                seed,
            },
        };
        Ok(Self {
            from,
            until,
            choice,
            note: fields.note,
            runs: fields.runs,
        })
    }
}

fn time(raw: &str) -> Result<NaiveTime, PlaybookRefusal> {
    NaiveTime::parse_from_str(raw, "%H:%M").map_err(|_| PlaybookRefusal::Time {
        raw: raw.to_string(),
    })
}

/// An entry's strategy, built over the trader's universe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Chosen {
    Flat,
    Noise(Noise),
}

impl Strategy for Chosen {
    fn decide(&self, state: &MarketState, book: &Book) -> Target {
        match self {
            Self::Flat => Target::default(),
            Self::Noise(noise) => noise.decide(state, book),
        }
    }
}

/// A playbook's strategies by the Eastern time each starts trading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Played<S = Chosen> {
    roll_off: TimeDelta,
    stretches: Vec<(NaiveTime, S)>,
}

/// The stretch deciding at a clock: the Eastern time it starts trading, and how far its roll-off has gone, whole for the
/// first stretch, which has nothing to roll from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stretch {
    from: NaiveTime,
    progress: Progress,
}

impl<S> Played<S> {
    /// One strategy trading the whole session, for a test that needs a played playbook of its own strategy.
    #[cfg(test)]
    pub fn throughout(strategy: S) -> Self {
        Self {
            roll_off: TimeDelta::zero(),
            stretches: vec![(REGULAR_OPEN, strategy)],
        }
    }

    /// The stretch deciding at the state's clock; before the open the first decides, after the close
    /// the last, and before any clock none does.
    pub fn stretch_at(&self, state: &MarketState) -> Option<Stretch> {
        self.indexed_stretch_at(state).map(|(_, stretch)| stretch)
    }

    fn indexed_stretch_at(&self, state: &MarketState) -> Option<(usize, Stretch)> {
        let at = eastern_time(state.clock()?);
        let index = self
            .stretches
            .iter()
            .rposition(|(from, _)| *from <= at)
            .unwrap_or(0);
        let from = self.stretches[index].0;
        let progress = match index {
            0 => Progress::WHOLE,
            1.. => Progress::of(at - from, self.roll_off),
        };
        Some((index, Stretch { from, progress }))
    }
}

impl<S: Strategy> Strategy for Played<S> {
    /// Decides with the stretch at the state's clock, rolled from the stretch before it by the stretch's progress, and
    /// holds nothing before any clock.
    fn decide(&self, state: &MarketState, book: &Book) -> Target {
        let Some((index, stretch)) = self.indexed_stretch_at(state) else {
            return Target::default();
        };
        let target = self.stretches[index].1.decide(state, book);
        match index.checked_sub(1) {
            None => target,
            Some(previous) => roll(
                &self.stretches[previous].1.decide(state, book),
                &target,
                stretch.progress,
            ),
        }
    }
}

/// The playbook a trading session read at its start, journaled whole so the session can be replayed under it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlaybookRead {
    contents: String,
}

impl PlaybookRead {
    pub fn new(contents: String) -> Self {
        Self { contents }
    }

    pub fn contents(&self) -> &str {
        &self.contents
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, TimeZone, Utc};
    use chrono_tz::America::New_York;
    use proptest::prelude::*;

    use super::*;
    use crate::common::market::state::MarketEvent;
    use crate::common::strategy::Strategy;

    const TWO_ENTRIES: &str = r#"
roll_off_minutes = 10

[[entries]]
from = "09:30"
until = "12:00"
strategy = { kind = "noise", shares = 2, seed = 7 }
note = "Exercise the session on paper"
runs = ["00000000-0000-0000-0000-000000000001"]

[[entries]]
from = "12:00"
until = "16:00"
strategy = { kind = "flat" }
note = "Hold nothing in the afternoon"
"#;

    fn time(hour: u32, minute: u32) -> NaiveTime {
        NaiveTime::from_hms_opt(hour, minute, 0).unwrap()
    }

    fn universe() -> BTreeSet<Symbol> {
        ["QQQ", "SPY", "IWM", "DIA"]
            .into_iter()
            .map(|raw| Symbol::new(raw).unwrap())
            .collect()
    }

    /// A state whose clock reads `eastern` on `date`.
    fn on(date: NaiveDate, eastern: NaiveTime) -> MarketState {
        let instant = New_York
            .from_local_datetime(&date.and_time(eastern))
            .single()
            .unwrap()
            .with_timezone(&Utc);
        MarketState::of(MarketEvent::Clock(instant))
    }

    /// A state whose clock reads `eastern` on 2026-10-08, under daylight saving time.
    fn at(eastern: NaiveTime) -> MarketState {
        on(NaiveDate::from_ymd_opt(2026, 10, 8).unwrap(), eastern)
    }

    /// Entries written as `(from, until, strategy)` with a note each, under a five-minute roll-off.
    fn written(entries: &[(&str, &str, &str)]) -> String {
        let entries: String = entries
            .iter()
            .map(|(from, until, strategy)| {
                format!("[[entries]]\nfrom = \"{from}\"\nuntil = \"{until}\"\nstrategy = {strategy}\nnote = \"n\"\n\n")
            })
            .collect();
        format!("roll_off_minutes = 5\n\n{entries}")
    }

    #[test]
    fn test_a_playbook_reads_its_entries_in_session_order() {
        let playbook = Playbook::parse(TWO_ENTRIES).unwrap();
        assert_eq!(playbook.roll_off(), TimeDelta::minutes(10));
        let entries: Vec<_> = playbook
            .entries()
            .iter()
            .map(|entry| {
                (
                    entry.from(),
                    entry.until(),
                    entry.choice(),
                    entry.note(),
                    entry
                        .runs()
                        .iter()
                        .map(RunId::to_string)
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        assert_eq!(
            entries,
            [
                (
                    time(9, 30),
                    time(12, 0),
                    Choice::Noise {
                        shares: Shares::whole(2).unwrap(),
                        seed: 7
                    },
                    "Exercise the session on paper",
                    vec!["00000000-0000-0000-0000-000000000001".to_string()]
                ),
                (
                    time(12, 0),
                    time(16, 0),
                    Choice::Flat,
                    "Hold nothing in the afternoon",
                    vec![]
                ),
            ]
        );
        let reversed = written(&[
            ("12:00", "16:00", r#"{ kind = "flat" }"#),
            ("09:30", "12:00", r#"{ kind = "flat" }"#),
        ]);
        let froms: Vec<_> = Playbook::parse(&reversed)
            .unwrap()
            .entries()
            .iter()
            .map(Entry::from)
            .collect();
        assert_eq!(froms, [time(9, 30), time(12, 0)]);
    }

    #[test]
    fn test_a_playbook_that_does_not_cover_the_session_once_is_refused() {
        let flat = r#"{ kind = "flat" }"#;
        let cases = [
            (
                "roll_off_minutes = 5\nentries = []\n".to_string(),
                PlaybookRefusal::Empty,
            ),
            (
                written(&[("09:30", "12:00", flat), ("12:05", "16:00", flat)]),
                PlaybookRefusal::Uncovered {
                    from: time(12, 0),
                    until: time(12, 5),
                },
            ),
            (
                written(&[("09:45", "16:00", flat)]),
                PlaybookRefusal::Uncovered {
                    from: time(9, 30),
                    until: time(9, 45),
                },
            ),
            (
                written(&[("09:30", "15:00", flat)]),
                PlaybookRefusal::Uncovered {
                    from: time(15, 0),
                    until: time(16, 0),
                },
            ),
            (
                written(&[("09:30", "12:00", flat), ("11:00", "16:00", flat)]),
                PlaybookRefusal::Overlapping { at: time(11, 0) },
            ),
            (
                written(&[("09:00", "16:00", flat)]),
                PlaybookRefusal::OutsideTheSession { at: time(9, 0) },
            ),
            (
                written(&[("09:30", "16:30", flat)]),
                PlaybookRefusal::OutsideTheSession { at: time(16, 30) },
            ),
            (
                written(&[("09:30", "09:30", flat)]),
                PlaybookRefusal::Backward {
                    from: time(9, 30),
                    until: time(9, 30),
                },
            ),
            (
                written(&[("9:30am", "16:00", flat)]),
                PlaybookRefusal::Time {
                    raw: "9:30am".to_string(),
                },
            ),
            (
                written(&[(
                    "09:30",
                    "16:00",
                    r#"{ kind = "noise", shares = 18446744073710, seed = 1 }"#,
                )]),
                PlaybookRefusal::TooManyShares {
                    from: time(9, 30),
                    shares: NonZeroU64::new(18_446_744_073_710).unwrap(),
                },
            ),
            (
                written(&[
                    ("09:30", "09:32", flat),
                    ("09:32", "09:36", flat),
                    ("09:36", "16:00", flat),
                ]),
                PlaybookRefusal::ShorterThanTheRollOff {
                    from: time(9, 32),
                    until: time(9, 36),
                    roll_off: TimeDelta::minutes(5),
                },
            ),
            (
                TWO_ENTRIES.replace("Hold nothing in the afternoon", " "),
                PlaybookRefusal::BlankNote { from: time(12, 0) },
            ),
        ];
        for (text, refusal) in cases {
            assert_eq!(Playbook::parse(&text), Err(refusal), "{text}");
        }
        for unreadable in [
            TWO_ENTRIES.replace("kind = \"flat\"", "kind = \"momentum\""),
            TWO_ENTRIES.replace("shares = 2", "shares = 0"),
            TWO_ENTRIES.replace("roll_off_minutes", "rolloff_minutes"),
            TWO_ENTRIES.replace("seed = 7", "seed = 7, symbols = 3"),
            TWO_ENTRIES.replace(
                "runs = [\"00000000-0000-0000-0000-000000000001\"]",
                "runs = [\"run one\"]",
            ),
        ] {
            assert!(
                matches!(
                    Playbook::parse(&unreadable),
                    Err(PlaybookRefusal::Unreadable { .. })
                ),
                "{unreadable}"
            );
        }
        let Err(PlaybookRefusal::Unreadable { cause }) =
            Playbook::parse(&TWO_ENTRIES.replace("kind = \"flat\"", "kind = \"momentum\""))
        else {
            panic!("an unknown strategy kind parsed");
        };
        assert!(cause.contains("line 14, column 21"), "{cause}");
    }

    /// The morning's noise decides alone, then rolls into the afternoon's flat target over ten minutes, keeping a tenth
    /// less of each holding a minute, and none from ten minutes on.
    #[test]
    fn test_the_played_playbook_decides_with_the_entry_at_its_clock_and_rolls_between() {
        let played = Playbook::parse(TWO_ENTRIES).unwrap().play(&universe());
        let noise = Noise::new(universe(), Shares::whole(2).unwrap(), 7);
        let book = Book::default();
        for morning in [time(9, 30), time(11, 59), time(8, 0)] {
            let state = at(morning);
            let target = played.decide(&state, &book);
            assert_eq!(target, noise.decide(&state, &book), "{morning}");
            assert!(
                !target.holdings().is_empty(),
                "{morning} draws nothing, so this case proves nothing"
            );
        }
        let mut drew = 0;
        for (minute, units) in [
            (1, 1_800_000),
            (2, 1_600_000),
            (3, 1_400_000),
            (4, 1_200_000),
            (5, 1_000_000),
            (6, 800_000),
            (7, 600_000),
            (8, 400_000),
            (9, 200_000),
        ] {
            let state = at(time(12, minute));
            let rolled: Vec<_> = played
                .decide(&state, &book)
                .holdings()
                .iter()
                .map(|(symbol, shares)| (symbol.clone(), shares.units()))
                .collect();
            let expected: Vec<_> = noise
                .decide(&state, &book)
                .holdings()
                .keys()
                .map(|symbol| (symbol.clone(), units))
                .collect();
            drew += expected.len();
            assert_eq!(rolled, expected, "12:0{minute}");
        }
        assert!(drew > 0, "the roll-off drew nothing, so it proves nothing");
        let winter = on(NaiveDate::from_ymd_opt(2026, 1, 15).unwrap(), time(11, 30));
        assert_eq!(
            played.decide(&winter, &book),
            noise.decide(&winter, &book),
            "11:30 Eastern in January is still the morning"
        );
        assert!(!played.decide(&winter, &book).holdings().is_empty());
        for afternoon in [time(12, 10), time(15, 59), time(17, 0)] {
            assert_eq!(
                played.decide(&at(afternoon), &book),
                Target::default(),
                "{afternoon}"
            );
        }
    }

    /// The stretch at a clock is the entry trading then and its share of the roll-off: whole in the first entry and
    /// from ten minutes on, and none before any clock.
    #[test]
    fn test_the_stretch_at_a_clock_names_its_start_and_its_roll() {
        let played = Playbook::parse(TWO_ENTRIES).unwrap().play(&universe());
        let stretches: Vec<Stretch> = [
            time(8, 0),
            time(11, 59),
            time(12, 0),
            time(12, 5),
            time(12, 10),
            time(17, 0),
        ]
        .into_iter()
        .map(|eastern| played.stretch_at(&at(eastern)).unwrap())
        .collect();
        let stretch = |from, parts: u32| Stretch {
            from,
            progress: Progress::new(parts).unwrap(),
        };
        assert_eq!(
            stretches,
            [
                stretch(time(9, 30), 1_000_000),
                stretch(time(9, 30), 1_000_000),
                stretch(time(12, 0), 0),
                stretch(time(12, 0), 500_000),
                stretch(time(12, 0), 1_000_000),
                stretch(time(12, 0), 1_000_000),
            ]
        );
        assert_eq!(played.stretch_at(&MarketState::default()), None);
    }

    /// The reverse switch, flat into noise, rolls up into the noise's holdings: none at 12:00, a tenth more a minute,
    /// and the whole from ten minutes on.
    #[test]
    fn test_a_switch_into_a_strategy_that_holds_rolls_up_to_it() {
        let reversed = r#"
roll_off_minutes = 10

[[entries]]
from = "09:30"
until = "12:00"
strategy = { kind = "flat" }
note = "Hold nothing in the morning"

[[entries]]
from = "12:00"
until = "16:00"
strategy = { kind = "noise", shares = 2, seed = 7 }
note = "Exercise the afternoon on paper"
"#;
        let played = Playbook::parse(reversed).unwrap().play(&universe());
        let noise = Noise::new(universe(), Shares::whole(2).unwrap(), 7);
        let book = Book::default();
        assert_eq!(played.decide(&at(time(11, 0)), &book), Target::default());
        let mut drew = 0;
        for (minute, units) in [
            (0, 0),
            (1, 200_000),
            (4, 800_000),
            (9, 1_800_000),
            (10, 2_000_000),
            (30, 2_000_000),
        ] {
            let state = at(time(12, 0) + TimeDelta::minutes(minute));
            let rolled: Vec<_> = played
                .decide(&state, &book)
                .holdings()
                .iter()
                .map(|(symbol, shares)| (symbol.clone(), shares.units()))
                .collect();
            let expected: Vec<_> = noise
                .decide(&state, &book)
                .holdings()
                .keys()
                .filter(|_| units > 0)
                .map(|symbol| (symbol.clone(), units))
                .collect();
            drew += expected.len();
            assert_eq!(rolled, expected, "{minute} minutes in");
        }
        assert!(drew > 0, "the roll-up drew nothing, so it proves nothing");
    }

    proptest! {
        /// Any cut of the session into stretches no shorter than the roll-off reads back as those stretches, in order.
        #[test]
        fn property_any_cut_of_the_session_reads_back(
            cuts in prop::collection::btree_set(1..78u32, 0..6),
        ) {
            let bounds: Vec<NaiveTime> = std::iter::once(0)
                .chain(cuts)
                .chain(std::iter::once(78))
                .map(|fives| REGULAR_OPEN + TimeDelta::minutes(5 * i64::from(fives)))
                .collect();
            let formatted: Vec<String> = bounds.iter().map(|time| time.format("%H:%M").to_string()).collect();
            let entries: Vec<(&str, &str, &str)> = formatted
                .windows(2)
                .map(|pair| (pair[0].as_str(), pair[1].as_str(), r#"{ kind = "flat" }"#))
                .collect();
            let playbook = Playbook::parse(&written(&entries)).unwrap();
            let read: Vec<(NaiveTime, NaiveTime)> =
                playbook.entries().iter().map(|entry| (entry.from(), entry.until())).collect();
            let expected: Vec<(NaiveTime, NaiveTime)> = bounds.windows(2).map(|pair| (pair[0], pair[1])).collect();
            prop_assert_eq!(read, expected);
        }
    }
}
