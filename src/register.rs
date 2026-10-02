//! The Register in S3: accessions listed and read, opened under the next free number, and closed only against the
//! version that was read, so two people can neither take one number nor overwrite each other's verdict.

use crate::archive::{Archive, ArchiveError};
use crate::common::register::{
    Accession, AccessionNumber, Closing, OpenAccession, Opening, RegisterRefusal, next_number,
};
use crate::common::storage::Key;

/// Attempts at taking the next number before giving up to whoever keeps winning it.
const OPEN_ATTEMPTS: u32 = 5;

pub struct Register {
    archive: Archive,
}

#[derive(Debug)]
pub enum RegisterError {
    Archive(ArchiveError),
    /// Something under the prefix that is not an accession; the Register refuses to read around it.
    Unrecognized {
        path: String,
    },
    Unreadable {
        number: AccessionNumber,
        reason: String,
    },
    Missing {
        number: AccessionNumber,
    },
    Refused(RegisterRefusal),
    /// Every attempt lost the next number to another writer.
    Contended {
        attempts: u32,
    },
    Exhausted,
}

impl std::fmt::Display for RegisterError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Archive(error) => write!(formatter, "{error}"),
            Self::Unrecognized { path } => write!(formatter, "{path} is not an accession"),
            Self::Unreadable { number, reason } => {
                write!(formatter, "accession {number} does not read: {reason}")
            }
            Self::Missing { number } => write!(formatter, "accession {number} does not exist"),
            Self::Refused(refusal) => write!(formatter, "{refusal}"),
            Self::Contended { attempts } => {
                write!(
                    formatter,
                    "another writer took the next number {attempts} times; try again"
                )
            }
            Self::Exhausted => write!(formatter, "every accession number is taken"),
        }
    }
}

impl From<ArchiveError> for RegisterError {
    fn from(error: ArchiveError) -> Self {
        Self::Archive(error)
    }
}

impl From<RegisterRefusal> for RegisterError {
    fn from(refusal: RegisterRefusal) -> Self {
        Self::Refused(refusal)
    }
}

impl Register {
    pub fn new(archive: Archive) -> Self {
        Self { archive }
    }

    /// Every accession number held, in order.
    pub async fn numbers(&self) -> Result<Vec<AccessionNumber>, RegisterError> {
        let prefix = Key::Register {
            number: AccessionNumber::FIRST,
        }
        .series();
        let mut numbers = self
            .archive
            .list(&prefix)
            .await?
            .into_iter()
            .map(|path| match Key::parse(&path) {
                Ok(Key::Register { number }) => Ok(number),
                Ok(_) | Err(_) => Err(RegisterError::Unrecognized { path }),
            })
            .collect::<Result<Vec<_>, _>>()?;
        numbers.sort();
        Ok(numbers)
    }

    pub async fn read(&self, number: AccessionNumber) -> Result<Accession, RegisterError> {
        let (body, _) = self.read_tagged(number).await?;
        Ok(body)
    }

    pub async fn all(&self) -> Result<Vec<Accession>, RegisterError> {
        let mut accessions = Vec::new();
        for number in self.numbers().await? {
            accessions.push(self.read(number).await?);
        }
        Ok(accessions)
    }

    /// The proof a study needs, refused unless the accession it names is open now.
    pub async fn study(&self, number: AccessionNumber) -> Result<OpenAccession, RegisterError> {
        let accessions = self.all().await?;
        let accession = accessions
            .iter()
            .find(|accession| accession.number() == number)
            .ok_or(RegisterError::Missing { number })?;
        Ok(accession.study(&accessions)?)
    }

    /// Opens `opening` under the next free number; a successor is admitted only as its predecessor allows.
    pub async fn open(&self, opening: Opening) -> Result<Accession, RegisterError> {
        for _ in 0..OPEN_ATTEMPTS {
            let accessions = self.all().await?;
            if let Some(predecessor) = opening.supersedes() {
                admit(&accessions, predecessor, &opening)?;
            }
            let number = next_number(accessions.iter().map(Accession::number))
                .ok_or(RegisterError::Exhausted)?;
            let accession = Accession::open(number, opening.clone());
            match self
                .archive
                .create(&Key::Register { number }, encode(&accession))
                .await
            {
                Ok(()) => return Ok(accession),
                Err(ArchiveError::Contended { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(RegisterError::Contended {
            attempts: OPEN_ATTEMPTS,
        })
    }

    /// Records the verdict, written only over the version read, so a concurrent close is never overwritten.
    pub async fn close(
        &self,
        number: AccessionNumber,
        closing: Closing,
    ) -> Result<Accession, RegisterError> {
        let (accession, tag) = self.read_tagged(number).await?;
        let closed = accession.close(closing)?;
        self.archive
            .replace(&Key::Register { number }, encode(&closed), &tag)
            .await?;
        Ok(closed)
    }

    async fn read_tagged(
        &self,
        number: AccessionNumber,
    ) -> Result<(Accession, crate::archive::Tag), RegisterError> {
        let (body, tag) = self
            .archive
            .get_tagged(&Key::Register { number })
            .await?
            .ok_or(RegisterError::Missing { number })?;
        let accession: Accession =
            serde_json::from_slice(&body).map_err(|error| RegisterError::Unreadable {
                number,
                reason: error.to_string(),
            })?;
        if accession.number() != number {
            return Err(RegisterError::Unreadable {
                number,
                reason: format!("it names itself {}", accession.number()),
            });
        }
        Ok((accession, tag))
    }
}

fn admit(
    accessions: &[Accession],
    predecessor: AccessionNumber,
    successor: &Opening,
) -> Result<(), RegisterError> {
    let held = accessions
        .iter()
        .find(|accession| accession.number() == predecessor)
        .ok_or(RegisterError::Missing {
            number: predecessor,
        })?;
    Ok(held.admit_successor(successor)?)
}

/// Pretty-printed with a trailing newline, since people read these objects directly.
fn encode(accession: &Accession) -> Vec<u8> {
    let mut body = serde_json::to_vec_pretty(accession)
        .expect("an accession has only string keys, so it serializes");
    body.push(b'\n');
    body
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use super::*;
    use crate::common::register::{
        Bid, Closing, Family, Horizon, Measured, RegisterRefusal, Sample, StudyCost, Universe,
        Verdict,
    };
    use crate::common::time::SessionDate;

    fn opening(supersedes: Option<AccessionNumber>) -> Opening {
        Opening::new(
            Family::Baselines,
            Universe::Legacy("the register's own seam".to_string()),
            Horizon::Described("none".to_string()),
            "the Register's S3 seam behaves as its rules say".to_string(),
            Bid::Unrecorded,
            SessionDate::at(Utc::now()),
            supersedes,
            None,
        )
        .unwrap()
    }

    fn refute() -> Closing {
        Closing::new(
            Verdict::Refute,
            "a live check".to_string(),
            Measured::NotMeasured,
            Sample::Counted {
                count: 0,
                unit: "checks".parse().unwrap(),
            },
            Vec::new(),
            SessionDate::at(Utc::now()),
            None,
            StudyCost::default(),
        )
        .unwrap()
    }

    /// Opening, closing, a create over a taken number, and successors, against real conditional writes.
    #[tokio::test]
    #[ignore = "writes accessions to the bucket AWS_S3_REGISTER_BUCKET_NAME names, which must be a development bucket; \
                clear its records/register/ objects and versions afterwards"]
    async fn live_the_register_seam_keeps_its_rules() {
        let bucket = std::env::var("AWS_S3_REGISTER_BUCKET_NAME").unwrap();
        assert!(
            bucket.contains("development"),
            "refusing to write test accessions into {bucket}"
        );
        let configuration = aws_config::load_from_env().await;
        let register = Register::new(Archive::register(&configuration).unwrap());

        let first = register.open(opening(None)).await.unwrap();
        register.close(first.number(), refute()).await.unwrap();
        assert!(matches!(
            register.close(first.number(), refute()).await,
            Err(RegisterError::Refused(
                RegisterRefusal::AlreadyClosed { .. }
            ))
        ));
        assert!(matches!(
            register
                .archive
                .create(
                    &Key::Register {
                        number: first.number()
                    },
                    encode(&first)
                )
                .await,
            Err(ArchiveError::Contended { .. })
        ));

        // Two re-tests of one accession are two tests; a re-test of one still open is refused.
        let successor = register.open(opening(Some(first.number()))).await.unwrap();
        let second = register.open(opening(Some(first.number()))).await.unwrap();
        assert_eq!(
            (
                successor.opening().supersedes(),
                second.opening().supersedes()
            ),
            (Some(first.number()), Some(first.number()))
        );
        assert!(matches!(
            register.open(opening(Some(second.number()))).await,
            Err(RegisterError::Refused(RegisterRefusal::StillOpen { .. }))
        ));
    }
}
