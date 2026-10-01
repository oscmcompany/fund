//! The Register in S3: accessions listed and read, opened under the next free number, and closed only against the
//! version that was read, so two people can neither take one number nor overwrite each other's verdict.

use crate::archive::{Archive, ArchiveError};
use crate::common::register::{
    Accession, AccessionNumber, Closing, OpenAccession, Opening, RegisterRefusal, next_number,
    successor_of,
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
        Ok(self.read(number).await?.study()?)
    }

    /// Opens `opening` under the next free number; a successor is admitted only as its predecessor allows.
    pub async fn open(&self, opening: Opening) -> Result<Accession, RegisterError> {
        if let Some(predecessor) = opening.supersedes() {
            let accessions = self.all().await?;
            let held = accessions
                .iter()
                .find(|accession| accession.number() == predecessor)
                .ok_or(RegisterError::Missing {
                    number: predecessor,
                })?;
            held.admit_successor(successor_of(&accessions, predecessor), &opening)?;
        }
        for _ in 0..OPEN_ATTEMPTS {
            let number = next_number(self.numbers().await?).ok_or(RegisterError::Exhausted)?;
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

/// Pretty-printed with a trailing newline, since people read these objects directly.
fn encode(accession: &Accession) -> Vec<u8> {
    let mut body = serde_json::to_vec_pretty(accession)
        .expect("an accession has only string keys, so it serializes");
    body.push(b'\n');
    body
}
