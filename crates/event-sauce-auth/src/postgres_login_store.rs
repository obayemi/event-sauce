//! The `PostgreSQL` [`LoginStore`].

use std::time::Duration;

use async_trait::async_trait;
use sqlx::PgPool;

use crate::{ttl, LoginKey, LoginStore, LoginStoreError};

/// The schema a [`PostgresLoginStore`] uses unless told otherwise.
const DEFAULT_SCHEMA: &str = "event_sauce_login";

const MAX_IDENTIFIER_LEN: usize = 63;

/// The statements creating the schema, run as one implicit transaction behind an
/// advisory lock, so every one is idempotent and instances may race. The
/// `{schema}` placeholder is replaced by the validated schema name.
const MIGRATION: &str = "
    SELECT pg_advisory_xact_lock(hashtextextended('event_sauce_login:{schema}', 0));
    CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {schema}.entries (
        key        TEXT        PRIMARY KEY,
        value      BYTEA       NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL
    );
    CREATE INDEX IF NOT EXISTS entries_expires_at ON {schema}.entries (expires_at);
";

/// The rejected schema name of [`PostgresLoginStore::with_schema`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error(
    "invalid schema name {0:?}: use at most 63 lowercase ascii letters, digits or underscores, \
     not starting with a digit"
)]
pub struct InvalidSchemaName(pub String);

fn validate_schema(name: &str) -> Result<(), InvalidSchemaName> {
    let mut chars = name.chars();
    let valid_start = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_');
    let valid_rest = chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if valid_start && valid_rest && name.len() <= MAX_IDENTIFIER_LEN {
        Ok(())
    } else {
        Err(InvalidSchemaName(name.to_owned()))
    }
}

/// A [`LoginStore`] in `PostgreSQL`, in a schema of its own.
///
/// Expiry is judged by the database clock and filtered on every read, so an
/// expired entry is never visible even before it is deleted. Expired rows
/// pile up until [`purge_expired`](Self::purge_expired) removes them, which a
/// periodic task should call.
///
/// # Examples
///
/// ```no_run
/// use event_sauce_auth::PostgresLoginStore;
///
/// # async fn run(pool: sqlx::PgPool) -> Result<(), Box<dyn std::error::Error>> {
/// let store = PostgresLoginStore::new(pool);
/// store.migrate().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PostgresLoginStore {
    pool: PgPool,
    schema: String,
}

impl PostgresLoginStore {
    /// A store in the default schema, `event_sauce_login`.
    ///
    /// Call [`migrate`](Self::migrate) before first use.
    #[must_use]
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            schema: DEFAULT_SCHEMA.to_owned(),
        }
    }

    /// Keeps the entries in `schema` instead of the default one.
    ///
    /// # Errors
    ///
    /// Fails unless `schema` is at most 63 characters of lowercase ascii
    /// letters, digits and underscores, not starting with a digit.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Result<Self, InvalidSchemaName> {
        let schema = schema.into();
        validate_schema(&schema)?;
        self.schema = schema;
        Ok(self)
    }

    /// Creates the schema and its tables, and applies the changes not yet
    /// applied. Safe to call on every start and from several instances at
    /// once.
    ///
    /// # Errors
    ///
    /// Fails if the database fails.
    pub async fn migrate(&self) -> Result<(), LoginStoreError> {
        sqlx::raw_sql(&MIGRATION.replace("{schema}", &self.schema))
            .execute(&self.pool)
            .await
            .map(drop)
            .map_err(backend("migrate"))
    }

    /// Deletes every expired entry and returns how many were deleted.
    ///
    /// # Errors
    ///
    /// Fails if the database fails.
    pub async fn purge_expired(&self) -> Result<u64, LoginStoreError> {
        sqlx::query(&format!(
            "DELETE FROM {}.entries WHERE expires_at <= now()",
            self.schema
        ))
        .execute(&self.pool)
        .await
        .map(|done| done.rows_affected())
        .map_err(backend("purge expired entries"))
    }
}

fn backend(context: &'static str) -> impl FnOnce(sqlx::Error) -> LoginStoreError {
    move |error| LoginStoreError::backend(context, error)
}

#[async_trait]
impl LoginStore for PostgresLoginStore {
    async fn put(
        &self,
        key: &LoginKey,
        value: &[u8],
        ttl: Duration,
    ) -> Result<(), LoginStoreError> {
        let millis = ttl::millis(ttl).ok_or(LoginStoreError::InvalidTtl(ttl))?;
        sqlx::query(&format!(
            "INSERT INTO {0}.entries (key, value, expires_at)
             VALUES ($1, $2, now() + $3::bigint * interval '1 millisecond')
             ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at",
            self.schema
        ))
        .bind(key.as_str())
        .bind(value)
        .bind(millis)
        .execute(&self.pool)
        .await
        .map(drop)
        .map_err(backend("put entry"))
    }

    async fn take(&self, key: &LoginKey) -> Result<Option<Vec<u8>>, LoginStoreError> {
        let taken: Option<(Vec<u8>, bool)> = sqlx::query_as(&format!(
            "DELETE FROM {}.entries WHERE key = $1 RETURNING value, expires_at > now()",
            self.schema
        ))
        .bind(key.as_str())
        .fetch_optional(&self.pool)
        .await
        .map_err(backend("take entry"))?;
        Ok(taken.and_then(|(value, live)| live.then_some(value)))
    }

    async fn mark_once(&self, key: &LoginKey, ttl: Duration) -> Result<bool, LoginStoreError> {
        let millis = ttl::millis(ttl).ok_or(LoginStoreError::InvalidTtl(ttl))?;
        sqlx::query(&format!(
            "INSERT INTO {0}.entries AS entry (key, value, expires_at)
             VALUES ($1, ''::bytea, now() + $2::bigint * interval '1 millisecond')
             ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at
             WHERE entry.expires_at <= now()",
            self.schema
        ))
        .bind(key.as_str())
        .bind(millis)
        .execute(&self.pool)
        .await
        .map(|done| done.rows_affected() == 1)
        .map_err(backend("mark entry"))
    }
}

#[cfg(test)]
mod tests;
