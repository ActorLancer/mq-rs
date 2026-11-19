pub mod inbox;
pub mod processor;
pub mod recovery;

use sqlx::{PgPool, migrate::Migrator};

/// Embedded schema migrations shared by the worker and integration tests.
pub static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> {
    MIGRATOR.run(pool).await
}
pub mod retry;
