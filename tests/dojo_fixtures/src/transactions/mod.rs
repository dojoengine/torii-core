//! Transaction sending logic for baseline and upgrade contracts.

mod baseline;
mod upgrade;

pub use baseline::send_baseline;
pub use upgrade::send_upgrade;
