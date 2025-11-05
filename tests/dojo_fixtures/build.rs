//! Build script for Dojo fixtures.
//!
//! Compiles Cairo contracts for baseline and upgrade versions.
//!
//! Set the `DOJO_FIXTURES_BUILD_CONTRACTS` environment variable to `1` to force the build of the contracts.
//!
//! In case you need to debug, use the following command to see the build output:
//! `DOJO_FIXTURES_BUILD_CONTRACTS=1 CC_ENABLE_DEBUG_OUTPUT=1 cargo build -vv`

use anyhow::Result;
use std::{env, path::PathBuf};

#[path = "src/constants.rs"]
mod constants;
#[path = "src/sozo.rs"]
mod sozo;

use constants::{BASELINE_CAIRO_DIR, UPGRADE_CAIRO_DIR};

fn main() -> Result<()> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);

    if env::var("DOJO_FIXTURES_BUILD_CONTRACTS").unwrap_or_default() != "1" {
        return Ok(());
    }

    let baseline_dir = manifest_dir.join(BASELINE_CAIRO_DIR);
    let upgrade_dir = manifest_dir.join(UPGRADE_CAIRO_DIR);

    sozo::run(&baseline_dir, &["build"])?;
    sozo::run(&upgrade_dir, &["build"])?;

    Ok(())
}
