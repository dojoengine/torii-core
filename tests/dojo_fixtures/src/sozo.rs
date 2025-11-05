use anyhow::{anyhow, Context, Result};
use std::{path::PathBuf, process::Command};

pub fn run(project_dir: &PathBuf, args: &[&str]) -> Result<()> {
    let mut command = Command::new("sozo");
    command.current_dir(project_dir).args(args);
    let status = command
        .status()
        .with_context(|| format!("failed to run sozo {:?}", args))?;
    if !status.success() {
        return Err(anyhow!(
            "sozo command {:?} failed in {}",
            args,
            project_dir.display()
        ));
    }
    Ok(())
}
