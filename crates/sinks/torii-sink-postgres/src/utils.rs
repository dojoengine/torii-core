use rand::distr::{Alphanumeric, SampleString};
use rand::rng;

pub fn name_and_rand(base: &str, length: usize) -> String {
    format!("{}_{}", truncate(base, 31), random_alphanumeric(length))
}

pub fn random_alphanumeric(length: usize) -> String {
    Alphanumeric.sample_string(&mut rng(), length)
}

pub fn truncate(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        None => s,
        Some((idx, _)) => &s[..idx],
    }
}
