use xxhash_rust::xxh3::Xxh3;

pub fn truncate(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        None => s,
        Some((idx, _)) => &s[..idx],
    }
}

const ALLOWED_TYPE_NAME_CHARS: &str =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";

fn parse_type_name(type_name: &str) -> String {
    fn parse_char(c: char) -> char {
        if ALLOWED_TYPE_NAME_CHARS.contains(c) {
            c
        } else {
            '_'
        }
    }
    type_name
        .chars()
        .take(31)
        .map(parse_char)
        .collect::<String>()
        .to_lowercase()
}

pub trait HasherExt {
    fn new_based(base: &str) -> Self;
    fn type_name(&self, name: &str) -> String;
    fn branch(&self, name: &str) -> Self;
    fn branch_to_type_name(&self, leaf: &str, name: &str) -> String;
}

impl HasherExt for Xxh3 {
    fn new_based(base: &str) -> Self {
        let mut hash = Xxh3::new();
        hash.update(base.as_bytes());
        hash
    }

    fn type_name(&self, name: &str) -> String {
        let hash = &format!("{:032x}", self.digest128())[..31];
        format!("{}_{}", parse_type_name(name), hash)
    }

    fn branch(&self, name: &str) -> Xxh3 {
        let mut hasher = self.clone();
        hasher.update(&(name.len() as u32).to_le_bytes());
        hasher.update(name.as_bytes());
        hasher
    }

    fn branch_to_type_name(&self, leaf: &str, name: &str) -> String {
        self.branch(leaf).type_name(name)
    }
}
