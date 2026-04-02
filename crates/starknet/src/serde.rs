use serde::{Deserialize, Serialize};

use crate::Felt;

impl Serialize for Felt {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_hex_str())
        } else {
            let bytes = self.as_be_bytes();
            let first = bytes.iter().position(|&b| b != 0).unwrap_or(31);
            serializer.serialize_bytes(&bytes[first..])
        }
    }
}

impl<'de> Deserialize<'de> for Felt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            deserializer.deserialize_str(FeltVisitor)
        } else {
            deserializer.deserialize_bytes(FeltVisitor)
        }
    }
}

struct FeltVisitor;

impl serde::de::Visitor<'_> for FeltVisitor {
    type Value = Felt;

    fn expecting(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "a hex string prefixed with 0x or up to 32 bytes")
    }

    fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Felt::from_hex_str(value).map_err(|e| E::custom(e.to_string()))
    }

    fn visit_bytes<E: serde::de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
        if value.len() > 32 {
            return Err(E::invalid_length(value.len(), &self));
        }
        let mut buf = [0u8; 32];
        buf[32 - value.len()..].copy_from_slice(value);
        Ok(Felt(buf))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn serialize_zero_human_readable() {
        let felt = Felt::ZERO;
        assert_eq!(serde_json::to_string(&felt).unwrap(), "\"0x0\"");
    }

    #[test]
    fn serialize_one_human_readable() {
        let felt = Felt::ONE;
        assert_eq!(serde_json::to_string(&felt).unwrap(), "\"0x1\"");
    }

    #[test]
    fn serialize_human_readable_round_trip() {
        let original = Felt::from_hex_str("0xdeadbeef").unwrap();
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: Felt = serde_json::from_str(&serialized).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn serialize_max_human_readable() {
        let max =
            Felt::from_hex_str("0x800000000000011000000000000000000000000000000000000000000000000")
                .unwrap();
        let serialized = serde_json::to_string(&max).unwrap();
        let deserialized: Felt = serde_json::from_str(&serialized).unwrap();
        assert_eq!(max, deserialized);
    }

    #[test]
    fn deserialize_without_prefix_fails() {
        assert!(serde_json::from_str::<Felt>("\"deadbeef\"").is_err());
    }

    #[test]
    fn deserialize_overflow_fails() {
        // Field modulus - should fail
        assert!(serde_json::from_str::<Felt>(
            "\"0x800000000000011000000000000000000000000000000000000000000000001\""
        )
        .is_err());
    }

    #[test]
    fn serialize_binary_round_trip() {
        let original = Felt::from_hex_str("0xdeadbeef").unwrap();
        let serialized =
            bincode::serde::encode_to_vec(&original, bincode::config::standard()).unwrap();
        let deserialized: Felt =
            bincode::serde::decode_from_slice(&serialized, bincode::config::standard())
                .unwrap()
                .0;
        assert_eq!(original, deserialized);
    }

    #[test]
    fn serialize_binary_zero() {
        let felt = Felt::ZERO;
        let serialized = bincode::serde::encode_to_vec(&felt, bincode::config::standard()).unwrap();
        let deserialized: Felt =
            bincode::serde::decode_from_slice(&serialized, bincode::config::standard())
                .unwrap()
                .0;
        assert_eq!(felt, deserialized);
    }

    #[test]
    fn serialize_binary_compact() {
        let felt = Felt::from_hex_str("0xbabe").unwrap();
        let serialized = bincode::serde::encode_to_vec(&felt, bincode::config::standard()).unwrap();
        assert!(serialized.len() < 32);
        let deserialized: Felt =
            bincode::serde::decode_from_slice(&serialized, bincode::config::standard())
                .unwrap()
                .0;
        assert_eq!(felt, deserialized);
    }
}
