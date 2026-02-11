/**
 * Convert hex string to Uint8Array (32-byte big-endian) for protobuf bytes fields
 */
export function hexToBytes(hex: string): Uint8Array {
  const cleanHex = hex.replace(/^0x/, "");
  const paddedHex = cleanHex.padStart(64, "0");
  const bytes = new Uint8Array(paddedHex.length / 2);
  for (let i = 0; i < bytes.length; i++) {
    bytes[i] = parseInt(paddedHex.substring(i * 2, i * 2 + 2), 16);
  }
  return bytes;
}

/**
 * Convert hex string to base64 for API requests
 */
export function hexToBase64(hex: string): string {
  const bytes = hexToBytes(hex);
  return btoa(String.fromCharCode(...bytes));
}

/**
 * Convert bytes (Uint8Array or base64 string) to hex string for display
 */
export function bytesToHex(value: Uint8Array | string | undefined): string {
  if (!value) return "0x0";
  try {
    let bytes: Uint8Array;
    if (value instanceof Uint8Array) {
      bytes = value;
    } else {
      bytes = new TextEncoder().encode(value);
    }
    let hex = Array.from(bytes)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    hex = hex.replace(/^0+/, "") || "0";
    return "0x" + hex;
  } catch {
    return String(value);
  }
}

/**
 * Convert base64 to hex string for display
 */
export function base64ToHex(b64: string): string {
  return bytesToHex(b64);
}

/**
 * Format U256 bytes to readable string
 * U256 values are base64-encoded big-endian bytes
 */
export function formatU256(bytes: string | Uint8Array | undefined): string {
  if (!bytes) return "0";

  try {
    let data: Uint8Array;
    if (typeof bytes === "string") {
      data = new TextEncoder().encode(bytes);
    } else {
      data = bytes;
    }

    let hex = Array.from(data)
      .map((b) => b.toString(16).padStart(2, "0"))
      .join("");
    hex = hex.replace(/^0+/, "") || "0";

    const value = BigInt("0x" + hex);
    return formatBigInt(value);
  } catch {
    return String(bytes);
  }
}

/**
 * Format BigInt with thousand separators
 */
export function formatBigInt(value: bigint): string {
  return value.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

/**
 * Format timestamp to human-readable string
 */
export function formatTimestamp(ts: number | string | undefined): string {
  if (!ts) return "-";
  const date = new Date(Number(ts) * 1000);
  return date.toLocaleString();
}

/**
 * Truncate address for display
 */
export function truncateAddress(address: string, chars = 6): string {
  if (!address) return "";
  if (address.length <= chars * 2 + 2) return address;
  return `${address.slice(0, chars + 2)}...${address.slice(-chars)}`;
}

/**
 * Get update type name from enum value
 */
export function getUpdateTypeName(type: number): string {
  switch (type) {
    case 0:
      return "CREATED";
    case 1:
      return "UPDATED";
    case 2:
      return "DELETED";
    default:
      return "UNKNOWN";
  }
}

/**
 * Generate a random client ID
 */
export function generateClientId(): string {
  return `client-${Math.random().toString(36).substring(7)}`;
}
