CREATE OR REPLACE FUNCTION public.hex2int(hex_text TEXT) RETURNS BIGINT AS $$
DECLARE
    stripped TEXT;
BEGIN
    IF hex_text IS NULL THEN
        RETURN NULL;
    END IF;

    -- Strip a single leading 0x / 0X prefix (not all occurrences).
    IF left(hex_text, 2) IN ('0x', '0X') THEN
        stripped := substr(hex_text, 3);
    ELSE
        stripped := hex_text;
    END IF;

    -- Take the rightmost 16 hex chars (lower 64 bits). Hex strings may be up
    -- to 256 bits; values that fit in u64 are preserved exactly, larger values
    -- are truncated to their low 64 bits.
    IF length(stripped) > 16 THEN
        stripped := right(stripped, 16);
    END IF;

    RETURN ('x' || lpad(stripped, 16, '0'))::bit(64)::bigint;
END;
$$ LANGUAGE plpgsql IMMUTABLE STRICT;
