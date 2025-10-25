-- Initialize all custom domains for Starknet/Cairo types
-- Using DO blocks for conditional creation since IF NOT EXISTS isn't supported in older PostgreSQL versions


DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint8') THEN
        CREATE DOMAIN uint8 AS SmallInt
        CHECK (VALUE >= 0 AND VALUE < 256);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint16') THEN
        CREATE DOMAIN uint16 AS Int
        CHECK (VALUE >= 0 AND VALUE < 65536);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint32') THEN
        CREATE DOMAIN uint32 AS BigInt
        CHECK (VALUE >= 0 AND VALUE < 4294967296);
    END IF;
END $$;

-- uint64: Unsigned 64-bit integer (0 to 2^64-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint64') THEN
        CREATE DOMAIN uint64 AS NUMERIC(20, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 64));
    END IF;
END $$;

-- uint128: Unsigned 128-bit integer (0 to 2^128-1)  
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint128') THEN
        CREATE DOMAIN uint128 AS NUMERIC(39, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 128));
    END IF;
END $$;

-- int128: Signed 128-bit integer (-2^127 to 2^127-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'int128') THEN
        CREATE DOMAIN int128 AS NUMERIC(39, 0)
        CHECK (VALUE >= -power(2::numeric, 127) AND VALUE < power(2::numeric, 127));
    END IF;
END $$;

-- uint256: Unsigned 256-bit integer (0 to 2^256-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'uint256') THEN
        CREATE DOMAIN uint256 AS NUMERIC(78, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 256));
    END IF;
END $$;

-- felt252: Cairo field element (0 to PRIME-1)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'felt252') THEN
        CREATE DOMAIN felt252 AS NUMERIC(76, 0)
        CHECK (VALUE >= 0 AND VALUE < 3618502788666131213697322783095070105623107215331596699973092056135872020481);
    END IF;
END $$;

-- starknet_hash: Starknet addresses and class hashes (uint251)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'starknet_hash') THEN
        CREATE DOMAIN starknet_hash AS NUMERIC(76, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 251));
    END IF;
END $$;

-- eth_address: Ethereum address (160-bit / 20 bytes)
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'eth_address') THEN
        CREATE DOMAIN eth_address AS NUMERIC(49, 0)
        CHECK (VALUE >= 0 AND VALUE < power(2::numeric, 160));
    END IF;
END $$;
