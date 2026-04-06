use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sqlx::{any::AnyPoolOptions, Any, Pool, Row};
use starknet::core::types::{Felt, U256};
use torii_common::{blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VotingPowerSource {
    DelegateVotesChanged,
    RpcGetVotes,
    Erc20Balance,
}

impl VotingPowerSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::DelegateVotesChanged => "delegate_votes_changed",
            Self::RpcGetVotes => "rpc_get_votes",
            Self::Erc20Balance => "erc20_balance",
        }
    }

    pub fn priority(self) -> i32 {
        match self {
            Self::DelegateVotesChanged => 3,
            Self::RpcGetVotes => 2,
            Self::Erc20Balance => 1,
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "delegate_votes_changed" => Self::DelegateVotesChanged,
            "rpc_get_votes" => Self::RpcGetVotes,
            _ => Self::Erc20Balance,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GovernorRecord {
    pub address: Felt,
    pub votes_token: Option<Felt>,
    pub name: Option<String>,
    pub version: Option<String>,
    pub src5_supported: bool,
    pub governor_interface_supported: bool,
}

#[derive(Debug, Clone)]
pub struct ProposalRecord {
    pub id: i64,
    pub governor: Felt,
    pub proposal_id: Felt,
    pub proposer: Option<Felt>,
    pub description: Option<String>,
    pub snapshot: u64,
    pub deadline: u64,
    pub status: String,
    pub created_block: u64,
    pub created_tx_hash: Felt,
    pub created_timestamp: Option<i64>,
    pub queued_block: Option<u64>,
    pub executed_block: Option<u64>,
    pub canceled_block: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct VoteRecord {
    pub id: i64,
    pub governor: Felt,
    pub proposal_id: Felt,
    pub voter: Felt,
    pub support: u8,
    pub weight: U256,
    pub reason: Option<String>,
    pub params: Vec<u8>,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct DelegationRecord {
    pub id: i64,
    pub votes_token: Felt,
    pub delegator: Felt,
    pub from_delegate: Felt,
    pub to_delegate: Felt,
    pub block_number: u64,
    pub tx_hash: Felt,
    pub timestamp: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct VotingPowerRecord {
    pub votes_token: Felt,
    pub account: Felt,
    pub delegate: Option<Felt>,
    pub voting_power: U256,
    pub source: VotingPowerSource,
    pub last_block: u64,
    pub last_tx_hash: Felt,
}

#[derive(Debug, Clone, Copy)]
pub struct ProposalCursor {
    pub id: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct VoteCursor {
    pub id: i64,
}

#[derive(Debug, Clone, Copy)]
pub struct DelegationCursor {
    pub id: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Backend {
    Sqlite,
    Postgres,
}

pub struct GovernanceStorage {
    pool: Pool<Any>,
    backend: Backend,
}

impl GovernanceStorage {
    pub async fn new(url: &str) -> Result<Self> {
        sqlx::any::install_default_drivers();

        let backend = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Backend::Postgres
        } else {
            Backend::Sqlite
        };

        let database_url = if backend == Backend::Sqlite && !url.starts_with("sqlite:") {
            let path = std::path::Path::new(url);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    tokio::fs::create_dir_all(parent).await?;
                }
            }
            format!("sqlite:{}", path.display())
        } else {
            url.to_string()
        };

        let pool = AnyPoolOptions::new()
            .max_connections(if backend == Backend::Sqlite { 1 } else { 5 })
            .connect(&database_url)
            .await?;

        let storage = Self { pool, backend };
        storage.init_schema().await?;
        Ok(storage)
    }

    fn table(&self, name: &str) -> String {
        match self.backend {
            Backend::Sqlite => name.to_string(),
            Backend::Postgres => format!("governance.{name}"),
        }
    }

    async fn init_schema(&self) -> Result<()> {
        if self.backend == Backend::Sqlite {
            sqlx::query("PRAGMA journal_mode=WAL")
                .execute(&self.pool)
                .await?;
            sqlx::query("PRAGMA synchronous=NORMAL")
                .execute(&self.pool)
                .await?;
        } else {
            sqlx::query("CREATE SCHEMA IF NOT EXISTS governance")
                .execute(&self.pool)
                .await?;
        }

        let statements = match self.backend {
            Backend::Sqlite => vec![
                "CREATE TABLE IF NOT EXISTS governors (address BLOB PRIMARY KEY, votes_token BLOB, name TEXT, version TEXT, src5_supported INTEGER NOT NULL, governor_interface_supported INTEGER NOT NULL)",
                "CREATE TABLE IF NOT EXISTS proposals (id INTEGER PRIMARY KEY AUTOINCREMENT, governor BLOB NOT NULL, proposal_id BLOB NOT NULL, proposer BLOB, description TEXT, snapshot INTEGER NOT NULL DEFAULT 0, deadline INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL, created_block INTEGER NOT NULL, created_tx_hash BLOB NOT NULL, created_timestamp INTEGER, queued_block INTEGER, executed_block INTEGER, canceled_block INTEGER, UNIQUE(governor, proposal_id))",
                "CREATE INDEX IF NOT EXISTS proposals_governor_idx ON proposals (governor, id DESC)",
                "CREATE TABLE IF NOT EXISTS votes (id INTEGER PRIMARY KEY AUTOINCREMENT, governor BLOB NOT NULL, proposal_id BLOB NOT NULL, voter BLOB NOT NULL, support INTEGER NOT NULL, weight BLOB NOT NULL, reason TEXT, params BLOB NOT NULL, block_number INTEGER NOT NULL, tx_hash BLOB NOT NULL, timestamp INTEGER)",
                "CREATE INDEX IF NOT EXISTS votes_query_idx ON votes (governor, proposal_id, voter, id DESC)",
                "CREATE TABLE IF NOT EXISTS delegations (id INTEGER PRIMARY KEY AUTOINCREMENT, votes_token BLOB NOT NULL, delegator BLOB NOT NULL, from_delegate BLOB NOT NULL, to_delegate BLOB NOT NULL, block_number INTEGER NOT NULL, tx_hash BLOB NOT NULL, timestamp INTEGER)",
                "CREATE INDEX IF NOT EXISTS delegations_query_idx ON delegations (votes_token, delegator, to_delegate, id DESC)",
                "CREATE TABLE IF NOT EXISTS voting_power (votes_token BLOB NOT NULL, account BLOB NOT NULL, delegate BLOB, voting_power BLOB NOT NULL, source TEXT NOT NULL, source_priority INTEGER NOT NULL, last_block INTEGER NOT NULL, last_tx_hash BLOB NOT NULL, PRIMARY KEY(votes_token, account))",
            ],
            Backend::Postgres => vec![
                "CREATE TABLE IF NOT EXISTS governance.governors (address BYTEA PRIMARY KEY, votes_token BYTEA, name TEXT, version TEXT, src5_supported BOOLEAN NOT NULL, governor_interface_supported BOOLEAN NOT NULL)",
                "CREATE TABLE IF NOT EXISTS governance.proposals (id BIGSERIAL PRIMARY KEY, governor BYTEA NOT NULL, proposal_id BYTEA NOT NULL, proposer BYTEA, description TEXT, snapshot BIGINT NOT NULL DEFAULT 0, deadline BIGINT NOT NULL DEFAULT 0, status TEXT NOT NULL, created_block BIGINT NOT NULL, created_tx_hash BYTEA NOT NULL, created_timestamp BIGINT, queued_block BIGINT, executed_block BIGINT, canceled_block BIGINT, UNIQUE(governor, proposal_id))",
                "CREATE INDEX IF NOT EXISTS proposals_governor_idx ON governance.proposals (governor, id DESC)",
                "CREATE TABLE IF NOT EXISTS governance.votes (id BIGSERIAL PRIMARY KEY, governor BYTEA NOT NULL, proposal_id BYTEA NOT NULL, voter BYTEA NOT NULL, support INTEGER NOT NULL, weight BYTEA NOT NULL, reason TEXT, params BYTEA NOT NULL, block_number BIGINT NOT NULL, tx_hash BYTEA NOT NULL, timestamp BIGINT)",
                "CREATE INDEX IF NOT EXISTS votes_query_idx ON governance.votes (governor, proposal_id, voter, id DESC)",
                "CREATE TABLE IF NOT EXISTS governance.delegations (id BIGSERIAL PRIMARY KEY, votes_token BYTEA NOT NULL, delegator BYTEA NOT NULL, from_delegate BYTEA NOT NULL, to_delegate BYTEA NOT NULL, block_number BIGINT NOT NULL, tx_hash BYTEA NOT NULL, timestamp BIGINT)",
                "CREATE INDEX IF NOT EXISTS delegations_query_idx ON governance.delegations (votes_token, delegator, to_delegate, id DESC)",
                "CREATE TABLE IF NOT EXISTS governance.voting_power (votes_token BYTEA NOT NULL, account BYTEA NOT NULL, delegate BYTEA, voting_power BYTEA NOT NULL, source TEXT NOT NULL, source_priority INTEGER NOT NULL, last_block BIGINT NOT NULL, last_tx_hash BYTEA NOT NULL, PRIMARY KEY(votes_token, account))",
            ],
        };

        for statement in statements {
            sqlx::query(statement).execute(&self.pool).await?;
        }

        Ok(())
    }

    pub async fn upsert_governor(&self, record: &GovernorRecord) -> Result<()> {
        let table = self.table("governors");
        let sql = match self.backend {
            Backend::Sqlite => format!(
                "INSERT INTO {table} (address, votes_token, name, version, src5_supported, governor_interface_supported) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(address) DO UPDATE SET votes_token = COALESCE(excluded.votes_token, {table}.votes_token), name = COALESCE(excluded.name, {table}.name), version = COALESCE(excluded.version, {table}.version), src5_supported = excluded.src5_supported, governor_interface_supported = excluded.governor_interface_supported"
            ),
            Backend::Postgres => format!(
                "INSERT INTO {table} (address, votes_token, name, version, src5_supported, governor_interface_supported) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT(address) DO UPDATE SET votes_token = COALESCE(EXCLUDED.votes_token, {table}.votes_token), name = COALESCE(EXCLUDED.name, {table}.name), version = COALESCE(EXCLUDED.version, {table}.version), src5_supported = EXCLUDED.src5_supported, governor_interface_supported = EXCLUDED.governor_interface_supported"
            ),
        };

        sqlx::query(&sql)
            .bind(felt_to_blob(record.address))
            .bind(record.votes_token.map(felt_to_blob))
            .bind(record.name.clone())
            .bind(record.version.clone())
            .bind(record.src5_supported)
            .bind(record.governor_interface_supported)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_governor(&self, governor: Felt) -> Result<Option<GovernorRecord>> {
        let table = self.table("governors");
        let sql = if self.backend == Backend::Sqlite {
            format!("SELECT address, votes_token, name, version, src5_supported, governor_interface_supported FROM {table} WHERE address = ?")
        } else {
            format!("SELECT address, votes_token, name, version, src5_supported, governor_interface_supported FROM {table} WHERE address = $1")
        };
        let row = sqlx::query(&sql)
            .bind(felt_to_blob(governor))
            .fetch_optional(&self.pool)
            .await?;

        Ok(row.map(|row| GovernorRecord {
            address: blob_to_felt(row.get::<Vec<u8>, _>(0).as_slice()),
            votes_token: row
                .get::<Option<Vec<u8>>, _>(1)
                .map(|value| blob_to_felt(&value)),
            name: row.get(2),
            version: row.get(3),
            src5_supported: bool_from_row(&row, 4),
            governor_interface_supported: bool_from_row(&row, 5),
        }))
    }

    pub async fn list_governors(&self, addresses: &[Felt]) -> Result<Vec<GovernorRecord>> {
        let table = self.table("governors");
        let mut sql = format!("SELECT address, votes_token, name, version, src5_supported, governor_interface_supported FROM {table}");
        let rows = if addresses.is_empty() {
            sql.push_str(" ORDER BY address");
            sqlx::query(&sql).fetch_all(&self.pool).await?
        } else {
            let placeholders = (0..addresses.len())
                .map(|idx| match self.backend {
                    Backend::Sqlite => "?".to_string(),
                    Backend::Postgres => format!("${}", idx + 1),
                })
                .collect::<Vec<_>>()
                .join(", ");
            sql.push_str(&format!(
                " WHERE address IN ({placeholders}) ORDER BY address"
            ));
            let mut query = sqlx::query(&sql);
            for address in addresses {
                query = query.bind(felt_to_blob(*address));
            }
            query.fetch_all(&self.pool).await?
        };

        rows.into_iter()
            .map(|row| {
                Ok(GovernorRecord {
                    address: blob_to_felt(row.get::<Vec<u8>, _>(0).as_slice()),
                    votes_token: row
                        .get::<Option<Vec<u8>>, _>(1)
                        .map(|value| blob_to_felt(&value)),
                    name: row.get(2),
                    version: row.get(3),
                    src5_supported: bool_from_row(&row, 4),
                    governor_interface_supported: bool_from_row(&row, 5),
                })
            })
            .collect()
    }

    pub async fn upsert_proposal(&self, record: &ProposalRecord) -> Result<()> {
        let table = self.table("proposals");
        let sql = match self.backend {
            Backend::Sqlite => format!(
                "INSERT INTO {table} (governor, proposal_id, proposer, description, snapshot, deadline, status, created_block, created_tx_hash, created_timestamp, queued_block, executed_block, canceled_block) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(governor, proposal_id) DO UPDATE SET proposer = COALESCE(excluded.proposer, {table}.proposer), description = COALESCE(excluded.description, {table}.description), snapshot = CASE WHEN excluded.snapshot = 0 THEN {table}.snapshot ELSE excluded.snapshot END, deadline = CASE WHEN excluded.deadline = 0 THEN {table}.deadline ELSE excluded.deadline END, status = excluded.status, queued_block = COALESCE(excluded.queued_block, {table}.queued_block), executed_block = COALESCE(excluded.executed_block, {table}.executed_block), canceled_block = COALESCE(excluded.canceled_block, {table}.canceled_block)"
            ),
            Backend::Postgres => format!(
                "INSERT INTO {table} (governor, proposal_id, proposer, description, snapshot, deadline, status, created_block, created_tx_hash, created_timestamp, queued_block, executed_block, canceled_block) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT(governor, proposal_id) DO UPDATE SET proposer = COALESCE(EXCLUDED.proposer, {table}.proposer), description = COALESCE(EXCLUDED.description, {table}.description), snapshot = CASE WHEN EXCLUDED.snapshot = 0 THEN {table}.snapshot ELSE EXCLUDED.snapshot END, deadline = CASE WHEN EXCLUDED.deadline = 0 THEN {table}.deadline ELSE EXCLUDED.deadline END, status = EXCLUDED.status, queued_block = COALESCE(EXCLUDED.queued_block, {table}.queued_block), executed_block = COALESCE(EXCLUDED.executed_block, {table}.executed_block), canceled_block = COALESCE(EXCLUDED.canceled_block, {table}.canceled_block)"
            ),
        };

        sqlx::query(&sql)
            .bind(felt_to_blob(record.governor))
            .bind(felt_to_blob(record.proposal_id))
            .bind(record.proposer.map(felt_to_blob))
            .bind(record.description.clone())
            .bind(record.snapshot as i64)
            .bind(record.deadline as i64)
            .bind(record.status.clone())
            .bind(record.created_block as i64)
            .bind(felt_to_blob(record.created_tx_hash))
            .bind(record.created_timestamp)
            .bind(record.queued_block.map(|value| value as i64))
            .bind(record.executed_block.map(|value| value as i64))
            .bind(record.canceled_block.map(|value| value as i64))
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn get_proposal(
        &self,
        governor: Felt,
        proposal_id: Felt,
    ) -> Result<Option<ProposalRecord>> {
        let table = self.table("proposals");
        let sql = if self.backend == Backend::Sqlite {
            format!("SELECT id, governor, proposal_id, proposer, description, snapshot, deadline, status, created_block, created_tx_hash, created_timestamp, queued_block, executed_block, canceled_block FROM {table} WHERE governor = ? AND proposal_id = ?")
        } else {
            format!("SELECT id, governor, proposal_id, proposer, description, snapshot, deadline, status, created_block, created_tx_hash, created_timestamp, queued_block, executed_block, canceled_block FROM {table} WHERE governor = $1 AND proposal_id = $2")
        };
        let row = sqlx::query(&sql)
            .bind(felt_to_blob(governor))
            .bind(felt_to_blob(proposal_id))
            .fetch_optional(&self.pool)
            .await?;
        row.map(parse_proposal_row).transpose()
    }

    pub async fn get_proposals_filtered(
        &self,
        governor: Option<Felt>,
        proposer: Option<Felt>,
        status: Option<&str>,
        cursor: Option<ProposalCursor>,
        limit: u32,
    ) -> Result<(Vec<ProposalRecord>, Option<ProposalCursor>)> {
        let table = self.table("proposals");
        let mut sql = format!("SELECT id, governor, proposal_id, proposer, description, snapshot, deadline, status, created_block, created_tx_hash, created_timestamp, queued_block, executed_block, canceled_block FROM {table} WHERE 1=1");
        let mut binds: Vec<BindValue> = Vec::new();
        if let Some(governor) = governor {
            sql.push_str(" AND governor = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(governor)));
        }
        if let Some(proposer) = proposer {
            sql.push_str(" AND proposer = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(proposer)));
        }
        if let Some(status) = status {
            sql.push_str(" AND status = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::String(status.to_string()));
        }
        if let Some(cursor) = cursor {
            sql.push_str(" AND id < ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::I64(cursor.id));
        }
        sql.push_str(" ORDER BY id DESC LIMIT ");
        push_placeholder(&mut sql, self.backend, binds.len() + 1);
        binds.push(BindValue::I64(limit as i64 + 1));

        let rows = bind_query(sqlx::query(&sql), binds)
            .fetch_all(&self.pool)
            .await?;
        let mut proposals = rows
            .into_iter()
            .map(parse_proposal_row)
            .collect::<Result<Vec<_>>>()?;
        let has_more = proposals.len() as u32 > limit;
        let next_cursor = if proposals.len() as u32 > limit {
            let last = proposals.pop().context("expected extra proposal row")?;
            Some(ProposalCursor { id: last.id })
        } else {
            None
        };
        Ok((proposals, next_cursor.filter(|_| has_more)))
    }

    pub async fn insert_vote(&self, record: &VoteRecord) -> Result<()> {
        let table = self.table("votes");
        let sql = if self.backend == Backend::Sqlite {
            format!("INSERT INTO {table} (governor, proposal_id, voter, support, weight, reason, params, block_number, tx_hash, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
        } else {
            format!("INSERT INTO {table} (governor, proposal_id, voter, support, weight, reason, params, block_number, tx_hash, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)")
        };
        sqlx::query(&sql)
            .bind(felt_to_blob(record.governor))
            .bind(felt_to_blob(record.proposal_id))
            .bind(felt_to_blob(record.voter))
            .bind(record.support as i32)
            .bind(u256_to_blob(record.weight))
            .bind(record.reason.clone())
            .bind(record.params.clone())
            .bind(record.block_number as i64)
            .bind(felt_to_blob(record.tx_hash))
            .bind(record.timestamp)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_votes_filtered(
        &self,
        governor: Option<Felt>,
        proposal_id: Option<Felt>,
        voter: Option<Felt>,
        cursor: Option<VoteCursor>,
        limit: u32,
    ) -> Result<(Vec<VoteRecord>, Option<VoteCursor>)> {
        let table = self.table("votes");
        let mut sql = format!("SELECT id, governor, proposal_id, voter, support, weight, reason, params, block_number, tx_hash, timestamp FROM {table} WHERE 1=1");
        let mut binds = Vec::new();
        if let Some(governor) = governor {
            sql.push_str(" AND governor = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(governor)));
        }
        if let Some(proposal_id) = proposal_id {
            sql.push_str(" AND proposal_id = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(proposal_id)));
        }
        if let Some(voter) = voter {
            sql.push_str(" AND voter = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(voter)));
        }
        if let Some(cursor) = cursor {
            sql.push_str(" AND id < ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::I64(cursor.id));
        }
        sql.push_str(" ORDER BY id DESC LIMIT ");
        push_placeholder(&mut sql, self.backend, binds.len() + 1);
        binds.push(BindValue::I64(limit as i64 + 1));
        let rows = bind_query(sqlx::query(&sql), binds)
            .fetch_all(&self.pool)
            .await?;
        let mut votes = rows
            .into_iter()
            .map(parse_vote_row)
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = if votes.len() as u32 > limit {
            let last = votes.pop().context("expected extra vote row")?;
            Some(VoteCursor { id: last.id })
        } else {
            None
        };
        Ok((votes, next_cursor))
    }

    pub async fn insert_delegation(&self, record: &DelegationRecord) -> Result<()> {
        let table = self.table("delegations");
        let sql = if self.backend == Backend::Sqlite {
            format!("INSERT INTO {table} (votes_token, delegator, from_delegate, to_delegate, block_number, tx_hash, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)")
        } else {
            format!("INSERT INTO {table} (votes_token, delegator, from_delegate, to_delegate, block_number, tx_hash, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7)")
        };
        sqlx::query(&sql)
            .bind(felt_to_blob(record.votes_token))
            .bind(felt_to_blob(record.delegator))
            .bind(felt_to_blob(record.from_delegate))
            .bind(felt_to_blob(record.to_delegate))
            .bind(record.block_number as i64)
            .bind(felt_to_blob(record.tx_hash))
            .bind(record.timestamp)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn get_delegations_filtered(
        &self,
        votes_token: Option<Felt>,
        delegator: Option<Felt>,
        delegatee: Option<Felt>,
        cursor: Option<DelegationCursor>,
        limit: u32,
    ) -> Result<(Vec<DelegationRecord>, Option<DelegationCursor>)> {
        let table = self.table("delegations");
        let mut sql = format!("SELECT id, votes_token, delegator, from_delegate, to_delegate, block_number, tx_hash, timestamp FROM {table} WHERE 1=1");
        let mut binds = Vec::new();
        if let Some(votes_token) = votes_token {
            sql.push_str(" AND votes_token = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(votes_token)));
        }
        if let Some(delegator) = delegator {
            sql.push_str(" AND delegator = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(delegator)));
        }
        if let Some(delegatee) = delegatee {
            sql.push_str(" AND to_delegate = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(delegatee)));
        }
        if let Some(cursor) = cursor {
            sql.push_str(" AND id < ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::I64(cursor.id));
        }
        sql.push_str(" ORDER BY id DESC LIMIT ");
        push_placeholder(&mut sql, self.backend, binds.len() + 1);
        binds.push(BindValue::I64(limit as i64 + 1));
        let rows = bind_query(sqlx::query(&sql), binds)
            .fetch_all(&self.pool)
            .await?;
        let mut delegations = rows
            .into_iter()
            .map(parse_delegation_row)
            .collect::<Result<Vec<_>>>()?;
        let next_cursor = if delegations.len() as u32 > limit {
            let last = delegations.pop().context("expected extra delegation row")?;
            Some(DelegationCursor { id: last.id })
        } else {
            None
        };
        Ok((delegations, next_cursor))
    }

    pub async fn upsert_voting_power(&self, record: &VotingPowerRecord) -> Result<()> {
        let existing = self
            .get_voting_power_entry(record.votes_token, record.account)
            .await?;
        if existing
            .as_ref()
            .is_some_and(|current| current.source.priority() > record.source.priority())
        {
            return Ok(());
        }

        let table = self.table("voting_power");
        let sql = match self.backend {
            Backend::Sqlite => format!(
                "INSERT INTO {table} (votes_token, account, delegate, voting_power, source, source_priority, last_block, last_tx_hash) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(votes_token, account) DO UPDATE SET delegate = excluded.delegate, voting_power = excluded.voting_power, source = excluded.source, source_priority = excluded.source_priority, last_block = excluded.last_block, last_tx_hash = excluded.last_tx_hash"
            ),
            Backend::Postgres => format!(
                "INSERT INTO {table} (votes_token, account, delegate, voting_power, source, source_priority, last_block, last_tx_hash) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT(votes_token, account) DO UPDATE SET delegate = EXCLUDED.delegate, voting_power = EXCLUDED.voting_power, source = EXCLUDED.source, source_priority = EXCLUDED.source_priority, last_block = EXCLUDED.last_block, last_tx_hash = EXCLUDED.last_tx_hash"
            ),
        };
        sqlx::query(&sql)
            .bind(felt_to_blob(record.votes_token))
            .bind(felt_to_blob(record.account))
            .bind(record.delegate.map(felt_to_blob))
            .bind(u256_to_blob(record.voting_power))
            .bind(record.source.as_str())
            .bind(record.source.priority())
            .bind(record.last_block as i64)
            .bind(felt_to_blob(record.last_tx_hash))
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn get_voting_power_entry(
        &self,
        votes_token: Felt,
        account: Felt,
    ) -> Result<Option<VotingPowerRecord>> {
        let table = self.table("voting_power");
        let sql = if self.backend == Backend::Sqlite {
            format!("SELECT votes_token, account, delegate, voting_power, source, last_block, last_tx_hash FROM {table} WHERE votes_token = ? AND account = ?")
        } else {
            format!("SELECT votes_token, account, delegate, voting_power, source, last_block, last_tx_hash FROM {table} WHERE votes_token = $1 AND account = $2")
        };
        let row = sqlx::query(&sql)
            .bind(felt_to_blob(votes_token))
            .bind(felt_to_blob(account))
            .fetch_optional(&self.pool)
            .await?;
        row.map(parse_voting_power_row).transpose()
    }

    pub async fn get_voting_power(
        &self,
        votes_token: Option<Felt>,
        account: Option<Felt>,
    ) -> Result<Vec<VotingPowerRecord>> {
        let table = self.table("voting_power");
        let mut sql = format!("SELECT votes_token, account, delegate, voting_power, source, last_block, last_tx_hash FROM {table} WHERE 1=1");
        let mut binds = Vec::new();
        if let Some(votes_token) = votes_token {
            sql.push_str(" AND votes_token = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(votes_token)));
        }
        if let Some(account) = account {
            sql.push_str(" AND account = ");
            push_placeholder(&mut sql, self.backend, binds.len() + 1);
            binds.push(BindValue::Bytes(felt_to_blob(account)));
        }
        sql.push_str(" ORDER BY votes_token, account");
        bind_query(sqlx::query(&sql), binds)
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .map(parse_voting_power_row)
            .collect()
    }
}

enum BindValue {
    Bytes(Vec<u8>),
    String(String),
    I64(i64),
}

fn push_placeholder(sql: &mut String, backend: Backend, index: usize) {
    match backend {
        Backend::Sqlite => sql.push('?'),
        Backend::Postgres => sql.push_str(&format!("${index}")),
    }
}

fn bind_query<'q>(
    mut query: sqlx::query::Query<'q, Any, sqlx::any::AnyArguments<'q>>,
    binds: Vec<BindValue>,
) -> sqlx::query::Query<'q, Any, sqlx::any::AnyArguments<'q>> {
    for bind in binds {
        query = match bind {
            BindValue::Bytes(value) => query.bind(value),
            BindValue::String(value) => query.bind(value),
            BindValue::I64(value) => query.bind(value),
        };
    }
    query
}

fn parse_proposal_row(row: sqlx::any::AnyRow) -> Result<ProposalRecord> {
    Ok(ProposalRecord {
        id: row.get(0),
        governor: blob_to_felt(row.get::<Vec<u8>, _>(1).as_slice()),
        proposal_id: blob_to_felt(row.get::<Vec<u8>, _>(2).as_slice()),
        proposer: row
            .get::<Option<Vec<u8>>, _>(3)
            .map(|value| blob_to_felt(&value)),
        description: row.get(4),
        snapshot: row.get::<i64, _>(5) as u64,
        deadline: row.get::<i64, _>(6) as u64,
        status: row.get(7),
        created_block: row.get::<i64, _>(8) as u64,
        created_tx_hash: blob_to_felt(row.get::<Vec<u8>, _>(9).as_slice()),
        created_timestamp: row.get(10),
        queued_block: row.get::<Option<i64>, _>(11).map(|value| value as u64),
        executed_block: row.get::<Option<i64>, _>(12).map(|value| value as u64),
        canceled_block: row.get::<Option<i64>, _>(13).map(|value| value as u64),
    })
}

fn parse_vote_row(row: sqlx::any::AnyRow) -> Result<VoteRecord> {
    Ok(VoteRecord {
        id: row.get(0),
        governor: blob_to_felt(row.get::<Vec<u8>, _>(1).as_slice()),
        proposal_id: blob_to_felt(row.get::<Vec<u8>, _>(2).as_slice()),
        voter: blob_to_felt(row.get::<Vec<u8>, _>(3).as_slice()),
        support: row.get::<i32, _>(4) as u8,
        weight: blob_to_u256(row.get::<Vec<u8>, _>(5).as_slice()),
        reason: row.get(6),
        params: row.get(7),
        block_number: row.get::<i64, _>(8) as u64,
        tx_hash: blob_to_felt(row.get::<Vec<u8>, _>(9).as_slice()),
        timestamp: row.get(10),
    })
}

fn parse_delegation_row(row: sqlx::any::AnyRow) -> Result<DelegationRecord> {
    Ok(DelegationRecord {
        id: row.get(0),
        votes_token: blob_to_felt(row.get::<Vec<u8>, _>(1).as_slice()),
        delegator: blob_to_felt(row.get::<Vec<u8>, _>(2).as_slice()),
        from_delegate: blob_to_felt(row.get::<Vec<u8>, _>(3).as_slice()),
        to_delegate: blob_to_felt(row.get::<Vec<u8>, _>(4).as_slice()),
        block_number: row.get::<i64, _>(5) as u64,
        tx_hash: blob_to_felt(row.get::<Vec<u8>, _>(6).as_slice()),
        timestamp: row.get(7),
    })
}

fn parse_voting_power_row(row: sqlx::any::AnyRow) -> Result<VotingPowerRecord> {
    Ok(VotingPowerRecord {
        votes_token: blob_to_felt(row.get::<Vec<u8>, _>(0).as_slice()),
        account: blob_to_felt(row.get::<Vec<u8>, _>(1).as_slice()),
        delegate: row
            .get::<Option<Vec<u8>>, _>(2)
            .map(|value| blob_to_felt(&value)),
        voting_power: blob_to_u256(row.get::<Vec<u8>, _>(3).as_slice()),
        source: VotingPowerSource::parse(&row.get::<String, _>(4)),
        last_block: row.get::<i64, _>(5) as u64,
        last_tx_hash: blob_to_felt(row.get::<Vec<u8>, _>(6).as_slice()),
    })
}

fn bool_from_row(row: &sqlx::any::AnyRow, index: usize) -> bool {
    row.try_get::<bool, _>(index)
        .unwrap_or_else(|_| row.get::<i64, _>(index) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn stores_and_reads_governance_records() {
        let storage = GovernanceStorage::new("sqlite::memory:").await.unwrap();
        let governor = GovernorRecord {
            address: Felt::from(1u64),
            votes_token: Some(Felt::from(2u64)),
            name: Some("DAO".to_string()),
            version: Some("1".to_string()),
            src5_supported: true,
            governor_interface_supported: true,
        };
        storage.upsert_governor(&governor).await.unwrap();
        let stored = storage
            .get_governor(governor.address)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stored.votes_token, governor.votes_token);
    }

    #[tokio::test]
    async fn voting_power_priority_prefers_delegate_votes_changed() {
        let storage = GovernanceStorage::new("sqlite::memory:").await.unwrap();
        let token = Felt::from(10u64);
        let account = Felt::from(20u64);
        storage
            .upsert_voting_power(&VotingPowerRecord {
                votes_token: token,
                account,
                delegate: None,
                voting_power: U256::from(10u64),
                source: VotingPowerSource::DelegateVotesChanged,
                last_block: 10,
                last_tx_hash: Felt::from(30u64),
            })
            .await
            .unwrap();
        storage
            .upsert_voting_power(&VotingPowerRecord {
                votes_token: token,
                account,
                delegate: None,
                voting_power: U256::from(5u64),
                source: VotingPowerSource::Erc20Balance,
                last_block: 11,
                last_tx_hash: Felt::from(31u64),
            })
            .await
            .unwrap();
        let entries = storage
            .get_voting_power(Some(token), Some(account))
            .await
            .unwrap();
        assert_eq!(entries[0].voting_power, U256::from(10u64));
        assert_eq!(entries[0].source, VotingPowerSource::DelegateVotesChanged);
    }
}
