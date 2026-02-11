import {
  formatTimestamp,
  truncateAddress,
  type TokenMetadataResult,
} from "@torii-tokens/shared";

interface Stats {
  totalTransfers: number;
  totalApprovals?: number;
  uniqueTokens: number;
  uniqueAccounts: number;
}

interface Transfer {
  id: string;
  token: string;
  from: string;
  to: string;
  amount?: string;
  tokenId?: string;
  blockNumber: number;
  timestamp: number;
}

interface TokenPanelProps {
  title: string;
  tokenType: "erc20" | "erc721" | "erc1155";
  stats: Stats | null;
  transfers: Transfer[];
  metadata?: TokenMetadataResult[];
  showAmount: boolean;
}

export default function TokenPanel({
  title,
  tokenType,
  stats,
  transfers,
  metadata = [],
  showAmount,
}: TokenPanelProps) {
  return (
    <section className={`panel token-panel ${tokenType}`}>
      <h2>{title}</h2>

      {stats && (
        <div className="status-grid" style={{ marginBottom: "1rem" }}>
          <div className="stat">
            <div className="stat-label">Transfers</div>
            <div className="stat-value">{stats.totalTransfers}</div>
          </div>
          {stats.totalApprovals !== undefined && (
            <div className="stat">
              <div className="stat-label">Approvals</div>
              <div className="stat-value">{stats.totalApprovals}</div>
            </div>
          )}
          <div className="stat">
            <div className="stat-label">
              {tokenType === "erc721" ? "Collections" : "Tokens"}
            </div>
            <div className="stat-value">{stats.uniqueTokens}</div>
          </div>
          <div className="stat">
            <div className="stat-label">
              {tokenType === "erc721" ? "Owners" : "Accounts"}
            </div>
            <div className="stat-value">{stats.uniqueAccounts}</div>
          </div>
        </div>
      )}

      {metadata.length > 0 && (
        <div className="metadata-list" style={{ marginBottom: "1rem" }}>
          <h3 style={{ fontSize: "0.9rem", marginBottom: "0.5rem" }}>Token Metadata</h3>
          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>Contract</th>
                  <th>Name</th>
                  <th>Symbol</th>
                  {tokenType === "erc20" ? <th>Decimals</th> : null}
                </tr>
              </thead>
              <tbody>
                {metadata.map((m) => (
                  <tr key={m.token}>
                    <td className="address">{truncateAddress(m.token)}</td>
                    <td>{m.name ?? "—"}</td>
                    <td>{m.symbol ?? "—"}</td>
                    {tokenType === "erc20" ? <td>{m.decimals ?? "—"}</td> : null}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {transfers.length === 0 ? (
        <div className="empty-state">No transfers yet</div>
      ) : (
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>From</th>
                <th>To</th>
                <th>{showAmount ? "Amount" : "Token ID"}</th>
                <th>Block</th>
                <th>Time</th>
              </tr>
            </thead>
            <tbody>
              {transfers.map((t) => (
                <tr key={t.id}>
                  <td className="address">{truncateAddress(t.from)}</td>
                  <td className="address">{truncateAddress(t.to)}</td>
                  <td className="amount">
                    {showAmount ? t.amount : t.tokenId}
                  </td>
                  <td>{t.blockNumber}</td>
                  <td className="timestamp">{formatTimestamp(t.timestamp)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  );
}
