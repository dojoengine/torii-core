import {
  formatTimestamp,
  truncateAddress,
  type BalanceResult,
  type TransferResult,
} from "@torii-tokens/shared";

interface QueryResultsProps {
  contractAddress: string;
  wallet: string;
  erc20Balance: BalanceResult | null;
  erc20Transfers: TransferResult[];
  loading: boolean;
  error: string | null;
}

export default function QueryResults({
  contractAddress,
  wallet,
  erc20Balance,
  erc20Transfers,
  loading,
  error,
}: QueryResultsProps) {
  if (!contractAddress && !wallet) {
    return (
      <section className="panel results-panel full-width">
        <h2>Query Results</h2>
        <div className="empty-state">
          Enter a contract address or wallet to query balances and transfers
        </div>
      </section>
    );
  }

  if (loading) {
    return (
      <section className="panel results-panel full-width">
        <h2>Query Results</h2>
        <div className="empty-state">Loading...</div>
      </section>
    );
  }

  if (error) {
    return (
      <section className="panel results-panel full-width">
        <h2>Query Results</h2>
        <div className="error-state">{error}</div>
      </section>
    );
  }

  return (
    <section className="panel results-panel full-width">
      <h2>Query Results</h2>

      <div className="results-section">
        <h3>ERC20 Balance</h3>
        <div className="status-grid">
          <div className="stat">
            <div className="stat-label">Balance</div>
            <div className="stat-value">{erc20Balance?.balance ?? "0"}</div>
          </div>
          <div className="stat">
            <div className="stat-label">Last Updated Block</div>
            <div className="stat-value">{erc20Balance?.lastBlock ?? "-"}</div>
          </div>
        </div>
      </div>

      <div className="results-section" style={{ marginTop: "1.5rem" }}>
        <h3>Transfer History ({erc20Transfers.length})</h3>
        {erc20Transfers.length === 0 ? (
          <div className="empty-state">No transfers found</div>
        ) : (
          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>From</th>
                  <th>To</th>
                  <th>Amount</th>
                  <th>Block</th>
                  <th>Time</th>
                </tr>
              </thead>
              <tbody>
                {erc20Transfers.map((t, idx) => (
                  <tr key={`${t.txHash}-${idx}`}>
                    <td className="address">{truncateAddress(t.from)}</td>
                    <td className="address">{truncateAddress(t.to)}</td>
                    <td className="amount">{t.amount}</td>
                    <td>{t.blockNumber}</td>
                    <td className="timestamp">{formatTimestamp(t.timestamp)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </section>
  );
}
