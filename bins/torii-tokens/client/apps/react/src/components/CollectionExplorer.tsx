import { truncateAddress, type AttributeFacetCountResult } from "@torii-tokens/shared";

interface CollectionExplorerProps {
  standard: "erc721" | "erc1155";
  contractAddress: string;
  filtersText: string;
  loading: boolean;
  error: string | null;
  totalHits: number;
  tokenIds: string[];
  facets: AttributeFacetCountResult[];
  canNext: boolean;
  canPrev: boolean;
  onStandardChange: (v: "erc721" | "erc1155") => void;
  onContractAddressChange: (v: string) => void;
  onFiltersTextChange: (v: string) => void;
  onRun: () => void;
  onNext: () => void;
  onPrev: () => void;
}

export default function CollectionExplorer({
  standard,
  contractAddress,
  filtersText,
  loading,
  error,
  totalHits,
  tokenIds,
  facets,
  canNext,
  canPrev,
  onStandardChange,
  onContractAddressChange,
  onFiltersTextChange,
  onRun,
  onNext,
  onPrev,
}: CollectionExplorerProps) {
  return (
    <section className="panel full-width">
      <h2>Collection Explorer</h2>
      <p style={{ color: "var(--color-text-secondary)", marginBottom: "0.75rem" }}>
        Filter format: one line per key, ex: <code>Background=Blue|Red</code>
      </p>

      <div className="query-panel" style={{ marginBottom: "1rem" }}>
        <div style={{ display: "grid", gridTemplateColumns: "160px 1fr", gap: "0.75rem" }}>
          <div>
            <label>Standard</label>
            <select
              value={standard}
              onChange={(e) => onStandardChange(e.target.value as "erc721" | "erc1155")}
              style={{ width: "100%", padding: "0.6rem", borderRadius: "var(--radius-sm)", border: "1px solid var(--color-border)" }}
            >
              <option value="erc721">ERC721</option>
              <option value="erc1155">ERC1155</option>
            </select>
          </div>
          <div>
            <label>Contract Address</label>
            <input
              type="text"
              placeholder="0x..."
              value={contractAddress}
              onChange={(e) => onContractAddressChange(e.target.value)}
            />
          </div>
        </div>
        <div style={{ marginTop: "0.75rem" }}>
          <label>Trait Filters</label>
          <textarea
            value={filtersText}
            onChange={(e) => onFiltersTextChange(e.target.value)}
            rows={6}
            placeholder={"Background=Blue|Red\nEyes=Green"}
            style={{
              width: "100%",
              border: "1px solid var(--color-border)",
              borderRadius: "var(--radius-sm)",
              padding: "0.65rem",
              fontFamily: "var(--font-mono)",
              fontSize: "0.9rem",
            }}
          />
        </div>
        <div className="btn-group" style={{ marginTop: "0.75rem" }}>
          <button className="btn btn-primary" disabled={loading || !contractAddress.trim()} onClick={onRun}>
            {loading ? "Loading..." : "Run Filters"}
          </button>
        </div>
      </div>

      {error ? <div className="error-state">{error}</div> : null}

      <div className="results-section">
        <h3>Matched Token IDs ({totalHits})</h3>
        {tokenIds.length === 0 ? (
          <div className="empty-state">No matches</div>
        ) : (
          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>Token ID</th>
                </tr>
              </thead>
              <tbody>
                {tokenIds.map((tokenId) => (
                  <tr key={tokenId}>
                    <td className="address">{truncateAddress(tokenId, 10)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <div className="btn-group" style={{ marginTop: "0.5rem", justifyContent: "flex-end" }}>
          <button className="btn btn-sm" onClick={onPrev} disabled={!canPrev || loading}>Prev</button>
          <button className="btn btn-sm" onClick={onNext} disabled={!canNext || loading}>Next</button>
        </div>
      </div>

      <div className="results-section" style={{ marginTop: "1rem" }}>
        <h3>Facet Counts</h3>
        {facets.length === 0 ? (
          <div className="empty-state">No facet data</div>
        ) : (
          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>Trait</th>
                  <th>Value</th>
                  <th>Count</th>
                </tr>
              </thead>
              <tbody>
                {facets.map((facet) => (
                  <tr key={`${facet.key}:${facet.value}`}>
                    <td>{facet.key}</td>
                    <td>{facet.value}</td>
                    <td>{facet.count}</td>
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

