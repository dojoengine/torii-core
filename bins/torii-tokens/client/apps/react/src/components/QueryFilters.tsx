import { useState } from "react";

interface QueryFiltersProps {
  onQuery: (contractAddress: string, wallet: string) => void;
  loading?: boolean;
}

export default function QueryFilters({ onQuery, loading }: QueryFiltersProps) {
  const [contractAddress, setContractAddress] = useState("");
  const [wallet, setWallet] = useState("");

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (contractAddress || wallet) {
      onQuery(contractAddress, wallet);
    }
  };

  return (
    <section className="panel query-panel full-width">
      <h2>Query Balances & Transfers</h2>
      <form onSubmit={handleSubmit}>
        <div className="form-grid">
          <div className="form-group">
            <label htmlFor="contractAddress">Contract Address</label>
            <input
              type="text"
              id="contractAddress"
              value={contractAddress}
              onChange={(e) => setContractAddress(e.target.value)}
              placeholder="0x..."
              className="input"
            />
          </div>
          <div className="form-group">
            <label htmlFor="wallet">Wallet Address</label>
            <input
              type="text"
              id="wallet"
              value={wallet}
              onChange={(e) => setWallet(e.target.value)}
              placeholder="0x..."
              className="input"
            />
          </div>
        </div>
        <div className="btn-group" style={{ marginTop: "1rem", justifyContent: "center" }}>
          <button
            type="submit"
            className="btn btn-primary"
            disabled={(!contractAddress && !wallet) || loading}
          >
            {loading ? "Loading..." : "Query"}
          </button>
        </div>
      </form>
    </section>
  );
}
