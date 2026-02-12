interface StatusPanelProps {
  connected: boolean;
  clientId: string;
  serverUrl: string;
  updateCount: number;
  onSubscribe: () => void;
  onDisconnect: () => void;
}

export default function StatusPanel({
  connected,
  clientId,
  serverUrl,
  updateCount,
  onSubscribe,
  onDisconnect,
}: StatusPanelProps) {
  return (
    <section className="panel status-panel full-width">
      <h2>Status</h2>
      <div className="status-grid">
        <div className="stat">
          <div className="stat-label">Connection</div>
          <div className={`stat-value ${connected ? "success" : ""}`}>
            {connected ? "Connected" : "Disconnected"}
          </div>
        </div>
        <div className="stat">
          <div className="stat-label">Server</div>
          <div className="stat-value">{serverUrl}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Client ID</div>
          <div className="stat-value mono">{clientId}</div>
        </div>
        <div className="stat">
          <div className="stat-label">Updates</div>
          <div className="stat-value">{updateCount}</div>
        </div>
      </div>
      <div className="btn-group" style={{ marginTop: "1rem", justifyContent: "center" }}>
        {connected ? (
          <button className="btn btn-danger" onClick={onDisconnect}>
            Disconnect
          </button>
        ) : (
          <button className="btn btn-primary" onClick={onSubscribe}>
            Subscribe
          </button>
        )}
      </div>
    </section>
  );
}
