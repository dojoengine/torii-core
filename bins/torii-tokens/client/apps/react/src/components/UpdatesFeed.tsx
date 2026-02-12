import { formatTimestamp, getUpdateTypeName } from "@torii-tokens/shared";

interface Update {
  topic: string;
  updateType: number;
  timestamp: number;
  typeId: string;
  data?: unknown;
}

interface UpdatesFeedProps {
  updates: Update[];
  connected: boolean;
  onClear: () => void;
}

export default function UpdatesFeed({ updates, connected, onClear }: UpdatesFeedProps) {
  return (
    <section className="panel full-width">
      <div className="panel-header">
        <h2>Real-time Updates ({updates.length})</h2>
        <button className="btn btn-sm" onClick={onClear}>
          Clear
        </button>
      </div>

      {updates.length === 0 ? (
        <div className="empty-state">
          {connected
            ? "Waiting for updates..."
            : "Subscribe to start receiving updates"}
        </div>
      ) : (
        <div className="updates-list">
          {updates.map((u, i) => (
            <div key={i} className="update-item">
              <div className="update-header">
                <span
                  className={`badge badge-${getUpdateTypeName(u.updateType).toLowerCase()}`}
                >
                  {getUpdateTypeName(u.updateType)}
                </span>
                <span className="update-topic">{u.topic}</span>
                <span className="update-time">{formatTimestamp(u.timestamp)}</span>
              </div>
              <div className="update-body">
                <pre style={{ fontSize: "0.85rem", overflowX: "auto" }}>
                  {JSON.stringify(u.data, null, 2)}
                </pre>
              </div>
            </div>
          ))}
        </div>
      )}
    </section>
  );
}
