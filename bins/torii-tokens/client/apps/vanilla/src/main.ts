import "../../../shared/styles/shared.css";
import {
  createTokensClient,
  SERVER_URL,
  formatTimestamp,
  truncateAddress,
  getUpdateTypeName,
  generateClientId,
  getErc20Stats,
  getErc721Stats,
  getErc1155Stats,
  getErc20Transfers,
  getErc721Transfers,
  getErc1155Transfers,
  getErc20Balance,
  type BalanceResult,
  type TransferResult,
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

interface Update {
  topic: string;
  updateType: number;
  timestamp: number;
  typeId: string;
  data?: unknown;
}

class TokensApp {
  private client = createTokensClient(SERVER_URL);
  private clientId = generateClientId();
  private connected = false;
  private updates: Update[] = [];
  private unsubscribe: (() => void) | null = null;

  private erc20Stats: Stats | null = null;
  private erc20Transfers: Transfer[] = [];
  private erc721Stats: Stats | null = null;
  private erc721Transfers: Transfer[] = [];
  private erc1155Stats: Stats | null = null;
  private erc1155Transfers: Transfer[] = [];

  private queryContractAddress = "";
  private queryWallet = "";
  private queryLoading = false;
  private queryError: string | null = null;
  private queryErc20Balance: BalanceResult | null = null;
  private queryErc20Transfers: TransferResult[] = [];

  constructor() {
    this.render();
    this.init();
  }

  private async init() {
    await this.checkHealth();
    await this.loadAllStats();
    await this.loadAllTransfers();
    this.render();
  }

  private async checkHealth() {
    try {
      await this.client.getVersion();
      this.render();
    } catch (err) {
      console.error("Health check failed:", err);
    }
  }

  private async loadAllStats() {
    try {
      const [s20, s721, s1155] = await Promise.all([
        getErc20Stats(this.client),
        getErc721Stats(this.client),
        getErc1155Stats(this.client),
      ]);
      this.erc20Stats = {
        totalTransfers: s20.totalTransfers,
        totalApprovals: s20.totalApprovals,
        uniqueTokens: s20.uniqueTokens,
        uniqueAccounts: 0,
      };
      this.erc721Stats = {
        totalTransfers: s721.totalTransfers,
        uniqueTokens: s721.uniqueTokens,
        uniqueAccounts: s721.uniqueNfts ?? 0,
      };
      this.erc1155Stats = {
        totalTransfers: s1155.totalTransfers,
        uniqueTokens: s1155.uniqueTokens,
        uniqueAccounts: s1155.uniqueTokenIds ?? 0,
      };
    } catch (err) {
      console.error("Failed to load stats:", err);
      this.erc20Stats = { totalTransfers: 0, totalApprovals: 0, uniqueTokens: 0, uniqueAccounts: 0 };
      this.erc721Stats = { totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 };
      this.erc1155Stats = { totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 };
    }
  }

  private async loadAllTransfers() {
    const emptyQuery = { contractAddress: "", wallet: "" };
    try {
      const [t20, t721, t1155] = await Promise.all([
        getErc20Transfers(this.client, emptyQuery, 20),
        getErc721Transfers(this.client, emptyQuery, 20),
        getErc1155Transfers(this.client, emptyQuery, 20),
      ]);
      this.erc20Transfers = t20.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        amount: t.amount,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      }));
      this.erc721Transfers = t721.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        tokenId: (t as Record<string, unknown>).tokenId as string | undefined,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      }));
      this.erc1155Transfers = t1155.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        amount: t.amount,
        tokenId: t.tokenId,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      }));
    } catch (err) {
      console.error("Failed to load transfers:", err);
    }
  }

  private async handleQuery(contractAddress: string, wallet: string) {
    this.queryContractAddress = contractAddress;
    this.queryWallet = wallet;
    this.queryLoading = true;
    this.queryError = null;
    this.render();

    try {
      const [balanceResult, transfers] = await Promise.all([
        getErc20Balance(this.client, { contractAddress, wallet }),
        getErc20Transfers(this.client, { contractAddress, wallet }, 50),
      ]);
      this.queryErc20Balance = balanceResult;
      this.queryErc20Transfers = transfers;
    } catch (err) {
      console.error("Query failed:", err);
      this.queryError = err instanceof Error ? err.message : "Query failed";
    } finally {
      this.queryLoading = false;
      this.render();
    }
  }

  private async subscribe() {
    try {
      this.unsubscribe = await this.client.subscribeTopics(
        this.clientId,
        [
          { topic: "erc20.transfer" },
          { topic: "erc721.transfer" },
          { topic: "erc1155.transfer" },
        ],
        (update) => {
          this.updates = [update as Update, ...this.updates].slice(0, 50);
          this.render();
        },
        (err) => {
          console.error("Subscription error:", err);
          this.connected = false;
          this.render();
        },
        () => {
          this.connected = true;
          this.render();
        }
      );
    } catch (err) {
      console.error("Failed to subscribe:", err);
    }
  }

  private disconnect() {
    if (this.unsubscribe) {
      this.unsubscribe();
      this.unsubscribe = null;
    }
    this.connected = false;
    this.render();
  }

  private clearUpdates() {
    this.updates = [];
    this.render();
  }

  private render() {
    const app = document.getElementById("app")!;
    app.innerHTML = `
      <div class="container">
        <header>
          <h1>Torii Tokens - Vanilla</h1>
          <p class="subtitle">ERC20 / ERC721 / ERC1155 Token Indexer</p>
        </header>

        ${this.renderStatusPanel()}

        ${this.renderQueryPanel()}

        ${this.renderQueryResults()}

        <div class="panels">
          ${this.renderErc20Panel()}
          ${this.renderErc721Panel()}
          ${this.renderErc1155Panel()}
          ${this.renderUpdatesPanel()}
        </div>
      </div>
    `;

    this.attachEventListeners();
  }

  private renderQueryPanel(): string {
    return `
      <section class="panel query-panel full-width">
        <h2>Query Balances & Transfers</h2>
        <form id="query-form">
          <div class="form-grid">
            <div class="form-group">
              <label for="contractAddress">Contract Address</label>
              <input
                type="text"
                id="contractAddress"
                value="${this.queryContractAddress}"
                placeholder="0x..."
                class="input"
              />
            </div>
            <div class="form-group">
              <label for="wallet">Wallet Address</label>
              <input
                type="text"
                id="wallet"
                value="${this.queryWallet}"
                placeholder="0x..."
                class="input"
              />
            </div>
          </div>
          <div class="btn-group" style="margin-top: 1rem; justify-content: center;">
            <button
              type="submit"
              class="btn btn-primary"
              id="query-btn"
              ${this.queryLoading ? "disabled" : ""}
            >
              ${this.queryLoading ? "Loading..." : "Query"}
            </button>
          </div>
        </form>
      </section>
    `;
  }

  private renderQueryResults(): string {
    if (!this.queryContractAddress && !this.queryWallet) {
      return `
        <section class="panel results-panel full-width">
          <h2>Query Results</h2>
          <div class="empty-state">
            Enter a contract address or wallet to query balances and transfers
          </div>
        </section>
      `;
    }

    if (this.queryLoading) {
      return `
        <section class="panel results-panel full-width">
          <h2>Query Results</h2>
          <div class="empty-state">Loading...</div>
        </section>
      `;
    }

    if (this.queryError) {
      return `
        <section class="panel results-panel full-width">
          <h2>Query Results</h2>
          <div class="error-state">${this.queryError}</div>
        </section>
      `;
    }

    const balance = this.queryErc20Balance;
    const transfers = this.queryErc20Transfers;

    return `
      <section class="panel results-panel full-width">
        <h2>Query Results</h2>

        <div class="results-section">
          <h3>ERC20 Balance</h3>
          <div class="status-grid">
            <div class="stat">
              <div class="stat-label">Balance</div>
              <div class="stat-value">${balance?.balance ?? "0"}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Last Updated Block</div>
              <div class="stat-value">${balance?.lastBlock ?? "-"}</div>
            </div>
          </div>
        </div>

        <div class="results-section" style="margin-top: 1.5rem;">
          <h3>Transfer History (${transfers.length})</h3>
          ${
            transfers.length === 0
              ? `<div class="empty-state">No transfers found</div>`
              : `
            <div class="table-container">
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
                  ${transfers
                    .map(
                      (t) => `
                    <tr>
                      <td class="address">${truncateAddress(t.from)}</td>
                      <td class="address">${truncateAddress(t.to)}</td>
                      <td class="amount">${t.amount}</td>
                      <td>${t.blockNumber}</td>
                      <td class="timestamp">${formatTimestamp(t.timestamp)}</td>
                    </tr>
                  `
                    )
                    .join("")}
                </tbody>
              </table>
            </div>
          `
          }
        </div>
      </section>
    `;
  }

  private renderStatusPanel(): string {
    return `
      <section class="panel status-panel full-width">
        <h2>Status</h2>
        <div class="status-grid">
          <div class="stat">
            <div class="stat-label">Connection</div>
            <div class="stat-value ${this.connected ? "success" : ""}">
              ${this.connected ? "Connected" : "Disconnected"}
            </div>
          </div>
          <div class="stat">
            <div class="stat-label">Server</div>
            <div class="stat-value">${SERVER_URL}</div>
          </div>
          <div class="stat">
            <div class="stat-label">Client ID</div>
            <div class="stat-value mono">${this.clientId}</div>
          </div>
          <div class="stat">
            <div class="stat-label">Updates</div>
            <div class="stat-value">${this.updates.length}</div>
          </div>
        </div>
        <div class="btn-group" style="margin-top: 1rem; justify-content: center;">
          ${
            this.connected
              ? `<button class="btn btn-danger" id="disconnect-btn">Disconnect</button>`
              : `<button class="btn btn-primary" id="subscribe-btn">Subscribe</button>`
          }
        </div>
      </section>
    `;
  }

  private renderErc20Panel(): string {
    const stats = this.erc20Stats;
    return `
      <section class="panel token-panel erc20">
        <h2>ERC20 Tokens</h2>
        ${
          stats
            ? `
          <div class="status-grid" style="margin-bottom: 1rem;">
            <div class="stat">
              <div class="stat-label">Transfers</div>
              <div class="stat-value">${stats.totalTransfers}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Approvals</div>
              <div class="stat-value">${stats.totalApprovals || 0}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Tokens</div>
              <div class="stat-value">${stats.uniqueTokens}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Accounts</div>
              <div class="stat-value">${stats.uniqueAccounts}</div>
            </div>
          </div>
        `
            : ""
        }
        ${this.renderTransfersTable(this.erc20Transfers, true)}
      </section>
    `;
  }

  private renderErc721Panel(): string {
    const stats = this.erc721Stats;
    return `
      <section class="panel token-panel erc721">
        <h2>ERC721 NFTs</h2>
        ${
          stats
            ? `
          <div class="status-grid" style="margin-bottom: 1rem;">
            <div class="stat">
              <div class="stat-label">Transfers</div>
              <div class="stat-value">${stats.totalTransfers}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Collections</div>
              <div class="stat-value">${stats.uniqueTokens}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Owners</div>
              <div class="stat-value">${stats.uniqueAccounts}</div>
            </div>
          </div>
        `
            : ""
        }
        ${this.renderTransfersTable(this.erc721Transfers, false)}
      </section>
    `;
  }

  private renderErc1155Panel(): string {
    const stats = this.erc1155Stats;
    return `
      <section class="panel token-panel erc1155">
        <h2>ERC1155 Multi-Tokens</h2>
        ${
          stats
            ? `
          <div class="status-grid" style="margin-bottom: 1rem;">
            <div class="stat">
              <div class="stat-label">Transfers</div>
              <div class="stat-value">${stats.totalTransfers}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Tokens</div>
              <div class="stat-value">${stats.uniqueTokens}</div>
            </div>
            <div class="stat">
              <div class="stat-label">Accounts</div>
              <div class="stat-value">${stats.uniqueAccounts}</div>
            </div>
          </div>
        `
            : ""
        }
        ${this.renderTransfersTable(this.erc1155Transfers, false)}
      </section>
    `;
  }

  private renderTransfersTable(transfers: Transfer[], showAmount: boolean): string {
    if (transfers.length === 0) {
      return `<div class="empty-state">No transfers yet</div>`;
    }

    return `
      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>From</th>
              <th>To</th>
              ${showAmount ? "<th>Amount</th>" : "<th>Token ID</th>"}
              <th>Block</th>
              <th>Time</th>
            </tr>
          </thead>
          <tbody>
            ${transfers
              .map(
                (t) => `
              <tr>
                <td class="address">${truncateAddress(t.from)}</td>
                <td class="address">${truncateAddress(t.to)}</td>
                <td class="amount">${showAmount ? t.amount : t.tokenId}</td>
                <td>${t.blockNumber}</td>
                <td class="timestamp">${formatTimestamp(t.timestamp)}</td>
              </tr>
            `
              )
              .join("")}
          </tbody>
        </table>
      </div>
    `;
  }

  private renderUpdatesPanel(): string {
    return `
      <section class="panel full-width">
        <div class="panel-header">
          <h2>Real-time Updates (${this.updates.length})</h2>
          <button class="btn btn-sm" id="clear-updates-btn">Clear</button>
        </div>
        ${
          this.updates.length === 0
            ? `
          <div class="empty-state">
            ${this.connected ? "Waiting for updates..." : "Subscribe to start receiving updates"}
          </div>
        `
            : `
          <div class="updates-list">
            ${this.updates
              .map(
                (u) => `
              <div class="update-item">
                <div class="update-header">
                  <span class="badge badge-${getUpdateTypeName(u.updateType).toLowerCase()}">${getUpdateTypeName(u.updateType)}</span>
                  <span class="update-topic">${u.topic}</span>
                  <span class="update-time">${formatTimestamp(u.timestamp)}</span>
                </div>
                <div class="update-body">
                  <pre style="font-size: 0.85rem; overflow-x: auto;">${JSON.stringify(u.data, null, 2)}</pre>
                </div>
              </div>
            `
              )
              .join("")}
          </div>
        `
        }
      </section>
    `;
  }

  private attachEventListeners() {
    document.getElementById("subscribe-btn")?.addEventListener("click", () => this.subscribe());
    document.getElementById("disconnect-btn")?.addEventListener("click", () => this.disconnect());
    document.getElementById("clear-updates-btn")?.addEventListener("click", () => this.clearUpdates());
    document.getElementById("query-form")?.addEventListener("submit", (e) => {
      e.preventDefault();
      const contractAddress = (document.getElementById("contractAddress") as HTMLInputElement)?.value ?? "";
      const wallet = (document.getElementById("wallet") as HTMLInputElement)?.value ?? "";
      if (contractAddress || wallet) {
        this.handleQuery(contractAddress, wallet);
      }
    });
  }
}

new TokensApp();
