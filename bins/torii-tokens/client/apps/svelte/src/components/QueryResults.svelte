<script lang="ts">
  import {
    formatTimestamp,
    truncateAddress,
    type BalanceResult,
    type TransferResult,
  } from "@torii-tokens/shared";

  interface Props {
    contractAddress: string;
    wallet: string;
    erc20Balance: BalanceResult | null;
    erc20Transfers: TransferResult[];
    loading: boolean;
    error: string | null;
  }

  let { contractAddress, wallet, erc20Balance, erc20Transfers, loading, error }: Props = $props();
</script>

{#if !contractAddress && !wallet}
  <section class="panel results-panel full-width">
    <h2>Query Results</h2>
    <div class="empty-state">
      Enter a contract address or wallet to query balances and transfers
    </div>
  </section>
{:else if loading}
  <section class="panel results-panel full-width">
    <h2>Query Results</h2>
    <div class="empty-state">Loading...</div>
  </section>
{:else if error}
  <section class="panel results-panel full-width">
    <h2>Query Results</h2>
    <div class="error-state">{error}</div>
  </section>
{:else}
  <section class="panel results-panel full-width">
    <h2>Query Results</h2>

    <div class="results-section">
      <h3>ERC20 Balance</h3>
      <div class="status-grid">
        <div class="stat">
          <div class="stat-label">Balance</div>
          <div class="stat-value">{erc20Balance?.balance ?? "0"}</div>
        </div>
        <div class="stat">
          <div class="stat-label">Last Updated Block</div>
          <div class="stat-value">{erc20Balance?.lastBlock ?? "-"}</div>
        </div>
      </div>
    </div>

    <div class="results-section" style="margin-top: 1.5rem;">
      <h3>Transfer History ({erc20Transfers.length})</h3>
      {#if erc20Transfers.length === 0}
        <div class="empty-state">No transfers found</div>
      {:else}
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
              {#each erc20Transfers as t, idx (`${t.txHash}-${idx}`)}
                <tr>
                  <td class="address">{truncateAddress(t.from)}</td>
                  <td class="address">{truncateAddress(t.to)}</td>
                  <td class="amount">{t.amount}</td>
                  <td>{t.blockNumber}</td>
                  <td class="timestamp">{formatTimestamp(t.timestamp)}</td>
                </tr>
              {/each}
            </tbody>
          </table>
        </div>
      {/if}
    </div>
  </section>
{/if}
