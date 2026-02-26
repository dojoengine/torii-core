<script lang="ts">
  import { truncateAddress, type AttributeFacetCountResult } from "@torii-tokens/shared";

  interface Props {
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
    onStandardChange: (value: "erc721" | "erc1155") => void;
    onContractAddressChange: (value: string) => void;
    onFiltersTextChange: (value: string) => void;
    onRun: () => void;
    onNext: () => void;
    onPrev: () => void;
  }

  let {
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
  }: Props = $props();
</script>

<section class="panel full-width">
  <h2>Collection Explorer</h2>
  <p style="color: var(--color-text-secondary); margin-bottom: 0.75rem;">
    Filter format: one line per key, ex: <code>Background=Blue|Red</code>
  </p>
  <div class="query-panel" style="margin-bottom: 1rem;">
    <div style="display: grid; grid-template-columns: 160px 1fr; gap: 0.75rem;">
      <div>
        <label for="collection-standard">Standard</label>
        <select
          id="collection-standard"
          value={standard}
          onchange={(e) => onStandardChange((e.target as HTMLSelectElement).value as "erc721" | "erc1155")}
          style="width: 100%; padding: 0.6rem; border-radius: var(--radius-sm); border: 1px solid var(--color-border);"
        >
          <option value="erc721">ERC721</option>
          <option value="erc1155">ERC1155</option>
        </select>
      </div>
      <div>
        <label for="collection-contract">Contract Address</label>
        <input
          id="collection-contract"
          type="text"
          placeholder="0x..."
          value={contractAddress}
          oninput={(e) => onContractAddressChange((e.target as HTMLInputElement).value)}
        />
      </div>
    </div>
    <div style="margin-top: 0.75rem;">
      <label for="collection-filters">Trait Filters</label>
      <textarea
        id="collection-filters"
        rows="6"
        placeholder={"Background=Blue|Red\nEyes=Green"}
        value={filtersText}
        oninput={(e) => onFiltersTextChange((e.target as HTMLTextAreaElement).value)}
        style="width: 100%; border: 1px solid var(--color-border); border-radius: var(--radius-sm); padding: 0.65rem; font-family: var(--font-mono); font-size: 0.9rem;"
      ></textarea>
    </div>
    <div class="btn-group" style="margin-top: 0.75rem;">
      <button class="btn btn-primary" disabled={loading || !contractAddress.trim()} onclick={onRun}>
        {loading ? "Loading..." : "Run Filters"}
      </button>
    </div>
  </div>

  {#if error}
    <div class="error-state">{error}</div>
  {/if}

  <div class="results-section">
    <h3>Matched Token IDs ({totalHits})</h3>
    {#if tokenIds.length === 0}
      <div class="empty-state">No matches</div>
    {:else}
      <div class="table-container">
        <table>
          <thead><tr><th>Token ID</th></tr></thead>
          <tbody>
            {#each tokenIds as tokenId (tokenId)}
              <tr><td class="address">{truncateAddress(tokenId, 10)}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
    <div class="btn-group" style="margin-top: 0.5rem; justify-content: flex-end;">
      <button class="btn btn-sm" onclick={onPrev} disabled={!canPrev || loading}>Prev</button>
      <button class="btn btn-sm" onclick={onNext} disabled={!canNext || loading}>Next</button>
    </div>
  </div>

  <div class="results-section" style="margin-top: 1rem;">
    <h3>Facet Counts</h3>
    {#if facets.length === 0}
      <div class="empty-state">No facet data</div>
    {:else}
      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>Trait</th>
              <th>Value</th>
              <th>Count</th>
            </tr>
          </thead>
          <tbody>
            {#each facets as facet (`${facet.key}:${facet.value}`)}
              <tr>
                <td>{facet.key}</td>
                <td>{facet.value}</td>
                <td>{facet.count}</td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>
</section>

