<script lang="ts">
  import {
    truncateAddress,
    type AttributeFacetCountResult,
    type CollectionOverviewResult,
    type CollectionTokenResult,
    type TraitSummaryResult,
  } from "@torii-tokens/shared";

  interface Props {
    standard: "erc721" | "erc1155";
    contractAddress: string;
    filtersText: string;
    overviewContractsText: string;
    tokens: CollectionTokenResult[];
    tokensFacets: AttributeFacetCountResult[];
    tokensTotalHits: number;
    tokensLoading: boolean;
    tokensError: string | null;
    tokensCanPrev: boolean;
    tokensCanNext: boolean;
    traits: TraitSummaryResult[];
    traitFacets: AttributeFacetCountResult[];
    traitsTotalHits: number;
    traitsLoading: boolean;
    traitsError: string | null;
    overview: CollectionOverviewResult | null;
    overviewLoading: boolean;
    overviewError: string | null;
    overviewCanRun: boolean;
    onStandardChange: (value: "erc721" | "erc1155") => void;
    onContractAddressChange: (value: string) => void;
    onFiltersTextChange: (value: string) => void;
    onOverviewContractsTextChange: (value: string) => void;
    onRunTokens: () => void;
    onTokensPrev: () => void;
    onTokensNext: () => void;
    onRunTraitFacets: () => void;
    onRunOverview: () => void;
  }

  let {
    standard,
    contractAddress,
    filtersText,
    overviewContractsText,
    tokens,
    tokensFacets,
    tokensTotalHits,
    tokensLoading,
    tokensError,
    tokensCanPrev,
    tokensCanNext,
    traits,
    traitFacets,
    traitsTotalHits,
    traitsLoading,
    traitsError,
    overview,
    overviewLoading,
    overviewError,
    overviewCanRun,
    onStandardChange,
    onContractAddressChange,
    onFiltersTextChange,
    onOverviewContractsTextChange,
    onRunTokens,
    onTokensPrev,
    onTokensNext,
    onRunTraitFacets,
    onRunOverview,
  }: Props = $props();

  function toLocalImagePath(contractAddress: string, tokenId: string): string {
    const contract = contractAddress.replace(/^0x/, "");
    const token = tokenId.replace(/^0x/, "").padStart(64, "0");
    return `/image-cache/${contract}/${token}.png`;
  }

  function formatCollectionUri(uri?: string): { href?: string; label: string } {
    if (!uri) return { label: "-" };
    const value = uri.trim();
    if (!value) return { label: "-" };
    if (value.startsWith("data:application/json")) {
      return { label: "data:application/json (inline)" };
    }
    if (value.startsWith("{") || value.startsWith("[")) {
      return { label: "inline metadata json" };
    }
    if (value.startsWith("ipfs://")) {
      const cid = value.slice("ipfs://".length);
      return {
        href: `https://ipfs.io/ipfs/${cid}`,
        label: truncateAddress(value, 18),
      };
    }
    try {
      const parsed = new URL(value);
      if (parsed.protocol === "http:" || parsed.protocol === "https:") {
        const href = parsed.toString();
        return { href, label: truncateAddress(href, 18) };
      }
      return { label: truncateAddress(value, 18) };
    } catch {
      return { label: truncateAddress(value, 18) };
    }
  }
</script>

<section class="panel full-width">
  <h2>Marketplace gRPC Explorer</h2>
  <p style="color: var(--color-text-secondary); margin-bottom: 0.75rem;">
    Endpoints: <code>GetCollectionTokens</code>, <code>GetCollectionTraitFacets</code>, <code>GetCollectionOverview</code>
  </p>

  <div class="query-panel" style="margin-bottom: 1rem;">
    <div style="display: grid; grid-template-columns: 160px 1fr; gap: 0.75rem;">
      <div>
        <label for="marketplace-standard">Standard</label>
        <select
          id="marketplace-standard"
          value={standard}
          onchange={(e) => onStandardChange((e.target as HTMLSelectElement).value as "erc721" | "erc1155")}
          style="width: 100%; padding: 0.6rem; border-radius: var(--radius-sm); border: 1px solid var(--color-border);"
        >
          <option value="erc721">ERC721</option>
          <option value="erc1155">ERC1155</option>
        </select>
      </div>
      <div>
        <label for="marketplace-contract">Collection Contract Address</label>
        <input
          id="marketplace-contract"
          type="text"
          placeholder="0x..."
          value={contractAddress}
          oninput={(e) => onContractAddressChange((e.target as HTMLInputElement).value)}
        />
      </div>
    </div>
    <div style="margin-top: 0.75rem;">
      <label for="marketplace-filters">Trait Filters</label>
      <textarea
        id="marketplace-filters"
        rows="5"
        placeholder={"Background=Blue|Red\nEyes=Green"}
        value={filtersText}
        oninput={(e) => onFiltersTextChange((e.target as HTMLTextAreaElement).value)}
        style="width: 100%; border: 1px solid var(--color-border); border-radius: var(--radius-sm); padding: 0.65rem; font-family: var(--font-mono); font-size: 0.9rem;"
      ></textarea>
    </div>
  </div>

  <div class="results-section">
    <h3>GetCollectionTokens ({tokensTotalHits} matches)</h3>
    <div class="btn-group" style="margin-bottom: 0.75rem;">
      <button class="btn btn-primary" onclick={onRunTokens} disabled={tokensLoading || !contractAddress.trim()}>
        {tokensLoading ? "Loading..." : "Run GetCollectionTokens"}
      </button>
      <button class="btn btn-sm" onclick={onTokensPrev} disabled={tokensLoading || !tokensCanPrev}>Prev</button>
      <button class="btn btn-sm" onclick={onTokensNext} disabled={tokensLoading || !tokensCanNext}>Next</button>
    </div>
    {#if tokensError}
      <div class="error-state">{tokensError}</div>
    {/if}

    {#if tokens.length === 0}
      <div class="empty-state">No tokens returned</div>
    {:else}
      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>Contract</th>
              <th>Token ID</th>
              <th>URI</th>
              <th>Image</th>
            </tr>
          </thead>
          <tbody>
            {#each tokens as token (`${token.contractAddress}:${token.tokenId}`)}
              <tr>
                <td class="address">{truncateAddress(token.contractAddress, 10)}</td>
                <td class="address">{truncateAddress(token.tokenId, 10)}</td>
                <td>
                  {#if formatCollectionUri(token.uri).href}
                    <a href={formatCollectionUri(token.uri).href} target="_blank" rel="noreferrer">{formatCollectionUri(token.uri).label}</a>
                  {:else}
                    {formatCollectionUri(token.uri).label}
                  {/if}
                </td>
                <td>
                  <img
                    src={toLocalImagePath(token.contractAddress, token.tokenId)}
                    alt={`token ${token.tokenId}`}
                    loading="lazy"
                    style="width:44px;height:44px;object-fit:cover;border-radius:6px;border:1px solid var(--color-border-light);"
                    onerror={(e) => {
                      const img = e.currentTarget as HTMLImageElement;
                      if (token.imageUrl && img.src !== token.imageUrl) {
                        img.src = token.imageUrl;
                      } else {
                        img.style.display = "none";
                      }
                    }}
                  />
                </td>
              </tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}

    <h4 style="margin-top: 0.75rem; margin-bottom: 0.5rem;">Facet Counts</h4>
    {#if tokensFacets.length === 0}
      <div class="empty-state">No facet data</div>
    {:else}
      <div class="table-container">
        <table>
          <thead><tr><th>Trait</th><th>Value</th><th>Count</th></tr></thead>
          <tbody>
            {#each tokensFacets as facet (`${facet.key}:${facet.value}`)}
              <tr><td>{facet.key}</td><td>{facet.value}</td><td>{facet.count}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>

  <div class="results-section" style="margin-top: 1rem;">
    <h3>GetCollectionTraitFacets ({traitsTotalHits} matches)</h3>
    <div class="btn-group" style="margin-bottom: 0.75rem;">
      <button class="btn btn-primary" onclick={onRunTraitFacets} disabled={traitsLoading || !contractAddress.trim()}>
        {traitsLoading ? "Loading..." : "Run GetCollectionTraitFacets"}
      </button>
    </div>
    {#if traitsError}
      <div class="error-state">{traitsError}</div>
    {/if}

    <h4 style="margin-top: 0.25rem; margin-bottom: 0.5rem;">Trait Summary</h4>
    {#if traits.length === 0}
      <div class="empty-state">No trait summary data</div>
    {:else}
      <div class="table-container">
        <table>
          <thead><tr><th>Trait</th><th>Distinct Values</th></tr></thead>
          <tbody>
            {#each traits as trait (trait.key)}
              <tr><td>{trait.key}</td><td>{trait.valueCount}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}

    <h4 style="margin-top: 0.75rem; margin-bottom: 0.5rem;">Facet Counts</h4>
    {#if traitFacets.length === 0}
      <div class="empty-state">No facet data</div>
    {:else}
      <div class="table-container">
        <table>
          <thead><tr><th>Trait</th><th>Value</th><th>Count</th></tr></thead>
          <tbody>
            {#each traitFacets as facet (`${facet.key}:${facet.value}`)}
              <tr><td>{facet.key}</td><td>{facet.value}</td><td>{facet.count}</td></tr>
            {/each}
          </tbody>
        </table>
      </div>
    {/if}
  </div>

  <div class="results-section" style="margin-top: 1rem;">
    <h3>GetCollectionOverview</h3>
    <label for="marketplace-overview-contracts">Overview Contracts (comma, space, or newline separated)</label>
    <textarea
      id="marketplace-overview-contracts"
      rows="3"
      placeholder={"0xabc...\n0xdef..."}
      value={overviewContractsText}
      oninput={(e) => onOverviewContractsTextChange((e.target as HTMLTextAreaElement).value)}
      style="width: 100%; border: 1px solid var(--color-border); border-radius: var(--radius-sm); padding: 0.65rem; font-family: var(--font-mono); font-size: 0.9rem; margin-top: 0.5rem;"
    ></textarea>
    <div class="btn-group" style="margin-top: 0.75rem; margin-bottom: 0.75rem;">
      <button class="btn btn-primary" onclick={onRunOverview} disabled={overviewLoading || !overviewCanRun}>
        {overviewLoading ? "Loading..." : "Run GetCollectionOverview"}
      </button>
    </div>
    {#if overviewError}
      <div class="error-state">{overviewError}</div>
    {/if}
    {#if !overview || overview.overviews.length === 0}
      <div class="empty-state">No overview data</div>
    {:else}
      {#each overview.overviews as item (item.contractAddress)}
        <div style="margin-bottom: 1rem; border: 1px solid var(--color-border-light); border-radius: var(--radius-sm); padding: 0.75rem;">
          <h4 style="margin-bottom: 0.35rem;">{truncateAddress(item.contractAddress, 12)}</h4>
          <p style="color: var(--color-text-secondary); margin-bottom: 0.5rem;">
            totalHits={item.totalHits} {item.nextCursorTokenId ? `(next: ${truncateAddress(item.nextCursorTokenId, 10)})` : ""}
          </p>

          {#if item.tokens.length === 0}
            <div class="empty-state">No tokens returned</div>
          {:else}
            <div class="table-container">
              <table>
                <thead>
                  <tr>
                    <th>Contract</th>
                    <th>Token ID</th>
                    <th>URI</th>
                    <th>Image</th>
                  </tr>
                </thead>
                <tbody>
                  {#each item.tokens as token (`${token.contractAddress}:${token.tokenId}`)}
                    <tr>
                      <td class="address">{truncateAddress(token.contractAddress, 10)}</td>
                      <td class="address">{truncateAddress(token.tokenId, 10)}</td>
                      <td>
                        {#if formatCollectionUri(token.uri).href}
                          <a href={formatCollectionUri(token.uri).href} target="_blank" rel="noreferrer">{formatCollectionUri(token.uri).label}</a>
                        {:else}
                          {formatCollectionUri(token.uri).label}
                        {/if}
                      </td>
                      <td>
                        <img
                          src={toLocalImagePath(token.contractAddress, token.tokenId)}
                          alt={`token ${token.tokenId}`}
                          loading="lazy"
                          style="width:44px;height:44px;object-fit:cover;border-radius:6px;border:1px solid var(--color-border-light);"
                          onerror={(e) => {
                            const img = e.currentTarget as HTMLImageElement;
                            if (token.imageUrl && img.src !== token.imageUrl) {
                              img.src = token.imageUrl;
                            } else {
                              img.style.display = "none";
                            }
                          }}
                        />
                      </td>
                    </tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {/if}

          <h5 style="margin-top: 0.65rem; margin-bottom: 0.35rem;">Trait Summary</h5>
          {#if item.traits.length === 0}
            <div class="empty-state">No trait summary data</div>
          {:else}
            <div class="table-container">
              <table>
                <thead><tr><th>Trait</th><th>Distinct Values</th></tr></thead>
                <tbody>
                  {#each item.traits as trait (trait.key)}
                    <tr><td>{trait.key}</td><td>{trait.valueCount}</td></tr>
                  {/each}
                </tbody>
              </table>
            </div>
          {/if}
        </div>
      {/each}
    {/if}
  </div>
</section>
