import {
  truncateAddress,
  type AttributeFacetCountResult,
  type CollectionOverviewResult,
  type CollectionTokenResult,
  type TraitSummaryResult,
} from "@torii-tokens/shared";

export interface MarketplaceGrpcExplorerViewModel {
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
}

export interface MarketplaceGrpcExplorerHandlers {
  onStandardChange: (value: "erc721" | "erc1155") => void;
  onContractChange: (value: string) => void;
  onFiltersChange: (value: string) => void;
  onOverviewContractsChange: (value: string) => void;
  onRunTokens: () => void;
  onTokensPrev: () => void;
  onTokensNext: () => void;
  onRunTraitFacets: () => void;
  onRunOverview: () => void;
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function toLocalImagePath(contractAddress: string, tokenId: string): string {
  const contract = contractAddress.replace(/^0x/, "");
  const token = tokenId.replace(/^0x/, "").padStart(64, "0");
  return `/image-cache/${contract}/${token}.png`;
}

function formatCollectionUri(uri?: string): { href?: string; label: string } {
  if (!uri) return { label: "-" };
  const value = uri.trim();
  if (!value) return { label: "-" };
  if (value.startsWith("data:application/json")) return { label: "data:application/json (inline)" };
  if (value.startsWith("{") || value.startsWith("[")) return { label: "inline metadata json" };
  if (value.startsWith("ipfs://")) {
    const cid = value.slice("ipfs://".length);
    return { href: `https://ipfs.io/ipfs/${cid}`, label: truncateAddress(value, 18) };
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

function renderTokensTable(tokens: CollectionTokenResult[]): string {
  if (tokens.length === 0) {
    return `<div class="empty-state">No tokens returned</div>`;
  }

  return `
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
          ${tokens
            .map(
              (token) => {
                const formattedUri = formatCollectionUri(token.uri);
                return `
            <tr>
              <td class="address">${truncateAddress(token.contractAddress, 10)}</td>
              <td class="address">${truncateAddress(token.tokenId, 10)}</td>
              <td>${
                formattedUri.href
                  ? `<a href="${escapeHtml(formattedUri.href)}" target="_blank" rel="noopener noreferrer">${escapeHtml(formattedUri.label)}</a>`
                  : escapeHtml(formattedUri.label)
              }</td>
              <td>
                <img
                  src="${escapeHtml(toLocalImagePath(token.contractAddress, token.tokenId))}"
                  alt="${escapeHtml(`token ${token.tokenId}`)}"
                  loading="lazy"
                  style="width:44px;height:44px;object-fit:cover;border-radius:6px;border:1px solid var(--color-border-light);"
                  onerror="if (this.dataset.fallback) { this.onerror=null; this.src=this.dataset.fallback; } else { this.style.display='none'; }"
                  ${token.imageUrl ? `data-fallback="${escapeHtml(token.imageUrl)}"` : ""}
                />
              </td>
            </tr>
          `;
              }
            )
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderFacetsTable(facets: AttributeFacetCountResult[]): string {
  if (facets.length === 0) {
    return `<div class="empty-state">No facet data</div>`;
  }
  return `
    <div class="table-container">
      <table>
        <thead><tr><th>Trait</th><th>Value</th><th>Count</th></tr></thead>
        <tbody>
          ${facets
            .map((facet) => `<tr><td>${facet.key}</td><td>${facet.value}</td><td>${facet.count}</td></tr>`)
            .join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderTraitsTable(traits: TraitSummaryResult[]): string {
  if (traits.length === 0) {
    return `<div class="empty-state">No trait summary data</div>`;
  }
  return `
    <div class="table-container">
      <table>
        <thead><tr><th>Trait</th><th>Distinct Values</th></tr></thead>
        <tbody>
          ${traits.map((trait) => `<tr><td>${trait.key}</td><td>${trait.valueCount}</td></tr>`).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function renderOverviewBlocks(overview: CollectionOverviewResult | null): string {
  if (!overview || overview.overviews.length === 0) {
    return `<div class="empty-state">No overview data</div>`;
  }
  return overview.overviews
    .map(
      (item) => `
      <div style="margin-bottom: 1rem; border: 1px solid var(--color-border-light); border-radius: var(--radius-sm); padding: 0.75rem;">
        <h4 style="margin-bottom: 0.35rem;">${truncateAddress(item.contractAddress, 12)}</h4>
        <p style="color: var(--color-text-secondary); margin-bottom: 0.5rem;">
          totalHits=${item.totalHits} ${item.nextCursorTokenId ? `(next: ${truncateAddress(item.nextCursorTokenId, 10)})` : ""}
        </p>
        ${renderTokensTable(item.tokens)}
        <h5 style="margin-top: 0.65rem; margin-bottom: 0.35rem;">Trait Summary</h5>
        ${renderTraitsTable(item.traits)}
      </div>
    `
    )
    .join("");
}

export function renderMarketplaceGrpcExplorer(vm: MarketplaceGrpcExplorerViewModel): string {
  return `
    <section class="panel full-width">
      <h2>Marketplace gRPC Explorer</h2>
      <p style="color: var(--color-text-secondary); margin-bottom: 0.75rem;">
        Endpoints: <code>GetCollectionTokens</code>, <code>GetCollectionTraitFacets</code>, <code>GetCollectionOverview</code>
      </p>

      <div class="query-panel" style="margin-bottom: 1rem;">
        <div style="display: grid; grid-template-columns: 160px 1fr; gap: 0.75rem;">
          <div>
            <label for="marketplace-standard">Standard</label>
            <select id="marketplace-standard" style="width: 100%; padding: 0.6rem; border-radius: var(--radius-sm); border: 1px solid var(--color-border);">
              <option value="erc721" ${vm.standard === "erc721" ? "selected" : ""}>ERC721</option>
              <option value="erc1155" ${vm.standard === "erc1155" ? "selected" : ""}>ERC1155</option>
            </select>
          </div>
          <div>
            <label for="marketplace-contract">Collection Contract Address</label>
            <input id="marketplace-contract" type="text" placeholder="0x..." value="${vm.contractAddress}" />
          </div>
        </div>
        <div style="margin-top: 0.75rem;">
          <label for="marketplace-filters">Trait Filters</label>
          <textarea
            id="marketplace-filters"
            rows="5"
            placeholder="Background=Blue|Red&#10;Eyes=Green"
            style="width: 100%; border: 1px solid var(--color-border); border-radius: var(--radius-sm); padding: 0.65rem; font-family: var(--font-mono); font-size: 0.9rem;"
          >${vm.filtersText}</textarea>
        </div>
      </div>

      <div class="results-section">
        <h3>GetCollectionTokens (${vm.tokensTotalHits} matches)</h3>
        <div class="btn-group" style="margin-bottom: 0.75rem;">
          <button class="btn btn-primary" id="marketplace-run-tokens" ${vm.tokensLoading || !vm.contractAddress.trim() ? "disabled" : ""}>
            ${vm.tokensLoading ? "Loading..." : "Run GetCollectionTokens"}
          </button>
          <button class="btn btn-sm" id="marketplace-tokens-prev" ${vm.tokensLoading || !vm.tokensCanPrev ? "disabled" : ""}>Prev</button>
          <button class="btn btn-sm" id="marketplace-tokens-next" ${vm.tokensLoading || !vm.tokensCanNext ? "disabled" : ""}>Next</button>
        </div>
        ${vm.tokensError ? `<div class="error-state">${vm.tokensError}</div>` : ""}
        ${renderTokensTable(vm.tokens)}
        <h4 style="margin-top: 0.75rem; margin-bottom: 0.5rem;">Facet Counts</h4>
        ${renderFacetsTable(vm.tokensFacets)}
      </div>

      <div class="results-section" style="margin-top: 1rem;">
        <h3>GetCollectionTraitFacets (${vm.traitsTotalHits} matches)</h3>
        <div class="btn-group" style="margin-bottom: 0.75rem;">
          <button class="btn btn-primary" id="marketplace-run-traits" ${vm.traitsLoading || !vm.contractAddress.trim() ? "disabled" : ""}>
            ${vm.traitsLoading ? "Loading..." : "Run GetCollectionTraitFacets"}
          </button>
        </div>
        ${vm.traitsError ? `<div class="error-state">${vm.traitsError}</div>` : ""}
        <h4 style="margin-top: 0.25rem; margin-bottom: 0.5rem;">Trait Summary</h4>
        ${renderTraitsTable(vm.traits)}
        <h4 style="margin-top: 0.75rem; margin-bottom: 0.5rem;">Facet Counts</h4>
        ${renderFacetsTable(vm.traitFacets)}
      </div>

      <div class="results-section" style="margin-top: 1rem;">
        <h3>GetCollectionOverview</h3>
        <label for="marketplace-overview-contracts">Overview Contracts (comma, space, or newline separated)</label>
        <textarea
          id="marketplace-overview-contracts"
          rows="3"
          placeholder="0xabc...&#10;0xdef..."
          style="width: 100%; border: 1px solid var(--color-border); border-radius: var(--radius-sm); padding: 0.65rem; font-family: var(--font-mono); font-size: 0.9rem; margin-top: 0.5rem;"
        >${vm.overviewContractsText}</textarea>
        <div class="btn-group" style="margin-top: 0.75rem; margin-bottom: 0.75rem;">
          <button class="btn btn-primary" id="marketplace-run-overview" ${vm.overviewLoading || !vm.overviewCanRun ? "disabled" : ""}>
            ${vm.overviewLoading ? "Loading..." : "Run GetCollectionOverview"}
          </button>
        </div>
        ${vm.overviewError ? `<div class="error-state">${vm.overviewError}</div>` : ""}
        ${renderOverviewBlocks(vm.overview)}
      </div>
    </section>
  `;
}

export function bindMarketplaceGrpcExplorerHandlers(handlers: MarketplaceGrpcExplorerHandlers) {
  document.getElementById("marketplace-standard")?.addEventListener("change", (e) => {
    handlers.onStandardChange((e.target as HTMLSelectElement).value as "erc721" | "erc1155");
  });
  document.getElementById("marketplace-contract")?.addEventListener("input", (e) => {
    handlers.onContractChange((e.target as HTMLInputElement).value);
  });
  document.getElementById("marketplace-filters")?.addEventListener("input", (e) => {
    handlers.onFiltersChange((e.target as HTMLTextAreaElement).value);
  });
  document.getElementById("marketplace-overview-contracts")?.addEventListener("input", (e) => {
    handlers.onOverviewContractsChange((e.target as HTMLTextAreaElement).value);
  });
  document.getElementById("marketplace-run-tokens")?.addEventListener("click", handlers.onRunTokens);
  document.getElementById("marketplace-tokens-prev")?.addEventListener("click", handlers.onTokensPrev);
  document.getElementById("marketplace-tokens-next")?.addEventListener("click", handlers.onTokensNext);
  document.getElementById("marketplace-run-traits")?.addEventListener("click", handlers.onRunTraitFacets);
  document.getElementById("marketplace-run-overview")?.addEventListener("click", handlers.onRunOverview);
}
