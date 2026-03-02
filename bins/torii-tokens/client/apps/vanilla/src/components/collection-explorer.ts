import { truncateAddress, type AttributeFacetCountResult } from "@torii-tokens/shared";

export interface CollectionExplorerViewModel {
  standard: "erc721" | "erc1155";
  contractAddress: string;
  filtersText: string;
  tokenIds: string[];
  facets: AttributeFacetCountResult[];
  totalHits: number;
  loading: boolean;
  error: string | null;
  canPrev: boolean;
  canNext: boolean;
}

export interface CollectionExplorerHandlers {
  onStandardChange: (value: "erc721" | "erc1155") => void;
  onContractChange: (value: string) => void;
  onFiltersChange: (value: string) => void;
  onRun: () => void;
  onPrev: () => void;
  onNext: () => void;
}

export function renderCollectionExplorer(vm: CollectionExplorerViewModel): string {
  const tokenRows = vm.tokenIds
    .map((tokenId) => `<tr><td class="address">${truncateAddress(tokenId, 10)}</td></tr>`)
    .join("");
  const facetRows = vm.facets
    .map((facet) => `<tr><td>${facet.key}</td><td>${facet.value}</td><td>${facet.count}</td></tr>`)
    .join("");

  return `
    <section class="panel full-width collection-panel">
      <h2>Collection Explorer</h2>
      <p class="collection-hint">
        Filter format: one line per key, ex: <code>Background=Blue|Red</code>
      </p>
      <div class="query-panel collection-controls">
        <form id="collection-form">
          <div class="collection-form-grid">
            <div>
              <label for="collection-standard">Standard</label>
              <select
                id="collection-standard"
                class="input select-input"
              >
                <option value="erc721" ${vm.standard === "erc721" ? "selected" : ""}>ERC721</option>
                <option value="erc1155" ${vm.standard === "erc1155" ? "selected" : ""}>ERC1155</option>
              </select>
            </div>
            <div>
              <label for="collection-contract">Contract Address</label>
              <input
                id="collection-contract"
                type="text"
                placeholder="0x..."
                value="${vm.contractAddress}"
              />
            </div>
          </div>
          <div class="collection-filters-wrap">
            <label for="collection-filters">Trait Filters</label>
            <textarea
              id="collection-filters"
              rows="6"
              placeholder="Background=Blue|Red&#10;Eyes=Green"
              class="input code-input"
            >${vm.filtersText}</textarea>
          </div>
          <div class="btn-group collection-run-row">
            <button
              type="submit"
              class="btn btn-primary"
              id="collection-run"
              ${vm.loading || !vm.contractAddress.trim() ? "disabled" : ""}
            >
              ${vm.loading ? "Loading..." : "Run Filters"}
            </button>
          </div>
        </form>
      </div>

      ${vm.error ? `<div class="error-state">${vm.error}</div>` : ""}

      <div class="results-section">
        <h3>Matched Token IDs (${vm.totalHits})</h3>
        ${vm.tokenIds.length === 0
          ? `<div class="empty-state">No matches</div>`
          : `<div class="table-container"><table><thead><tr><th>Token ID</th></tr></thead><tbody>${tokenRows}</tbody></table></div>`}
        <div class="btn-group pagination-row">
          <button class="btn btn-sm" id="collection-prev" ${vm.canPrev && !vm.loading ? "" : "disabled"}>Prev</button>
          <button class="btn btn-sm" id="collection-next" ${vm.canNext && !vm.loading ? "" : "disabled"}>Next</button>
        </div>
      </div>

      <div class="results-section results-section-spaced">
        <h3>Facet Counts</h3>
        ${vm.facets.length === 0
          ? `<div class="empty-state">No facet data</div>`
          : `<div class="table-container"><table><thead><tr><th>Trait</th><th>Value</th><th>Count</th></tr></thead><tbody>${facetRows}</tbody></table></div>`}
      </div>
    </section>
  `;
}

export function bindCollectionExplorerHandlers(handlers: CollectionExplorerHandlers) {
  document.getElementById("collection-standard")?.addEventListener("change", (e) => {
    handlers.onStandardChange((e.target as HTMLSelectElement).value as "erc721" | "erc1155");
  });
  document.getElementById("collection-contract")?.addEventListener("input", (e) => {
    handlers.onContractChange((e.target as HTMLInputElement).value);
  });
  document.getElementById("collection-filters")?.addEventListener("input", (e) => {
    handlers.onFiltersChange((e.target as HTMLTextAreaElement).value);
  });
  document.getElementById("collection-form")?.addEventListener("submit", (e) => {
    e.preventDefault();
    handlers.onRun();
  });
  document.getElementById("collection-prev")?.addEventListener("click", handlers.onPrev);
  document.getElementById("collection-next")?.addEventListener("click", handlers.onNext);
}
