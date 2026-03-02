import { useCallback, useMemo, useState } from "react";
import {
  getErc721CollectionTokens,
  getErc721CollectionTraitFacets,
  getErc721CollectionOverview,
  getErc1155CollectionTokens,
  getErc1155CollectionTraitFacets,
  getErc1155CollectionOverview,
  truncateAddress,
  type AttributeFilterInput,
  type CollectionOverviewResult,
  type CollectionTokenResult,
  type TokensClient,
  type AttributeFacetCountResult,
  type TraitSummaryResult,
} from "@torii-tokens/shared";

interface MarketplaceGrpcExplorerProps {
  client: TokensClient;
}

const PAGE_SIZE = 50;
const FACET_LIMIT = 300;
const OVERVIEW_LIMIT = 20;

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
      return { href: parsed.toString(), label: truncateAddress(parsed.toString(), 18) };
    }
    return { label: truncateAddress(value, 18) };
  } catch {
    return { label: truncateAddress(value, 18) };
  }
}

function parseTraitFilters(input: string): AttributeFilterInput[] {
  return input
    .split("\n")
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => {
      const [rawKey, rawValues = ""] = line.split("=", 2);
      const key = rawKey.trim();
      const values = rawValues
        .split("|")
        .map((v) => v.trim())
        .filter(Boolean);
      return { key, values };
    })
    .filter((f) => f.key.length > 0 && f.values.length > 0);
}

function parseContractAddresses(input: string): string[] {
  return input
    .split(/[\n, ]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function TraitsTable({ traits }: { traits: TraitSummaryResult[] }) {
  if (traits.length === 0) {
    return <div className="empty-state">No trait summary data</div>;
  }

  return (
    <div className="table-container">
      <table>
        <thead>
          <tr>
            <th>Trait</th>
            <th>Distinct Values</th>
          </tr>
        </thead>
        <tbody>
          {traits.map((trait) => (
            <tr key={trait.key}>
              <td>{trait.key}</td>
              <td>{trait.valueCount}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function FacetsTable({ facets }: { facets: AttributeFacetCountResult[] }) {
  if (facets.length === 0) {
    return <div className="empty-state">No facet data</div>;
  }

  return (
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
  );
}

function TokensTable({ tokens }: { tokens: CollectionTokenResult[] }) {
  if (tokens.length === 0) {
    return <div className="empty-state">No tokens returned</div>;
  }

  return (
    <div className="table-container">
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
          {tokens.map((token) => (
            <tr key={`${token.contractAddress}:${token.tokenId}`}>
              <td className="address">{truncateAddress(token.contractAddress, 10)}</td>
              <td className="address">{truncateAddress(token.tokenId, 10)}</td>
              <td>
                {(() => {
                  const formattedUri = formatCollectionUri(token.uri);
                  return formattedUri.href
                    ? <a href={formattedUri.href} target="_blank" rel="noreferrer">{formattedUri.label}</a>
                    : formattedUri.label;
                })()}
              </td>
              <td>
                <img
                  src={toLocalImagePath(token.contractAddress, token.tokenId)}
                  alt={`token ${token.tokenId}`}
                  loading="lazy"
                  style={{ width: 44, height: 44, objectFit: "cover", borderRadius: 6, border: "1px solid var(--color-border-light)" }}
                  onError={(e) => {
                    const el = e.currentTarget;
                    if (token.imageUrl && el.src !== token.imageUrl) {
                      el.src = token.imageUrl;
                      return;
                    }
                    el.style.display = "none";
                  }}
                />
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function MarketplaceGrpcExplorer({ client }: MarketplaceGrpcExplorerProps) {
  const [standard, setStandard] = useState<"erc721" | "erc1155">("erc721");
  const [contractAddress, setContractAddress] = useState("");
  const [overviewContractsText, setOverviewContractsText] = useState("");
  const [filtersText, setFiltersText] = useState("");

  const [tokensLoading, setTokensLoading] = useState(false);
  const [tokensError, setTokensError] = useState<string | null>(null);
  const [tokens, setTokens] = useState<CollectionTokenResult[]>([]);
  const [tokensFacets, setTokensFacets] = useState<AttributeFacetCountResult[]>([]);
  const [tokensTotalHits, setTokensTotalHits] = useState(0);
  const [tokensCursor, setTokensCursor] = useState<string | undefined>(undefined);
  const [tokensNextCursor, setTokensNextCursor] = useState<string | undefined>(undefined);
  const [tokensHistory, setTokensHistory] = useState<(string | undefined)[]>([]);

  const [traitsLoading, setTraitsLoading] = useState(false);
  const [traitsError, setTraitsError] = useState<string | null>(null);
  const [traitFacets, setTraitFacets] = useState<AttributeFacetCountResult[]>([]);
  const [traitSummary, setTraitSummary] = useState<TraitSummaryResult[]>([]);
  const [traitsTotalHits, setTraitsTotalHits] = useState(0);

  const [overviewLoading, setOverviewLoading] = useState(false);
  const [overviewError, setOverviewError] = useState<string | null>(null);
  const [overview, setOverview] = useState<CollectionOverviewResult | null>(null);

  const parsedFilters = useMemo(() => parseTraitFilters(filtersText), [filtersText]);

  const runCollectionTokens = useCallback(async (cursorTokenId?: string) => {
    if (!contractAddress.trim()) return;
    setTokensLoading(true);
    setTokensError(null);
    try {
      const result = standard === "erc721"
        ? await getErc721CollectionTokens(client, {
            contractAddress: contractAddress.trim(),
            filters: parsedFilters,
            cursorTokenId,
            limit: PAGE_SIZE,
            includeFacets: true,
            facetLimit: FACET_LIMIT,
            includeImages: true,
          })
        : await getErc1155CollectionTokens(client, {
            contractAddress: contractAddress.trim(),
            filters: parsedFilters,
            cursorTokenId,
            limit: PAGE_SIZE,
            includeFacets: true,
            facetLimit: FACET_LIMIT,
            includeImages: true,
          });

      setTokens(result.tokens);
      setTokensFacets(result.facets);
      setTokensTotalHits(result.totalHits);
      setTokensCursor(cursorTokenId);
      setTokensNextCursor(result.nextCursorTokenId);
    } catch (err) {
      setTokensError(err instanceof Error ? err.message : "GetCollectionTokens failed");
    } finally {
      setTokensLoading(false);
    }
  }, [client, contractAddress, parsedFilters, standard]);

  const runCollectionTokensFresh = useCallback(async () => {
    setTokensHistory([]);
    setTokensCursor(undefined);
    setTokensNextCursor(undefined);
    await runCollectionTokens(undefined);
  }, [runCollectionTokens]);

  const navigateCollectionTokens = useCallback(async (direction: "next" | "prev") => {
    if (direction === "next") {
      if (!tokensNextCursor) return;
      setTokensHistory([...tokensHistory, tokensCursor]);
      await runCollectionTokens(tokensNextCursor);
      return;
    }
    if (tokensHistory.length === 0) return;
    const prevCursor = tokensHistory[tokensHistory.length - 1];
    setTokensHistory(tokensHistory.slice(0, -1));
    await runCollectionTokens(prevCursor);
  }, [runCollectionTokens, tokensCursor, tokensHistory, tokensNextCursor]);

  const runTraitFacets = useCallback(async () => {
    if (!contractAddress.trim()) return;
    setTraitsLoading(true);
    setTraitsError(null);
    try {
      const result = standard === "erc721"
        ? await getErc721CollectionTraitFacets(client, {
            contractAddress: contractAddress.trim(),
            filters: parsedFilters,
            facetLimit: FACET_LIMIT,
          })
        : await getErc1155CollectionTraitFacets(client, {
            contractAddress: contractAddress.trim(),
            filters: parsedFilters,
            facetLimit: FACET_LIMIT,
          });

      setTraitFacets(result.facets);
      setTraitSummary(result.traits);
      setTraitsTotalHits(result.totalHits);
    } catch (err) {
      setTraitsError(err instanceof Error ? err.message : "GetCollectionTraitFacets failed");
    } finally {
      setTraitsLoading(false);
    }
  }, [client, contractAddress, parsedFilters, standard]);

  const runOverview = useCallback(async () => {
    const contractAddresses = parseContractAddresses(overviewContractsText);
    if (contractAddresses.length === 0) return;
    setOverviewLoading(true);
    setOverviewError(null);
    try {
      const contractFilters = parsedFilters.length === 0
        ? undefined
        : contractAddresses.map((address) => ({ contractAddress: address, filters: parsedFilters }));
      const result = standard === "erc721"
        ? await getErc721CollectionOverview(client, {
            contractAddresses,
            perContractLimit: OVERVIEW_LIMIT,
            includeFacets: true,
            facetLimit: FACET_LIMIT,
            includeImages: true,
            contractFilters,
          })
        : await getErc1155CollectionOverview(client, {
            contractAddresses,
            perContractLimit: OVERVIEW_LIMIT,
            includeFacets: true,
            facetLimit: FACET_LIMIT,
            includeImages: true,
            contractFilters,
          });
      setOverview(result);
    } catch (err) {
      setOverviewError(err instanceof Error ? err.message : "GetCollectionOverview failed");
    } finally {
      setOverviewLoading(false);
    }
  }, [client, overviewContractsText, parsedFilters, standard]);

  return (
    <section className="panel full-width">
      <h2>Marketplace gRPC Explorer</h2>
      <p style={{ color: "var(--color-text-secondary)", marginBottom: "0.75rem" }}>
        Endpoints: <code>GetCollectionTokens</code>, <code>GetCollectionTraitFacets</code>, <code>GetCollectionOverview</code>
      </p>

      <div className="query-panel" style={{ marginBottom: "1rem" }}>
        <div style={{ display: "grid", gridTemplateColumns: "160px 1fr", gap: "0.75rem" }}>
          <div>
            <label>Standard</label>
            <select
              value={standard}
              onChange={(e) => setStandard(e.target.value as "erc721" | "erc1155")}
              style={{ width: "100%", padding: "0.6rem", borderRadius: "var(--radius-sm)", border: "1px solid var(--color-border)" }}
            >
              <option value="erc721">ERC721</option>
              <option value="erc1155">ERC1155</option>
            </select>
          </div>
          <div>
            <label>Collection Contract Address</label>
            <input
              type="text"
              placeholder="0x..."
              value={contractAddress}
              onChange={(e) => setContractAddress(e.target.value)}
            />
          </div>
        </div>
        <div style={{ marginTop: "0.75rem" }}>
          <label>Trait Filters</label>
          <textarea
            value={filtersText}
            onChange={(e) => setFiltersText(e.target.value)}
            rows={5}
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
      </div>

      <div className="results-section">
        <h3>GetCollectionTokens ({tokensTotalHits} matches)</h3>
        <div className="btn-group" style={{ marginBottom: "0.75rem" }}>
          <button className="btn btn-primary" onClick={() => void runCollectionTokensFresh()} disabled={tokensLoading || !contractAddress.trim()}>
            {tokensLoading ? "Loading..." : "Run GetCollectionTokens"}
          </button>
          <button className="btn btn-sm" onClick={() => void navigateCollectionTokens("prev")} disabled={tokensLoading || tokensHistory.length === 0}>Prev</button>
          <button className="btn btn-sm" onClick={() => void navigateCollectionTokens("next")} disabled={tokensLoading || tokensNextCursor == null}>Next</button>
        </div>
        {tokensError ? <div className="error-state">{tokensError}</div> : null}
        <TokensTable tokens={tokens} />
        <h4 style={{ marginTop: "0.75rem", marginBottom: "0.5rem" }}>Facet Counts</h4>
        <FacetsTable facets={tokensFacets} />
      </div>

      <div className="results-section" style={{ marginTop: "1rem" }}>
        <h3>GetCollectionTraitFacets ({traitsTotalHits} matches)</h3>
        <div className="btn-group" style={{ marginBottom: "0.75rem" }}>
          <button className="btn btn-primary" onClick={() => void runTraitFacets()} disabled={traitsLoading || !contractAddress.trim()}>
            {traitsLoading ? "Loading..." : "Run GetCollectionTraitFacets"}
          </button>
        </div>
        {traitsError ? <div className="error-state">{traitsError}</div> : null}
        <h4 style={{ marginTop: "0.25rem", marginBottom: "0.5rem" }}>Trait Summary</h4>
        <TraitsTable traits={traitSummary} />
        <h4 style={{ marginTop: "0.75rem", marginBottom: "0.5rem" }}>Facet Counts</h4>
        <FacetsTable facets={traitFacets} />
      </div>

      <div className="results-section" style={{ marginTop: "1rem" }}>
        <h3>GetCollectionOverview</h3>
        <label>Overview Contracts (comma, space, or newline separated)</label>
        <textarea
          value={overviewContractsText}
          onChange={(e) => setOverviewContractsText(e.target.value)}
          rows={3}
          placeholder={"0xabc...\n0xdef..."}
          style={{
            width: "100%",
            border: "1px solid var(--color-border)",
            borderRadius: "var(--radius-sm)",
            padding: "0.65rem",
            fontFamily: "var(--font-mono)",
            fontSize: "0.9rem",
            marginTop: "0.5rem",
          }}
        />
        <div className="btn-group" style={{ marginTop: "0.75rem", marginBottom: "0.75rem" }}>
          <button className="btn btn-primary" onClick={() => void runOverview()} disabled={overviewLoading || parseContractAddresses(overviewContractsText).length === 0}>
            {overviewLoading ? "Loading..." : "Run GetCollectionOverview"}
          </button>
        </div>
        {overviewError ? <div className="error-state">{overviewError}</div> : null}
        {!overview || overview.overviews.length === 0 ? (
          <div className="empty-state">No overview data</div>
        ) : (
          overview.overviews.map((item) => (
            <div key={item.contractAddress} style={{ marginBottom: "1rem", border: "1px solid var(--color-border-light)", borderRadius: "var(--radius-sm)", padding: "0.75rem" }}>
              <h4 style={{ marginBottom: "0.35rem" }}>{truncateAddress(item.contractAddress, 12)}</h4>
              <p style={{ color: "var(--color-text-secondary)", marginBottom: "0.5rem" }}>
                totalHits={item.totalHits} {item.nextCursorTokenId ? `(next: ${truncateAddress(item.nextCursorTokenId, 10)})` : ""}
              </p>
              <TokensTable tokens={item.tokens} />
              <h5 style={{ marginTop: "0.65rem", marginBottom: "0.35rem" }}>Trait Summary</h5>
              <TraitsTable traits={item.traits} />
            </div>
          ))
        )}
      </div>
    </section>
  );
}
