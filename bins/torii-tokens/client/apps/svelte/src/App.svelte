<script lang="ts">
  import { onMount, onDestroy } from "svelte";
  import {
    createTokensClient,
    SERVER_URL,
    formatTimestamp,
    getUpdateTypeName,
    generateClientId,
    getErc20Stats,
    getErc721Stats,
    getErc721TokensByAttributesPage,
    getErc721CollectionTokens,
    getErc721CollectionTraitFacets,
    getErc721CollectionOverview,
    getErc1155Stats,
    getErc1155TokensByAttributesPage,
    getErc1155CollectionTokens,
    getErc1155CollectionTraitFacets,
    getErc1155CollectionOverview,
    getErc20TransfersPage,
    getErc721TransfersPage,
    getErc1155TransfersPage,
    getErc20BalancesPage,
    getErc20TokenMetadataPage,
    getErc721TokenMetadataPage,
    getErc1155TokenMetadataPage,
    type TransferCursorResult,
    type TokenBalanceResult,
    type TransferResult,
    type TokenMetadataResult,
    type AttributeFilterInput,
    type AttributeFacetCountResult,
    type CollectionTokenResult,
    type TraitSummaryResult,
    type CollectionOverviewResult,
  } from "@torii-tokens/shared";

  import StatusPanel from "./components/StatusPanel.svelte";
  import TokenPanel from "./components/TokenPanel.svelte";
  import UpdatesFeed from "./components/UpdatesFeed.svelte";
  import QueryFilters from "./components/QueryFilters.svelte";
  import QueryResults from "./components/QueryResults.svelte";
  import CollectionExplorer from "./components/CollectionExplorer.svelte";
  import MarketplaceGrpcExplorer from "./components/MarketplaceGrpcExplorer.svelte";

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
    value?: string;
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

  const client = createTokensClient(SERVER_URL);
  const clientId = generateClientId();
  const PAGE_SIZE = 100;

  let connected = $state(false);
  let updates = $state<Update[]>([]);
  let unsubscribe: (() => void) | null = null;

  let erc20Stats = $state<Stats | null>(null);
  let erc20Transfers = $state<Transfer[]>([]);
  let erc721Stats = $state<Stats | null>(null);
  let erc721Transfers = $state<Transfer[]>([]);
  let erc1155Stats = $state<Stats | null>(null);
  let erc1155Transfers = $state<Transfer[]>([]);

  let erc20Metadata = $state<TokenMetadataResult[]>([]);
  let erc721Metadata = $state<TokenMetadataResult[]>([]);
  let erc1155Metadata = $state<TokenMetadataResult[]>([]);

  let queryContractAddress = $state("");
  let queryWallet = $state("");
  let queryLoading = $state(false);
  let queryBalancesLoading = $state(false);
  let queryTransfersLoading = $state(false);
  let queryError = $state<string | null>(null);
  let queryErc20Balances = $state<TokenBalanceResult[]>([]);
  let queryErc20Transfers = $state<TransferResult[]>([]);
  let erc20MetadataLoading = $state(false);
  let erc721MetadataLoading = $state(false);
  let erc1155MetadataLoading = $state(false);
  let erc20TransfersLoading = $state(false);
  let erc721TransfersLoading = $state(false);
  let erc1155TransfersLoading = $state(false);
  let erc20TransfersHistory = $state<(TransferCursorResult | undefined)[]>([]);
  let erc20TransfersCursor = $state<TransferCursorResult | undefined>(undefined);
  let erc20TransfersNext = $state<TransferCursorResult | undefined>(undefined);
  let erc721TransfersHistory = $state<(TransferCursorResult | undefined)[]>([]);
  let erc721TransfersCursor = $state<TransferCursorResult | undefined>(undefined);
  let erc721TransfersNext = $state<TransferCursorResult | undefined>(undefined);
  let erc1155TransfersHistory = $state<(TransferCursorResult | undefined)[]>([]);
  let erc1155TransfersCursor = $state<TransferCursorResult | undefined>(undefined);
  let erc1155TransfersNext = $state<TransferCursorResult | undefined>(undefined);
  let erc20MetadataHistory = $state<(string | undefined)[]>([]);
  let erc20MetadataCursor = $state<string | undefined>(undefined);
  let erc20MetadataNext = $state<string | undefined>(undefined);
  let erc721MetadataHistory = $state<(string | undefined)[]>([]);
  let erc721MetadataCursor = $state<string | undefined>(undefined);
  let erc721MetadataNext = $state<string | undefined>(undefined);
  let erc1155MetadataHistory = $state<(string | undefined)[]>([]);
  let erc1155MetadataCursor = $state<string | undefined>(undefined);
  let erc1155MetadataNext = $state<string | undefined>(undefined);
  let queryBalancesHistory = $state<(number | undefined)[]>([]);
  let queryBalancesCursor = $state<number | undefined>(undefined);
  let queryBalancesNext = $state<number | undefined>(undefined);
  let queryTransfersHistory = $state<(TransferCursorResult | undefined)[]>([]);
  let queryTransfersCursor = $state<TransferCursorResult | undefined>(undefined);
  let queryTransfersNext = $state<TransferCursorResult | undefined>(undefined);
  let collectionStandard = $state<"erc721" | "erc1155">("erc721");
  let collectionContractAddress = $state("");
  let collectionFiltersText = $state("");
  let collectionTokenIds = $state<string[]>([]);
  let collectionFacets = $state<AttributeFacetCountResult[]>([]);
  let collectionTotalHits = $state(0);
  let collectionCursor = $state<string | undefined>(undefined);
  let collectionNextCursor = $state<string | undefined>(undefined);
  let collectionHistory = $state<(string | undefined)[]>([]);
  let collectionLoading = $state(false);
  let collectionError = $state<string | null>(null);
  let marketplaceStandard = $state<"erc721" | "erc1155">("erc721");
  let marketplaceContractAddress = $state("");
  let marketplaceFiltersText = $state("");
  let marketplaceOverviewContractsText = $state("");
  let marketplaceTokens = $state<CollectionTokenResult[]>([]);
  let marketplaceTokensFacets = $state<AttributeFacetCountResult[]>([]);
  let marketplaceTokensTotalHits = $state(0);
  let marketplaceTokensCursor = $state<string | undefined>(undefined);
  let marketplaceTokensNextCursor = $state<string | undefined>(undefined);
  let marketplaceTokensHistory = $state<(string | undefined)[]>([]);
  let marketplaceTokensLoading = $state(false);
  let marketplaceTokensError = $state<string | null>(null);
  let marketplaceTraitFacets = $state<AttributeFacetCountResult[]>([]);
  let marketplaceTraits = $state<TraitSummaryResult[]>([]);
  let marketplaceTraitsTotalHits = $state(0);
  let marketplaceTraitsLoading = $state(false);
  let marketplaceTraitsError = $state<string | null>(null);
  let marketplaceOverview = $state<CollectionOverviewResult | null>(null);
  let marketplaceOverviewLoading = $state(false);
  let marketplaceOverviewError = $state<string | null>(null);
  let activePage = $state<"dashboard" | "collection" | "marketplaceGrpc">("dashboard");

  async function handleQuery(contractAddress: string, wallet: string) {
    queryContractAddress = contractAddress;
    queryWallet = wallet;
    queryBalancesHistory = [];
    queryTransfersHistory = [];
    queryBalancesCursor = undefined;
    queryTransfersCursor = undefined;
    queryLoading = true;
    queryBalancesLoading = true;
    queryTransfersLoading = true;
    queryError = null;

    try {
      const [balances, transfers] = await Promise.all([
        getErc20BalancesPage(client, { contractAddress, wallet, limit: PAGE_SIZE }),
        getErc20TransfersPage(client, { contractAddress, wallet, limit: PAGE_SIZE }),
      ]);
      queryErc20Balances = balances.items;
      queryBalancesNext = balances.nextCursor;
      queryErc20Transfers = transfers.items;
      queryTransfersNext = transfers.nextCursor;
    } catch (err) {
      console.error("Query failed:", err);
      queryError = err instanceof Error ? err.message : "Query failed";
    } finally {
      queryBalancesLoading = false;
      queryTransfersLoading = false;
      queryLoading = false;
    }
  }

  async function checkHealth() {
    try {
      const version = await client.getVersion();
      console.log("Server version:", version);
    } catch (err) {
      console.error("Health check failed:", err);
    }
  }

  async function loadStats() {
    try {
      const [s20, s721, s1155] = await Promise.all([
        getErc20Stats(client),
        getErc721Stats(client),
        getErc1155Stats(client),
      ]);
      erc20Stats = {
        totalTransfers: s20.totalTransfers,
        totalApprovals: s20.totalApprovals,
        uniqueTokens: s20.uniqueTokens,
        uniqueAccounts: 0,
      };
      erc721Stats = {
        totalTransfers: s721.totalTransfers,
        uniqueTokens: s721.uniqueTokens,
        uniqueAccounts: s721.uniqueNfts ?? 0,
      };
      erc1155Stats = {
        totalTransfers: s1155.totalTransfers,
        uniqueTokens: s1155.uniqueTokens,
        uniqueAccounts: s1155.uniqueTokenIds ?? 0,
      };
    } catch (err) {
      console.error("Failed to load stats:", err);
      erc20Stats = { totalTransfers: 0, totalApprovals: 0, uniqueTokens: 0, uniqueAccounts: 0 };
      erc721Stats = { totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 };
      erc1155Stats = { totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 };
    }
  }

  async function loadMetadata(tokenType: "erc20" | "erc721" | "erc1155", cursor?: string) {
    try {
      if (tokenType === "erc20") {
        erc20MetadataLoading = true;
        const page = await getErc20TokenMetadataPage(client, { cursor, limit: PAGE_SIZE });
        erc20Metadata = page.items;
        erc20MetadataNext = page.nextCursor;
        return;
      }
      if (tokenType === "erc721") {
        erc721MetadataLoading = true;
        const page = await getErc721TokenMetadataPage(client, { cursor, limit: PAGE_SIZE });
        erc721Metadata = page.items;
        erc721MetadataNext = page.nextCursor;
        return;
      }
      erc1155MetadataLoading = true;
      const page = await getErc1155TokenMetadataPage(client, { cursor, limit: PAGE_SIZE });
      erc1155Metadata = page.items;
      erc1155MetadataNext = page.nextCursor;
    } catch (err) {
      console.error("Failed to load metadata:", err);
    } finally {
      if (tokenType === "erc20") erc20MetadataLoading = false;
      if (tokenType === "erc721") erc721MetadataLoading = false;
      if (tokenType === "erc1155") erc1155MetadataLoading = false;
    }
  }

  async function loadTransfers(tokenType: "erc20" | "erc721" | "erc1155", cursor?: TransferCursorResult) {
    const emptyQuery = { contractAddress: "", wallet: "" };
    try {
      if (tokenType === "erc20") {
        erc20TransfersLoading = true;
        const page = await getErc20TransfersPage(client, { ...emptyQuery, cursor, limit: PAGE_SIZE });
        erc20TransfersNext = page.nextCursor;
        erc20Transfers = page.items.map((t, i) => ({
          id: `${t.txHash}-${i}`,
          token: t.token,
          from: t.from,
          to: t.to,
          amount: t.amount,
          blockNumber: t.blockNumber,
          timestamp: t.timestamp,
        }));
        return;
      }
      if (tokenType === "erc721") {
        erc721TransfersLoading = true;
        const page = await getErc721TransfersPage(client, { ...emptyQuery, cursor, limit: PAGE_SIZE });
        erc721TransfersNext = page.nextCursor;
        erc721Transfers = page.items.map((t, i) => ({
          id: `${t.txHash}-${i}`,
          token: t.token,
          from: t.from,
          to: t.to,
          tokenId: (t as unknown as Record<string, unknown>).tokenId as string | undefined,
          blockNumber: t.blockNumber,
          timestamp: t.timestamp,
        }));
        return;
      }
      erc1155TransfersLoading = true;
      const page = await getErc1155TransfersPage(client, { ...emptyQuery, cursor, limit: PAGE_SIZE });
      erc1155TransfersNext = page.nextCursor;
      erc1155Transfers = page.items.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        value: t.value,
        amount: t.amount,
        tokenId: t.tokenId,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      }));
    } catch (err) {
      console.error("Failed to load transfers:", err);
    } finally {
      if (tokenType === "erc20") erc20TransfersLoading = false;
      if (tokenType === "erc721") erc721TransfersLoading = false;
      if (tokenType === "erc1155") erc1155TransfersLoading = false;
    }
  }

  async function navigateDashboardTransfers(tokenType: "erc20" | "erc721" | "erc1155", dir: "next" | "prev") {
    if (tokenType === "erc20") {
      const target = dir === "next" ? erc20TransfersNext : erc20TransfersHistory[erc20TransfersHistory.length - 1];
      if (!target && dir === "next") return;
      erc20TransfersHistory = dir === "next" ? [...erc20TransfersHistory, erc20TransfersCursor] : erc20TransfersHistory.slice(0, -1);
      erc20TransfersCursor = target;
      await loadTransfers("erc20", target);
      return;
    }
    if (tokenType === "erc721") {
      const target = dir === "next" ? erc721TransfersNext : erc721TransfersHistory[erc721TransfersHistory.length - 1];
      if (!target && dir === "next") return;
      erc721TransfersHistory = dir === "next" ? [...erc721TransfersHistory, erc721TransfersCursor] : erc721TransfersHistory.slice(0, -1);
      erc721TransfersCursor = target;
      await loadTransfers("erc721", target);
      return;
    }
    const target = dir === "next" ? erc1155TransfersNext : erc1155TransfersHistory[erc1155TransfersHistory.length - 1];
    if (!target && dir === "next") return;
    erc1155TransfersHistory = dir === "next" ? [...erc1155TransfersHistory, erc1155TransfersCursor] : erc1155TransfersHistory.slice(0, -1);
    erc1155TransfersCursor = target;
    await loadTransfers("erc1155", target);
  }

  async function navigateDashboardMetadata(tokenType: "erc20" | "erc721" | "erc1155", dir: "next" | "prev") {
    if (tokenType === "erc20") {
      const target = dir === "next" ? erc20MetadataNext : erc20MetadataHistory[erc20MetadataHistory.length - 1];
      if (!target && dir === "next") return;
      erc20MetadataHistory = dir === "next" ? [...erc20MetadataHistory, erc20MetadataCursor] : erc20MetadataHistory.slice(0, -1);
      erc20MetadataCursor = target;
      await loadMetadata("erc20", target);
      return;
    }
    if (tokenType === "erc721") {
      const target = dir === "next" ? erc721MetadataNext : erc721MetadataHistory[erc721MetadataHistory.length - 1];
      if (!target && dir === "next") return;
      erc721MetadataHistory = dir === "next" ? [...erc721MetadataHistory, erc721MetadataCursor] : erc721MetadataHistory.slice(0, -1);
      erc721MetadataCursor = target;
      await loadMetadata("erc721", target);
      return;
    }
    const target = dir === "next" ? erc1155MetadataNext : erc1155MetadataHistory[erc1155MetadataHistory.length - 1];
    if (!target && dir === "next") return;
    erc1155MetadataHistory = dir === "next" ? [...erc1155MetadataHistory, erc1155MetadataCursor] : erc1155MetadataHistory.slice(0, -1);
    erc1155MetadataCursor = target;
    await loadMetadata("erc1155", target);
  }

  async function navigateQueryBalances(dir: "next" | "prev") {
    const target = dir === "next" ? queryBalancesNext : queryBalancesHistory[queryBalancesHistory.length - 1];
    if (target == null && dir === "next") return;
    queryBalancesLoading = true;
    queryError = null;
    try {
      queryBalancesHistory = dir === "next" ? [...queryBalancesHistory, queryBalancesCursor] : queryBalancesHistory.slice(0, -1);
      queryBalancesCursor = target;
      const page = await getErc20BalancesPage(client, {
        contractAddress: queryContractAddress,
        wallet: queryWallet,
        cursor: target,
        limit: PAGE_SIZE,
      });
      queryErc20Balances = page.items;
      queryBalancesNext = page.nextCursor;
    } catch (err) {
      console.error("Balance pagination failed:", err);
      queryError = err instanceof Error ? err.message : "Balance pagination failed";
    } finally {
      queryBalancesLoading = false;
    }
  }

  async function navigateQueryTransfers(dir: "next" | "prev") {
    const target = dir === "next" ? queryTransfersNext : queryTransfersHistory[queryTransfersHistory.length - 1];
    if (!target && dir === "next") return;
    queryTransfersLoading = true;
    queryError = null;
    try {
      queryTransfersHistory = dir === "next" ? [...queryTransfersHistory, queryTransfersCursor] : queryTransfersHistory.slice(0, -1);
      queryTransfersCursor = target;
      const page = await getErc20TransfersPage(client, {
        contractAddress: queryContractAddress,
        wallet: queryWallet,
        cursor: target,
        limit: PAGE_SIZE,
      });
      queryErc20Transfers = page.items;
      queryTransfersNext = page.nextCursor;
    } catch (err) {
      console.error("Transfer pagination failed:", err);
      queryError = err instanceof Error ? err.message : "Transfer pagination failed";
    } finally {
      queryTransfersLoading = false;
    }
  }

  async function subscribe() {
    try {
      unsubscribe = await client.subscribeTopics(
        clientId,
        [
          { topic: "erc20.transfer" },
          { topic: "erc20.metadata" },
          { topic: "erc721.transfer" },
          { topic: "erc721.metadata" },
          { topic: "erc1155.transfer" },
          { topic: "erc1155.metadata" },
          { topic: "erc1155.uri" },
        ],
        (update) => {
          updates = [update as Update, ...updates].slice(0, 50);
        },
        (err: Error) => {
          console.error("Subscription error:", err);
          connected = false;
        },
        () => {
          connected = true;
        }
      );
    } catch (err) {
      console.error("Failed to subscribe:", err);
    }
  }

  function disconnect() {
    if (unsubscribe) {
      unsubscribe();
      unsubscribe = null;
    }
    connected = false;
  }

  function clearUpdates() {
    updates = [];
  }

  function parseCollectionFilters(input: string): AttributeFilterInput[] {
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

  async function runCollectionQuery(cursorTokenId?: string) {
    if (!collectionContractAddress.trim()) return;
    collectionLoading = true;
    collectionError = null;
    try {
      const filters = parseCollectionFilters(collectionFiltersText);
      const result = collectionStandard === "erc721"
        ? await getErc721TokensByAttributesPage(client, {
            contractAddress: collectionContractAddress.trim(),
            filters,
            cursorTokenId,
            limit: PAGE_SIZE,
            includeFacets: true,
            facetLimit: 300,
          })
        : await getErc1155TokensByAttributesPage(client, {
            contractAddress: collectionContractAddress.trim(),
            filters,
            cursorTokenId,
            limit: PAGE_SIZE,
            includeFacets: true,
            facetLimit: 300,
          });

      collectionTokenIds = result.tokenIds;
      collectionFacets = result.facets;
      collectionTotalHits = result.totalHits;
      collectionNextCursor = result.nextCursorTokenId;
      collectionCursor = cursorTokenId;
    } catch (err) {
      console.error("Collection query failed:", err);
      collectionError = err instanceof Error ? err.message : "Collection query failed";
    } finally {
      collectionLoading = false;
    }
  }

  async function runCollectionQueryFresh() {
    collectionHistory = [];
    collectionCursor = undefined;
    collectionNextCursor = undefined;
    await runCollectionQuery(undefined);
  }

  async function navigateCollection(dir: "next" | "prev") {
    if (dir === "next") {
      if (!collectionNextCursor) return;
      collectionHistory = [...collectionHistory, collectionCursor];
      await runCollectionQuery(collectionNextCursor);
      return;
    }
    if (collectionHistory.length === 0) return;
    const prevCursor = collectionHistory[collectionHistory.length - 1];
    collectionHistory = collectionHistory.slice(0, -1);
    await runCollectionQuery(prevCursor);
  }

  function parseOverviewContractAddresses(input: string): string[] {
    return input
      .split(/[\n, ]+/)
      .map((item) => item.trim())
      .filter(Boolean);
  }

  async function runMarketplaceCollectionTokens(cursorTokenId?: string) {
    if (!marketplaceContractAddress.trim()) return;
    marketplaceTokensLoading = true;
    marketplaceTokensError = null;
    try {
      const filters = parseCollectionFilters(marketplaceFiltersText);
      const result = marketplaceStandard === "erc721"
        ? await getErc721CollectionTokens(client, {
            contractAddress: marketplaceContractAddress.trim(),
            filters,
            cursorTokenId,
            limit: 50,
            includeFacets: true,
            facetLimit: 300,
            includeImages: true,
          })
        : await getErc1155CollectionTokens(client, {
            contractAddress: marketplaceContractAddress.trim(),
            filters,
            cursorTokenId,
            limit: 50,
            includeFacets: true,
            facetLimit: 300,
            includeImages: true,
          });
      marketplaceTokens = result.tokens;
      marketplaceTokensFacets = result.facets;
      marketplaceTokensTotalHits = result.totalHits;
      marketplaceTokensCursor = cursorTokenId;
      marketplaceTokensNextCursor = result.nextCursorTokenId;
    } catch (err) {
      console.error("GetCollectionTokens failed:", err);
      marketplaceTokensError = err instanceof Error ? err.message : "GetCollectionTokens failed";
    } finally {
      marketplaceTokensLoading = false;
    }
  }

  async function runMarketplaceCollectionTokensFresh() {
    marketplaceTokensHistory = [];
    marketplaceTokensCursor = undefined;
    marketplaceTokensNextCursor = undefined;
    await runMarketplaceCollectionTokens(undefined);
  }

  async function navigateMarketplaceCollectionTokens(dir: "next" | "prev") {
    if (dir === "next") {
      if (!marketplaceTokensNextCursor) return;
      marketplaceTokensHistory = [...marketplaceTokensHistory, marketplaceTokensCursor];
      await runMarketplaceCollectionTokens(marketplaceTokensNextCursor);
      return;
    }
    if (marketplaceTokensHistory.length === 0) return;
    const prevCursor = marketplaceTokensHistory[marketplaceTokensHistory.length - 1];
    marketplaceTokensHistory = marketplaceTokensHistory.slice(0, -1);
    await runMarketplaceCollectionTokens(prevCursor);
  }

  async function runMarketplaceTraitFacets() {
    if (!marketplaceContractAddress.trim()) return;
    marketplaceTraitsLoading = true;
    marketplaceTraitsError = null;
    try {
      const filters = parseCollectionFilters(marketplaceFiltersText);
      const result = marketplaceStandard === "erc721"
        ? await getErc721CollectionTraitFacets(client, {
            contractAddress: marketplaceContractAddress.trim(),
            filters,
            facetLimit: 300,
          })
        : await getErc1155CollectionTraitFacets(client, {
            contractAddress: marketplaceContractAddress.trim(),
            filters,
            facetLimit: 300,
          });
      marketplaceTraitFacets = result.facets;
      marketplaceTraits = result.traits;
      marketplaceTraitsTotalHits = result.totalHits;
    } catch (err) {
      console.error("GetCollectionTraitFacets failed:", err);
      marketplaceTraitsError = err instanceof Error ? err.message : "GetCollectionTraitFacets failed";
    } finally {
      marketplaceTraitsLoading = false;
    }
  }

  async function runMarketplaceOverview() {
    const contractAddresses = parseOverviewContractAddresses(marketplaceOverviewContractsText);
    if (contractAddresses.length === 0) return;
    marketplaceOverviewLoading = true;
    marketplaceOverviewError = null;
    try {
      const parsedFilters = parseCollectionFilters(marketplaceFiltersText);
      const contractFilters = parsedFilters.length === 0
        ? undefined
        : contractAddresses.map((address) => ({ contractAddress: address, filters: parsedFilters }));
      const result = marketplaceStandard === "erc721"
        ? await getErc721CollectionOverview(client, {
            contractAddresses,
            perContractLimit: 20,
            includeFacets: true,
            facetLimit: 300,
            includeImages: true,
            contractFilters,
          })
        : await getErc1155CollectionOverview(client, {
            contractAddresses,
            perContractLimit: 20,
            includeFacets: true,
            facetLimit: 300,
            includeImages: true,
            contractFilters,
          });
      marketplaceOverview = result;
    } catch (err) {
      console.error("GetCollectionOverview failed:", err);
      marketplaceOverviewError = err instanceof Error ? err.message : "GetCollectionOverview failed";
    } finally {
      marketplaceOverviewLoading = false;
    }
  }

  onMount(() => {
    checkHealth();
    loadStats();
    void Promise.all([
      loadTransfers("erc20"),
      loadTransfers("erc721"),
      loadTransfers("erc1155"),
      loadMetadata("erc20"),
      loadMetadata("erc721"),
      loadMetadata("erc1155"),
    ]);
  });

  onDestroy(() => {
    disconnect();
  });
</script>

<div class="container">
  <header>
    <h1>Torii Tokens - Svelte</h1>
    <p class="subtitle">ERC20 / ERC721 / ERC1155 Token Indexer</p>
    <div class="btn-group" style="justify-content: center; margin-top: 0.75rem;">
      <button class={`btn ${activePage === "dashboard" ? "btn-primary" : ""}`} onclick={() => (activePage = "dashboard")}>
        Dashboard
      </button>
      <button class={`btn ${activePage === "collection" ? "btn-primary" : ""}`} onclick={() => (activePage = "collection")}>
        Collection Explorer
      </button>
      <button class={`btn ${activePage === "marketplaceGrpc" ? "btn-primary" : ""}`} onclick={() => (activePage = "marketplaceGrpc")}>
        Marketplace gRPC
      </button>
    </div>
  </header>

  <StatusPanel
    {connected}
    {clientId}
    serverUrl={SERVER_URL}
    updateCount={updates.length}
    onSubscribe={subscribe}
    onDisconnect={disconnect}
  />

  {#if activePage === "dashboard"}
    <QueryFilters onQuery={handleQuery} loading={queryLoading} />

    <QueryResults
      contractAddress={queryContractAddress}
      wallet={queryWallet}
      erc20Balances={queryErc20Balances}
      erc20Transfers={queryErc20Transfers}
      loading={queryLoading}
      error={queryError}
      onBalancesPrev={() => navigateQueryBalances("prev")}
      onBalancesNext={() => navigateQueryBalances("next")}
      balancesCanPrev={queryBalancesHistory.length > 0}
      balancesCanNext={queryBalancesNext != null}
      onTransfersPrev={() => navigateQueryTransfers("prev")}
      onTransfersNext={() => navigateQueryTransfers("next")}
      transfersCanPrev={queryTransfersHistory.length > 0}
      transfersCanNext={queryTransfersNext != null}
      balancesLoading={queryBalancesLoading}
      transfersLoading={queryTransfersLoading}
    />

    <div class="panels">
      <TokenPanel
        title="ERC20 Tokens"
        tokenType="erc20"
        stats={erc20Stats}
        transfers={erc20Transfers}
        metadata={erc20Metadata}
        showAmount={true}
        onMetadataPrev={() => navigateDashboardMetadata("erc20", "prev")}
        onMetadataNext={() => navigateDashboardMetadata("erc20", "next")}
        metadataCanPrev={erc20MetadataHistory.length > 0}
        metadataCanNext={erc20MetadataNext != null}
        onTransfersPrev={() => navigateDashboardTransfers("erc20", "prev")}
        onTransfersNext={() => navigateDashboardTransfers("erc20", "next")}
        transfersCanPrev={erc20TransfersHistory.length > 0}
        transfersCanNext={erc20TransfersNext != null}
        metadataLoading={erc20MetadataLoading}
        transfersLoading={erc20TransfersLoading}
      />

      <TokenPanel
        title="ERC721 NFTs"
        tokenType="erc721"
        stats={erc721Stats}
        transfers={erc721Transfers}
        metadata={erc721Metadata}
        showAmount={false}
        onMetadataPrev={() => navigateDashboardMetadata("erc721", "prev")}
        onMetadataNext={() => navigateDashboardMetadata("erc721", "next")}
        metadataCanPrev={erc721MetadataHistory.length > 0}
        metadataCanNext={erc721MetadataNext != null}
        onTransfersPrev={() => navigateDashboardTransfers("erc721", "prev")}
        onTransfersNext={() => navigateDashboardTransfers("erc721", "next")}
        transfersCanPrev={erc721TransfersHistory.length > 0}
        transfersCanNext={erc721TransfersNext != null}
        metadataLoading={erc721MetadataLoading}
        transfersLoading={erc721TransfersLoading}
      />

      <TokenPanel
        title="ERC1155 Multi-Tokens"
        tokenType="erc1155"
        stats={erc1155Stats}
        transfers={erc1155Transfers}
        metadata={erc1155Metadata}
        showAmount={true}
        onMetadataPrev={() => navigateDashboardMetadata("erc1155", "prev")}
        onMetadataNext={() => navigateDashboardMetadata("erc1155", "next")}
        metadataCanPrev={erc1155MetadataHistory.length > 0}
        metadataCanNext={erc1155MetadataNext != null}
        onTransfersPrev={() => navigateDashboardTransfers("erc1155", "prev")}
        onTransfersNext={() => navigateDashboardTransfers("erc1155", "next")}
        transfersCanPrev={erc1155TransfersHistory.length > 0}
        transfersCanNext={erc1155TransfersNext != null}
        metadataLoading={erc1155MetadataLoading}
        transfersLoading={erc1155TransfersLoading}
      />

      <UpdatesFeed
        {updates}
        {connected}
        onClear={clearUpdates}
      />
    </div>
  {:else if activePage === "collection"}
    <CollectionExplorer
      standard={collectionStandard}
      contractAddress={collectionContractAddress}
      filtersText={collectionFiltersText}
      loading={collectionLoading}
      error={collectionError}
      totalHits={collectionTotalHits}
      tokenIds={collectionTokenIds}
      facets={collectionFacets}
      canNext={collectionNextCursor != null}
      canPrev={collectionHistory.length > 0}
      onStandardChange={(v) => (collectionStandard = v)}
      onContractAddressChange={(v) => (collectionContractAddress = v)}
      onFiltersTextChange={(v) => (collectionFiltersText = v)}
      onRun={() => runCollectionQueryFresh()}
      onNext={() => navigateCollection("next")}
      onPrev={() => navigateCollection("prev")}
    />
  {:else}
    <MarketplaceGrpcExplorer
      standard={marketplaceStandard}
      contractAddress={marketplaceContractAddress}
      filtersText={marketplaceFiltersText}
      overviewContractsText={marketplaceOverviewContractsText}
      tokens={marketplaceTokens}
      tokensFacets={marketplaceTokensFacets}
      tokensTotalHits={marketplaceTokensTotalHits}
      tokensLoading={marketplaceTokensLoading}
      tokensError={marketplaceTokensError}
      tokensCanPrev={marketplaceTokensHistory.length > 0}
      tokensCanNext={marketplaceTokensNextCursor != null}
      traits={marketplaceTraits}
      traitFacets={marketplaceTraitFacets}
      traitsTotalHits={marketplaceTraitsTotalHits}
      traitsLoading={marketplaceTraitsLoading}
      traitsError={marketplaceTraitsError}
      overview={marketplaceOverview}
      overviewLoading={marketplaceOverviewLoading}
      overviewError={marketplaceOverviewError}
      overviewCanRun={parseOverviewContractAddresses(marketplaceOverviewContractsText).length > 0}
      onStandardChange={(v) => (marketplaceStandard = v)}
      onContractAddressChange={(v) => (marketplaceContractAddress = v)}
      onFiltersTextChange={(v) => (marketplaceFiltersText = v)}
      onOverviewContractsTextChange={(v) => (marketplaceOverviewContractsText = v)}
      onRunTokens={() => runMarketplaceCollectionTokensFresh()}
      onTokensPrev={() => navigateMarketplaceCollectionTokens("prev")}
      onTokensNext={() => navigateMarketplaceCollectionTokens("next")}
      onRunTraitFacets={() => runMarketplaceTraitFacets()}
      onRunOverview={() => runMarketplaceOverview()}
    />
  {/if}
</div>
