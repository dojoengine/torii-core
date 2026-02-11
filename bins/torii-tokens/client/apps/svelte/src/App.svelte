<script lang="ts">
  import { onMount, onDestroy } from "svelte";
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
    getErc20TokenMetadata,
    getErc721TokenMetadata,
    getErc1155TokenMetadata,
    type BalanceResult,
    type TransferResult,
    type TokenMetadataResult,
  } from "@torii-tokens/shared";

  import StatusPanel from "./components/StatusPanel.svelte";
  import TokenPanel from "./components/TokenPanel.svelte";
  import UpdatesFeed from "./components/UpdatesFeed.svelte";
  import QueryFilters from "./components/QueryFilters.svelte";
  import QueryResults from "./components/QueryResults.svelte";

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

  const client = createTokensClient(SERVER_URL);
  const clientId = generateClientId();

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
  let queryError = $state<string | null>(null);
  let queryErc20Balance = $state<BalanceResult | null>(null);
  let queryErc20Transfers = $state<TransferResult[]>([]);

  async function handleQuery(contractAddress: string, wallet: string) {
    queryContractAddress = contractAddress;
    queryWallet = wallet;
    queryLoading = true;
    queryError = null;

    try {
      const [balanceResult, transfers] = await Promise.all([
        getErc20Balance(client, { contractAddress, wallet }),
        getErc20Transfers(client, { contractAddress, wallet }, 50),
      ]);
      queryErc20Balance = balanceResult;
      queryErc20Transfers = transfers;
    } catch (err) {
      console.error("Query failed:", err);
      queryError = err instanceof Error ? err.message : "Query failed";
    } finally {
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

  async function loadMetadata() {
    try {
      const [m20, m721, m1155] = await Promise.all([
        getErc20TokenMetadata(client),
        getErc721TokenMetadata(client),
        getErc1155TokenMetadata(client),
      ]);
      erc20Metadata = m20;
      erc721Metadata = m721;
      erc1155Metadata = m1155;
    } catch (err) {
      console.error("Failed to load metadata:", err);
    }
  }

  async function loadTransfers() {
    const emptyQuery = { contractAddress: "", wallet: "" };
    try {
      const [t20, t721, t1155] = await Promise.all([
        getErc20Transfers(client, emptyQuery, 20),
        getErc721Transfers(client, emptyQuery, 20),
        getErc1155Transfers(client, emptyQuery, 20),
      ]);
      erc20Transfers = t20.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        amount: t.amount,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      }));
      erc721Transfers = t721.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        tokenId: (t as Record<string, unknown>).tokenId as string | undefined,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      }));
      erc1155Transfers = t1155.map((t, i) => ({
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

  async function subscribe() {
    try {
      unsubscribe = await client.subscribeTopics(
        clientId,
        [
          { topic: "erc20.transfer" },
          { topic: "erc721.transfer" },
          { topic: "erc1155.transfer" },
        ],
        (update: Update) => {
          updates = [update, ...updates].slice(0, 50);
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

  onMount(() => {
    checkHealth();
    loadStats();
    loadTransfers();
    loadMetadata();
  });

  onDestroy(() => {
    disconnect();
  });
</script>

<div class="container">
  <header>
    <h1>Torii Tokens - Svelte</h1>
    <p class="subtitle">ERC20 / ERC721 / ERC1155 Token Indexer</p>
  </header>

  <StatusPanel
    {connected}
    {clientId}
    serverUrl={SERVER_URL}
    updateCount={updates.length}
    onSubscribe={subscribe}
    onDisconnect={disconnect}
  />

  <QueryFilters onQuery={handleQuery} loading={queryLoading} />

  <QueryResults
    contractAddress={queryContractAddress}
    wallet={queryWallet}
    erc20Balance={queryErc20Balance}
    erc20Transfers={queryErc20Transfers}
    loading={queryLoading}
    error={queryError}
  />

  <div class="panels">
    <TokenPanel
      title="ERC20 Tokens"
      tokenType="erc20"
      stats={erc20Stats}
      transfers={erc20Transfers}
      metadata={erc20Metadata}
      showAmount={true}
    />

    <TokenPanel
      title="ERC721 NFTs"
      tokenType="erc721"
      stats={erc721Stats}
      transfers={erc721Transfers}
      metadata={erc721Metadata}
      showAmount={false}
    />

    <TokenPanel
      title="ERC1155 Multi-Tokens"
      tokenType="erc1155"
      stats={erc1155Stats}
      transfers={erc1155Transfers}
      metadata={erc1155Metadata}
      showAmount={false}
    />

    <UpdatesFeed
      {updates}
      {connected}
      onClear={clearUpdates}
    />
  </div>
</div>
