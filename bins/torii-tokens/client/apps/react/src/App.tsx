import { useEffect, useState, useCallback, useRef } from "react";
import {
  createTokensClient,
  SERVER_URL,
  generateClientId,
  getErc20Balance,
  getErc20Transfers,
  getErc20Stats,
  getErc20TokenMetadata,
  getErc721Stats,
  getErc721Transfers,
  getErc721TokenMetadata,
  getErc1155Stats,
  getErc1155Transfers,
  getErc1155TokenMetadata,
  type BalanceResult,
  type TransferResult,
  type TokenMetadataResult,
} from "@torii-tokens/shared";
import StatusPanel from "./components/StatusPanel";
import TokenPanel from "./components/TokenPanel";
import UpdatesFeed from "./components/UpdatesFeed";
import QueryFilters from "./components/QueryFilters";
import QueryResults from "./components/QueryResults";

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

export default function App() {
  const [connected, setConnected] = useState(false);
  const [updates, setUpdates] = useState<Update[]>([]);
  const unsubscribeRef = useRef<(() => void) | null>(null);

  const [erc20Stats, setErc20Stats] = useState<Stats | null>(null);
  const [erc20Transfers, setErc20Transfers] = useState<Transfer[]>([]);
  const [erc721Stats, setErc721Stats] = useState<Stats | null>(null);
  const [erc721Transfers, setErc721Transfers] = useState<Transfer[]>([]);
  const [erc1155Stats, setErc1155Stats] = useState<Stats | null>(null);
  const [erc1155Transfers, setErc1155Transfers] = useState<Transfer[]>([]);

  const [erc20Metadata, setErc20Metadata] = useState<TokenMetadataResult[]>([]);
  const [erc721Metadata, setErc721Metadata] = useState<TokenMetadataResult[]>([]);
  const [erc1155Metadata, setErc1155Metadata] = useState<TokenMetadataResult[]>([]);

  const [queryContractAddress, setQueryContractAddress] = useState("");
  const [queryWallet, setQueryWallet] = useState("");
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [queryErc20Balance, setQueryErc20Balance] = useState<BalanceResult | null>(null);
  const [queryErc20Transfers, setQueryErc20Transfers] = useState<TransferResult[]>([]);

  const loadStats = useCallback(async () => {
    try {
      const [s20, s721, s1155] = await Promise.all([
        getErc20Stats(client),
        getErc721Stats(client),
        getErc1155Stats(client),
      ]);
      setErc20Stats({
        totalTransfers: s20.totalTransfers,
        totalApprovals: s20.totalApprovals,
        uniqueTokens: s20.uniqueTokens,
        uniqueAccounts: 0,
      });
      setErc721Stats({
        totalTransfers: s721.totalTransfers,
        uniqueTokens: s721.uniqueTokens,
        uniqueAccounts: s721.uniqueNfts ?? 0,
      });
      setErc1155Stats({
        totalTransfers: s1155.totalTransfers,
        uniqueTokens: s1155.uniqueTokens,
        uniqueAccounts: s1155.uniqueTokenIds ?? 0,
      });
    } catch (err) {
      console.error("Failed to load stats:", err);
      setErc20Stats({ totalTransfers: 0, totalApprovals: 0, uniqueTokens: 0, uniqueAccounts: 0 });
      setErc721Stats({ totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 });
      setErc1155Stats({ totalTransfers: 0, uniqueTokens: 0, uniqueAccounts: 0 });
    }
  }, []);

  const loadMetadata = useCallback(async () => {
    try {
      const [m20, m721, m1155] = await Promise.all([
        getErc20TokenMetadata(client),
        getErc721TokenMetadata(client),
        getErc1155TokenMetadata(client),
      ]);
      setErc20Metadata(m20);
      setErc721Metadata(m721);
      setErc1155Metadata(m1155);
    } catch (err) {
      console.error("Failed to load metadata:", err);
    }
  }, []);

  const loadTransfers = useCallback(async () => {
    const emptyQuery = { contractAddress: "", wallet: "" };
    try {
      const [t20, t721, t1155] = await Promise.all([
        getErc20Transfers(client, emptyQuery, 20),
        getErc721Transfers(client, emptyQuery, 20),
        getErc1155Transfers(client, emptyQuery, 20),
      ]);
      setErc20Transfers(t20.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        amount: t.amount,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      })));
      setErc721Transfers(t721.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        tokenId: (t as Record<string, unknown>).tokenId as string | undefined,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      })));
      setErc1155Transfers(t1155.map((t, i) => ({
        id: `${t.txHash}-${i}`,
        token: t.token,
        from: t.from,
        to: t.to,
        amount: t.amount,
        tokenId: t.tokenId,
        blockNumber: t.blockNumber,
        timestamp: t.timestamp,
      })));
    } catch (err) {
      console.error("Failed to load transfers:", err);
    }
  }, []);

  const handleQuery = useCallback(async (contractAddress: string, wallet: string) => {
    setQueryContractAddress(contractAddress);
    setQueryWallet(wallet);
    setQueryLoading(true);
    setQueryError(null);

    try {
      const [balanceResult, transfers] = await Promise.all([
        getErc20Balance(client, { contractAddress, wallet }),
        getErc20Transfers(client, { contractAddress, wallet }, 50),
      ]);
      setQueryErc20Balance(balanceResult);
      setQueryErc20Transfers(transfers);
    } catch (err) {
      console.error("Query failed:", err);
      setQueryError(err instanceof Error ? err.message : "Query failed");
    } finally {
      setQueryLoading(false);
    }
  }, []);

  const subscribe = useCallback(async () => {
    try {
      unsubscribeRef.current = await client.subscribeTopics(
        clientId,
        [
          { topic: "erc20.transfer" },
          { topic: "erc721.transfer" },
          { topic: "erc1155.transfer" },
        ],
        (update) => {
          setUpdates((prev) => [update as Update, ...prev].slice(0, 50));
        },
        (err) => {
          console.error("Subscription error:", err);
          setConnected(false);
        },
        () => {
          setConnected(true);
        }
      );
    } catch (err) {
      console.error("Failed to subscribe:", err);
    }
  }, []);

  const disconnect = useCallback(() => {
    if (unsubscribeRef.current) {
      unsubscribeRef.current();
      unsubscribeRef.current = null;
    }
    setConnected(false);
  }, []);

  const clearUpdates = useCallback(() => {
    setUpdates([]);
  }, []);

  useEffect(() => {
    loadStats();
    loadTransfers();
    loadMetadata();
    return () => {
      disconnect();
    };
  }, [loadStats, loadTransfers, loadMetadata, disconnect]);

  return (
    <div className="container">
      <header>
        <h1>Torii Tokens - React</h1>
        <p className="subtitle">ERC20 / ERC721 / ERC1155 Token Indexer</p>
      </header>

      <StatusPanel
        connected={connected}
        clientId={clientId}
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

      <div className="panels">
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
          updates={updates}
          connected={connected}
          onClear={clearUpdates}
        />
      </div>
    </div>
  );
}
