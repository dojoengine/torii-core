import { hexToBytes, bytesToHex, formatU256 } from "./utils";
import type { TokensClient } from "./client";
import {
  Erc20GetStatsRequest, Erc20GetStatsResponse,
  Erc20GetTransfersRequest, Erc20GetTransfersResponse,
  Erc20GetBalanceRequest, Erc20GetBalanceResponse,
  Erc20GetTokenMetadataRequest, Erc20GetTokenMetadataResponse,
  Erc721GetStatsRequest, Erc721GetStatsResponse,
  Erc721GetTransfersRequest, Erc721GetTransfersResponse,
  Erc721GetTokenMetadataRequest, Erc721GetTokenMetadataResponse,
  Erc1155GetStatsRequest, Erc1155GetStatsResponse,
  Erc1155GetTransfersRequest, Erc1155GetTransfersResponse,
  Erc1155GetBalanceRequest, Erc1155GetBalanceResponse,
  Erc1155GetTokenMetadataRequest, Erc1155GetTokenMetadataResponse,
} from "./schemas";

export interface TokenQuery {
  contractAddress: string;
  wallet: string;
}

export interface Erc1155TokenQuery extends TokenQuery {
  tokenId: string;
}

export interface TokenMetadataResult {
  token: string;
  name?: string;
  symbol?: string;
  decimals?: number;
  totalSupply?: string;
}

export interface BalanceResult {
  balance: string;
  balanceRaw: string;
  lastBlock: number;
}

export interface TransferResult {
  token: string;
  from: string;
  to: string;
  amount?: string;
  blockNumber: number;
  txHash: string;
  timestamp: number;
}

export interface Erc1155TransferResult extends TransferResult {
  tokenId: string;
  operator: string;
  isBatch: boolean;
}

export interface StatsResult {
  totalTransfers: number;
  totalApprovals?: number;
  uniqueTokens: number;
  uniqueNfts?: number;
  uniqueTokenIds?: number;
  latestBlock: number;
}

export async function getErc20Stats(
  client: TokensClient
): Promise<StatsResult> {
  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetStats",
    {},
    Erc20GetStatsRequest,
    Erc20GetStatsResponse
  );

  return {
    totalTransfers: Number(response.totalTransfers ?? 0),
    totalApprovals: Number(response.totalApprovals ?? 0),
    uniqueTokens: Number(response.uniqueTokens ?? 0),
    latestBlock: Number(response.latestBlock ?? 0),
  };
}

export async function getErc721Stats(
  client: TokensClient
): Promise<StatsResult> {
  const response = await client.call(
    "/torii.sinks.erc721.Erc721/GetStats",
    {},
    Erc721GetStatsRequest,
    Erc721GetStatsResponse
  );

  return {
    totalTransfers: Number(response.totalTransfers ?? 0),
    uniqueTokens: Number(response.uniqueTokens ?? 0),
    uniqueNfts: Number(response.uniqueNfts ?? 0),
    latestBlock: Number(response.latestBlock ?? 0),
  };
}

export async function getErc1155Stats(
  client: TokensClient
): Promise<StatsResult> {
  const response = await client.call(
    "/torii.sinks.erc1155.Erc1155/GetStats",
    {},
    Erc1155GetStatsRequest,
    Erc1155GetStatsResponse
  );

  return {
    totalTransfers: Number(response.totalTransfers ?? 0),
    uniqueTokens: Number(response.uniqueTokens ?? 0),
    uniqueTokenIds: Number(response.uniqueTokenIds ?? 0),
    latestBlock: Number(response.latestBlock ?? 0),
  };
}

export async function getErc20TokenMetadata(
  client: TokensClient,
  contractAddress?: string
): Promise<TokenMetadataResult[]> {
  const request: Record<string, unknown> = {};
  if (contractAddress) {
    request.token = hexToBytes(contractAddress);
  }

  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetTokenMetadata",
    request,
    Erc20GetTokenMetadataRequest,
    Erc20GetTokenMetadataResponse
  );

  const tokens = response.tokens;
  const list = Array.isArray(tokens) ? tokens : tokens ? [tokens] : [];

  return list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    name: t.name as string | undefined,
    symbol: t.symbol as string | undefined,
    decimals: t.decimals != null ? Number(t.decimals) : undefined,
  }));
}

export async function getErc20Balance(
  client: TokensClient,
  query: TokenQuery
): Promise<BalanceResult | null> {
  if (!query.contractAddress || !query.wallet) {
    return null;
  }

  // Fetch decimals from token metadata
  const metadata = await getErc20TokenMetadata(client, query.contractAddress);
  const decimals = metadata[0]?.decimals;

  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetBalance",
    {
      token: hexToBytes(query.contractAddress),
      wallet: hexToBytes(query.wallet),
    },
    Erc20GetBalanceRequest,
    Erc20GetBalanceResponse
  );

  return {
    balance: formatU256(response.balance as string | Uint8Array | undefined, decimals),
    balanceRaw: typeof response.balance === "string" ? response.balance : "",
    lastBlock: Number(response.lastBlock) || 0,
  };
}

export async function getErc20Transfers(
  client: TokensClient,
  query: TokenQuery,
  limit = 50
): Promise<TransferResult[]> {
  const filter: Record<string, unknown> = {};
  if (query.wallet) filter.wallet = hexToBytes(query.wallet);
  if (query.contractAddress) filter.tokens = [hexToBytes(query.contractAddress)];

  // Fetch decimals if filtering by a specific contract
  let decimals: number | undefined;
  if (query.contractAddress) {
    const metadata = await getErc20TokenMetadata(client, query.contractAddress);
    decimals = metadata[0]?.decimals;
  }

  const response = await client.call(
    "/torii.sinks.erc20.Erc20/GetTransfers",
    { filter, limit },
    Erc20GetTransfersRequest,
    Erc20GetTransfersResponse
  );

  const transfers = response.transfers;
  const list = Array.isArray(transfers) ? transfers : transfers ? [transfers] : [];

  return list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    from: bytesToHex(t.from as string | Uint8Array | undefined),
    to: bytesToHex(t.to as string | Uint8Array | undefined),
    amount: formatU256(t.amount as string | Uint8Array | undefined, decimals),
    blockNumber: Number(t.blockNumber ?? 0),
    txHash: bytesToHex(t.txHash as string | Uint8Array | undefined),
    timestamp: Number(t.timestamp ?? 0),
  }));
}

export async function getErc721Transfers(
  client: TokensClient,
  query: TokenQuery,
  limit = 50
): Promise<TransferResult[]> {
  const filter: Record<string, unknown> = {};
  if (query.wallet) filter.wallet = hexToBytes(query.wallet);
  if (query.contractAddress) filter.tokens = [hexToBytes(query.contractAddress)];

  const response = await client.call(
    "/torii.sinks.erc721.Erc721/GetTransfers",
    { filter, limit },
    Erc721GetTransfersRequest,
    Erc721GetTransfersResponse
  );

  const transfers = response.transfers;
  const list = Array.isArray(transfers) ? transfers : transfers ? [transfers] : [];

  return list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    tokenId: bytesToHex(t.tokenId as string | Uint8Array | undefined),
    from: bytesToHex(t.from as string | Uint8Array | undefined),
    to: bytesToHex(t.to as string | Uint8Array | undefined),
    blockNumber: Number(t.blockNumber ?? 0),
    txHash: bytesToHex(t.txHash as string | Uint8Array | undefined),
    timestamp: Number(t.timestamp ?? 0),
  }));
}

export async function getErc721TokenMetadata(
  client: TokensClient,
  contractAddress?: string
): Promise<TokenMetadataResult[]> {
  const request: Record<string, unknown> = {};
  if (contractAddress) {
    request.token = hexToBytes(contractAddress);
  }

  const response = await client.call(
    "/torii.sinks.erc721.Erc721/GetTokenMetadata",
    request,
    Erc721GetTokenMetadataRequest,
    Erc721GetTokenMetadataResponse
  );

  const tokens = response.tokens;
  const list = Array.isArray(tokens) ? tokens : tokens ? [tokens] : [];

  return list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    name: t.name as string | undefined,
    symbol: t.symbol as string | undefined,
    totalSupply: t.totalSupply ? formatU256(t.totalSupply as string | Uint8Array | undefined) : undefined,
  }));
}

export async function getErc1155TokenMetadata(
  client: TokensClient,
  contractAddress?: string
): Promise<TokenMetadataResult[]> {
  const request: Record<string, unknown> = {};
  if (contractAddress) {
    request.token = hexToBytes(contractAddress);
  }

  const response = await client.call(
    "/torii.sinks.erc1155.Erc1155/GetTokenMetadata",
    request,
    Erc1155GetTokenMetadataRequest,
    Erc1155GetTokenMetadataResponse
  );

  const tokens = response.tokens;
  const list = Array.isArray(tokens) ? tokens : tokens ? [tokens] : [];

  return list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    name: t.name as string | undefined,
    symbol: t.symbol as string | undefined,
    totalSupply: t.totalSupply ? formatU256(t.totalSupply as string | Uint8Array | undefined) : undefined,
  }));
}

export async function getErc1155Balance(
  client: TokensClient,
  query: Erc1155TokenQuery
): Promise<BalanceResult> {
  const response = await client.call(
    "/torii.sinks.erc1155.Erc1155/GetBalance",
    {
      contract: hexToBytes(query.contractAddress),
      wallet: hexToBytes(query.wallet),
      tokenId: hexToBytes(query.tokenId),
    },
    Erc1155GetBalanceRequest,
    Erc1155GetBalanceResponse
  );

  return {
    balance: formatU256(response.balance as string | Uint8Array | undefined),
    balanceRaw: typeof response.balance === "string" ? response.balance : "",
    lastBlock: Number(response.lastBlock) || 0,
  };
}

export async function getErc1155Transfers(
  client: TokensClient,
  query: TokenQuery,
  limit = 50
): Promise<Erc1155TransferResult[]> {
  const filter: Record<string, unknown> = {};
  if (query.wallet) filter.wallet = hexToBytes(query.wallet);
  if (query.contractAddress) filter.tokens = [hexToBytes(query.contractAddress)];

  const response = await client.call(
    "/torii.sinks.erc1155.Erc1155/GetTransfers",
    { filter, limit },
    Erc1155GetTransfersRequest,
    Erc1155GetTransfersResponse
  );

  const transfers = response.transfers;
  const list = Array.isArray(transfers) ? transfers : transfers ? [transfers] : [];

  return list.map((t: Record<string, unknown>) => ({
    token: bytesToHex(t.token as string | Uint8Array | undefined),
    operator: bytesToHex(t.operator as string | Uint8Array | undefined),
    from: bytesToHex(t.from as string | Uint8Array | undefined),
    to: bytesToHex(t.to as string | Uint8Array | undefined),
    tokenId: bytesToHex(t.tokenId as string | Uint8Array | undefined),
    amount: formatU256(t.amount as string | Uint8Array | undefined),
    blockNumber: Number(t.blockNumber ?? 0),
    txHash: bytesToHex(t.txHash as string | Uint8Array | undefined),
    timestamp: Number(t.timestamp ?? 0),
    isBatch: Boolean(t.isBatch),
  }));
}
