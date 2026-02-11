import { ToriiClient, GrpcTransport, setSchemaRegistry, type MessageSchema } from "@toriijs/sdk";
import { schemas } from "./schemas";

export const SERVER_URL = "http://localhost:3000";

export interface TokensClient {
  subscribeTopics: (
    clientId: string,
    topics: Array<{ topic: string; filters?: Record<string, string> }>,
    onUpdate: (update: unknown) => void,
    onError?: (error: Error) => void,
    onConnected?: () => void
  ) => Promise<() => void>;
  call: <T = Record<string, unknown>>(
    path: string,
    request: Record<string, unknown>,
    requestSchema: MessageSchema,
    responseSchema: MessageSchema
  ) => Promise<T>;
  disconnect: () => void;
  getVersion: () => Promise<{ version: string; buildTime: string }>;
}

export function createTokensClient(url: string = SERVER_URL): TokensClient {
  setSchemaRegistry(schemas);
  const baseClient = new ToriiClient(url, {});
  const transport = new GrpcTransport(url);

  return {
    subscribeTopics: baseClient.subscribeTopics.bind(baseClient),
    disconnect: baseClient.disconnect.bind(baseClient),
    getVersion: baseClient.getVersion.bind(baseClient),
    call: <T = Record<string, unknown>>(
      path: string,
      request: Record<string, unknown>,
      requestSchema: MessageSchema,
      responseSchema: MessageSchema
    ): Promise<T> => {
      return transport.unaryCall<T>(path, request, { requestSchema, responseSchema });
    },
  };
}
