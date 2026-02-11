import { ToriiClient } from "@toriijs/sdk";
import {
  SqlSinkClient,
  LogSinkClient,
  ToriiClient as GeneratedToriiClient
} from "../generated";

export const SERVER_URL = "http://localhost:8080";

export function createClient(url: string = SERVER_URL) {
  return new ToriiClient(url, {
    sql: SqlSinkClient,
    log: LogSinkClient,
    torii: GeneratedToriiClient,
  });
}

export type AppClient = ReturnType<typeof createClient>;
