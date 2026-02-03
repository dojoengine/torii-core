import { ToriiClient } from "@toriijs/sdk";
import { SqlSinkClient, LogSinkClient, ToriiClient as GeneratedToriiClient } from "./generated";

const SERVER_URL = process.env.TORII_URL || "http://localhost:8080";

async function main() {
  console.log("Connecting to Torii server at", SERVER_URL);

  const client = new ToriiClient(SERVER_URL, {
    sql: SqlSinkClient,
    log: LogSinkClient,
    torii: GeneratedToriiClient,
  });

  // Get server version
  const version = await client.getVersion();
  console.log("Server version:", version);

  // List available topics
  const topics = await client.listTopics();
  console.log("Available topics:", topics);

  // --- SQL Sink ---
  console.log("\n--- SQL Sink ---");

  const schema = await client.sql.getSchema({});
  console.log("Schema:", schema);

  const result = await client.sql.query({
    query: "SELECT * FROM sql_operation LIMIT 3",
  });
  console.log("Query result:", result);

  console.log("\nSubscribing to SQL updates...");
  const unsubscribeSql = await client.sql.onSubscribe(
    { clientId: "example-client" },
    (update) => console.log("SQL update:", update),
    (error) => console.error("SQL error:", error),
    () => console.log("SQL subscription connected!")
  );

  // --- Log Sink ---
  console.log("\n--- Log Sink ---");

  const logs = await client.log.queryLogs({ limit: 5 });
  console.log("Recent logs:", logs);

  console.log("\nSubscribing to log updates...");
  const unsubscribeLogs = await client.log.onSubscribeLogs(
    { limit: 10 },
    (update) => console.log("Log update:", update),
    (error) => console.error("Log error:", error),
    () => console.log("Log subscription connected!")
  );

  // --- Generic Topic Subscription (with schema-aware decoding) ---
  console.log("\n--- Generic Topic Subscription ---");
  console.log("Subscribing to multiple topics with filters...");

  const unsubscribeTopics = await client.torii.onSubscribeToTopicsStream(
    {
      clientId: "example-client",
      topics: [
        { topic: "sql", filters: { table: "user" }, filterData: { typeUrl: "", value: null } },
        { topic: "logs", filters: {}, filterData: { typeUrl: "", value: null } },
      ],
      unsubscribeTopics: [],
    },
    (update) => console.log(`Topic [${update.topic}]:`, update.data),
    (error) => console.error("Topic error:", error),
    () => console.log("Topic subscription connected!")
  );

  // Run for 10 seconds
  await new Promise((resolve) => setTimeout(resolve, 10000));

  unsubscribeSql();
  unsubscribeLogs();
  unsubscribeTopics();
  console.log("Done");
}

main().catch(console.error);
