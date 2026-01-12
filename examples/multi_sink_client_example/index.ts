import { ToriiClient } from "@toriijs/sdk";
import { SqlSinkClient } from "./generated";

const SERVER_URL = process.env.TORII_URL || "http://localhost:8080";

async function main() {
  console.log("Connecting to Torii server at", SERVER_URL);

  const client = new ToriiClient(SERVER_URL, {
    sql: SqlSinkClient,
  });

  // Get server version
  const version = await client.getVersion();
  console.log("Server version:", version);

  // List available topics
  const topics = await client.listTopics();
  console.log("Available topics:", topics);

  // Get database schema
  const schema = await client.sql.getSchema({});
  console.log("Schema:", schema);

  // Query data
  const result = await client.sql.query({
    query: "SELECT * FROM sql_operation LIMIT 3",
  });
  console.log("Query result:", result);

  // Subscribe to real-time updates
  console.log("\nSubscribing to SQL updates...");
  const unsubscribe = await client.sql.onSubscribe(
    { clientId: "example-client" },
    (update) => console.log("Update:", update),
    (error) => console.error("Error:", error),
    () => console.log("Connected!")
  );

  // Run for 10 seconds
  await new Promise((resolve) => setTimeout(resolve, 10000));

  unsubscribe();
  console.log("Done");
}

main().catch(console.error);
