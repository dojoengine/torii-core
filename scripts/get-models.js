import { Provider, selector, Contract } from "starknet";
import fs from "fs";
import * as path from "path";

export const resolvePath = (rpath) => {
  return path.resolve(__dirname, rpath);
};
const __dirname = process.cwd();
const CONTRACT_ADDRESS =
  "0x02d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb";
const RPC_URL = "https://starknet-mainnet.public.blastapi.io";
const from_block = 1460161;
const provider = new Provider({ nodeUrl: RPC_URL });
const modelRegisteredSelector = selector.getSelector("ModelRegistered");
const eventRegisteredSelector = selector.getSelector("EventRegistered");
const modelUpgradedSelector = selector.getSelector("ModelUpgraded");
const eventUpgradedSelector = selector.getSelector("EventUpgraded");
const dirPath = resolvePath("./test-data/blob-arena/model-contracts");

const writeToFile = (data, index) => {
  fs.writeFileSync(`${dirPath}/${index}.json`, JSON.stringify(data, null, 2));
};

const declareResult = await provider.getEvents({
  address: CONTRACT_ADDRESS,
  keys: [[modelRegisteredSelector, eventRegisteredSelector]],
  chunk_size: 1024,
  from_block: { block_number: from_block },
});
for (const event of declareResult.events) {
  const [class_hash, address] = event.data;
  console.log({ class_hash, address });
  let schema = await provider.callContract({
    contractAddress: address,
    entrypoint: "schema",
  });
  let use_legacy_storage = true;
  try {
    let legacy_result = await provider.callContract({
      contractAddress: address,
      entrypoint: "use_legacy_storage",
    });
    use_legacy_storage = BigInt(legacy_result[0]);
  } catch (e) {}

  writeToFile({ schema, use_legacy_storage }, address);
}

const upgradeResult = await provider.getEvents({
  address: CONTRACT_ADDRESS,
  keys: [[modelUpgradedSelector, eventUpgradedSelector]],
  chunk_size: 1024,
  from_block: { block_number: from_block },
});
for (const event of upgradeResult.events) {
  const [class_hash, address, previous_address] = event.data;
  console.log({ class_hash, address, previous_address });
  let schema = await provider.callContract({
    contractAddress: address,
    entrypoint: "schema",
  });
  let use_legacy_storage = false;
  try {
    let legacy_result = await provider.callContract({
      contractAddress: address,
      entrypoint: "use_legacy_storage",
    });
    use_legacy_storage = BigInt(legacy_result[0]);
  } catch (e) {}

  writeToFile({ schema, use_legacy_storage }, address);
}
