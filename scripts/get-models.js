import { Provider, selector, Contract } from "starknet";
import fs from "fs";
import * as path from "path";

export const resolvePath = (rpath) => {
  return path.resolve(__dirname, rpath);
};
const __dirname = process.cwd();
// const CONTRACT_ADDRESS =
//   "0x02d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb";
const CONTRACT_ADDRESS =
  "0x8b4838140a3cbd36ebe64d4b5aaf56a30cc3753c928a79338bf56c53f506c5"; //Pistols
const RPC_URL = "https://starknet-mainnet.public.blastapi.io";
const from_block = 1376382;
const provider = new Provider({ nodeUrl: RPC_URL });
const modelRegisteredSelector = selector.getSelector("ModelRegistered");
const eventRegisteredSelector = selector.getSelector("EventRegistered");
const modelUpgradedSelector = selector.getSelector("ModelUpgraded");
const eventUpgradedSelector = selector.getSelector("EventUpgraded");
const dirPath = resolvePath("/home/ben/tc-tests/pistols/model-contracts");

const writeToFile = (data, index) => {
  fs.writeFileSync(`${dirPath}/${index}.json`, JSON.stringify(data, null, 2));
};

const getAllEvents = async (address, keys, block_number) => {
  let events = [];
  let continuation_token = undefined;
  while (true) {
    const result = await provider.getEvents({
      address,
      keys,
      chunk_size: 1024,
      continuation_token,
      from_block: { block_number },
    });
    events.push(...result.events);
    continuation_token = result.continuation_token;
    if (continuation_token === undefined) {
      break;
    }
  }
  return events;
};

const declareEvents = await getAllEvents(
  CONTRACT_ADDRESS,
  [[modelRegisteredSelector, eventRegisteredSelector]],
  from_block
);
for (const event of declareEvents) {
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
    use_legacy_storage = Boolean(BigInt(legacy_result[0]));
  } catch (e) {}

  writeToFile({ schema, use_legacy_storage }, address);
}

const upgradeEvents = await getAllEvents(
  CONTRACT_ADDRESS,
  [[modelUpgradedSelector, eventUpgradedSelector]],
  from_block
);

for (const event of upgradeEvents) {
  const [class_hash, address, previous_address] = event.data;
  console.log({ class_hash, address, previous_address });
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
    console.log(legacy_result);
  } catch (e) {}

  writeToFile({ schema, use_legacy_storage }, address);
}
