import { Provider } from "starknet";
import fs from "fs";
// const CONTRACT_ADDRESS =
//   "0x02d26295d6c541d64740e1ae56abc079b82b22c35ab83985ef8bd15dc0f9edfb"; // Replace with your contract address
const CONTRACT_ADDRESS =
  "0x8b4838140a3cbd36ebe64d4b5aaf56a30cc3753c928a79338bf56c53f506c5"; //Pistols
const RPC_URL = "https://starknet-mainnet.public.blastapi.io";
// const from_block = 1376383;
const provider = new Provider({ nodeUrl: RPC_URL });

const dirPath = "/home/ben/tc-tests/pistols/events-2";
const writeToFile = (data, index) => {
  fs.writeFileSync(
    `${dirPath}/${index.toString().padStart(4, "0")}.json`,
    JSON.stringify(data, null, 2)
  );
};
let n = 0;
let continuation_token = undefined;
while (true) {
  const result = await provider.getEvents({
    continuation_token,
    address: CONTRACT_ADDRESS,
    chunk_size: 1024,
    // from_block: { block_number: from_block },
  });
  if (result.events.length) {
    writeToFile(result, n);
  }
  continuation_token = result.continuation_token;
  console.log(
    `n: ${n}, continuation_token: ${continuation_token} length: ${result.events.length}`
  );
  if (continuation_token === undefined) {
    break;
  }
  n++;
}
