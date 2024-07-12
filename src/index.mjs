import axios from "axios";
import { CID, create } from 'kubo-rpc-client';
import { vClient, vTransaction } from '@vsc.eco/client';
import { DID } from "dids";
import { Ed25519Provider } from "key-did-provider-ed25519";
import KeyResolver from 'key-did-resolver';
// pla: parameterize
const VSC_API = '192.168.0.213:1337';
const IPFS_OPTIONS = { host: '192.168.0.213', port: '5101', protocol: 'http' };
const BTC_RPC = 'https://bitcoin-mainnet.public.blastapi.io';
const CONTRACT_ID = 'vs41q9c3ygzksacmvzqqnsps0eshcrmljryngeqmfqalmct2jpgfmsjkhmefkqfyvw6j';
const DID_SECRET = "8b9226616064fbcd75711e67ba854e565a0278e94842ebfe52ee615f91a445f9";
const SUBMIT_AMOUNT = 10;
const QUEUE_MAX_SIZE = 5000;
const DELAY_BETWEEN_INJECTIONS = 1000 * 60 * 3; // 3 minutes
const MISSING_BLOCKS_CHECKS_BLOCK_INTERVAL = 5000; // 5000 blocks
const LIVE_BLOCK_CHECK_INTERVAL = 1000 * 60 * 5; // 5 minutes
const FAILED_REQUESTS_NOTIFICATION_INTERVAL = 1000 * 60 * 5; // 5 minute
const DELAY_BETWEEN_BTC_RPC_CALLS = 1000; // 1 seconds
const MAX_REQUEST_RETRIES = 5;
const MAX_REQUEST_FAIL_COUNTER = 10;
const headersIngestQueue = [];
let failCounter = 0;
const failedRequests = [];
async function rpcBitcoinCall(name, params) {
    let retries = 0;
    let waitTime = DELAY_BETWEEN_BTC_RPC_CALLS;
    while (retries < MAX_REQUEST_RETRIES) {
        try {
            const data = await axios.post(BTC_RPC, {
                "jsonrpc": "1.0", "id": "curltest", "method": name, "params": params
            });
            return data.data;
        }
        catch (error) {
            if (error.response && error.response.status === 429) {
                await sleep(waitTime);
                waitTime *= 1.5;
            }
            else {
                retries++;
                if (retries >= MAX_REQUEST_RETRIES) {
                    failCounter++;
                    failedRequests.push({ name, params });
                    if (failCounter >= MAX_REQUEST_FAIL_COUNTER) {
                        console.error("Maximum fail counter reached. Aborting program.");
                        process.exit(1);
                    }
                }
            }
        }
    }
    throw new Error(`Failed to execute ${name} after ${MAX_REQUEST_RETRIES} retries`);
}
async function fetchBlockRaw(height) {
    const blockHash = (await rpcBitcoinCall('getblockhash', [height])).result;
    const blockDataRaw = (await rpcBitcoinCall('getblockheader', [blockHash, false])).result;
    return blockDataRaw;
}
async function getLastOutputTransaction() {
    const STATE_GQL = `
        query MyQuery($contractId: String) {
            findContractOutput(filterOptions: {
                byContract: $contractId
                limit: 1
            }) {
                outputs {
                id
                }
            }
        }
    `;
    const { data } = await axios.post(`http://${VSC_API}/api/v1/graphql`, {
        query: STATE_GQL,
        variables: {
            contractId: CONTRACT_ID,
        },
    });
    return data.data.findContractOutput.outputs[0]?.id;
}
async function getContractState(lastOutputTx) {
    const STATE_GQL = `
        query MyQuery($outputId: String) {
            contractState(id: $outputId) {
                state
            }
        }
    `;
    const { data } = await axios.post(`http://${VSC_API}/api/v1/graphql`, {
        query: STATE_GQL,
        variables: {
            outputId: lastOutputTx,
        },
    });
    return data.data.contractState.state;
}
function sortByHeight(obj) {
    // Extract all object values from the main object
    const items = Object.values(obj);
    // Sort these objects based on the 'height' property
    items.sort((a, b) => a.height - b.height);
    // Return the sorted array of objects
    return items;
}
function findMissingHeights(data) {
    // Get sorted items by height
    const sortedItems = sortByHeight(data);
    // Find the highest height
    const highestHeight = sortedItems[sortedItems.length - 1]?.height || 0;
    // Create a complete range from 0 to highest height
    const completeRange = Array.from({ length: highestHeight + 1 }, (_, i) => i);
    // Extract the existing heights
    const existingHeights = sortedItems.map(item => item.height);
    // Determine missing heights
    const missingHeights = completeRange.filter(height => !existingHeights.includes(height));
    return missingHeights;
}
async function getContractBlockProgress() {
    const ipfs = await create(IPFS_OPTIONS);
    const txId = await getLastOutputTransaction();
    const state = await getContractState(txId);
    const preheaders = state["pre-headers"].Links[0].Hash["/"];
    const blocks = (await ipfs.dag.get(CID.parse(preheaders))).value;
    const sortedBlocks = sortByHeight(blocks);
    const lastHeight = sortedBlocks[sortedBlocks.length - 1].height;
    const missingHeights = findMissingHeights(blocks);
    return {
        lastHeight,
        missingHeights
    };
}
async function sendVSCTx(didSecret, action, payload) {
    const client = new vClient({
        api: `http://${VSC_API}`,
        loginType: 'offchain'
    });
    const secret = Buffer.from(didSecret, 'hex');
    const keyPrivate = new Ed25519Provider(secret);
    const did = new DID({ provider: keyPrivate, resolver: KeyResolver.getResolver() });
    await did.authenticate();
    await client.login(did);
    const tx = new vTransaction();
    tx.setTx({
        op: 'call_contract',
        action: action,
        contract_id: CONTRACT_ID,
        payload: payload
    });
    // await tx.broadcast(client);
}
async function startContractInjector() {
    setInterval(async () => {
        let currentHeaderIngestion = {};
        for (let x = 0; x < SUBMIT_AMOUNT; x++) {
            currentHeaderIngestion = { ...currentHeaderIngestion, ...headersIngestQueue.shift() };
        }
        const payload = {
            headers: Object.values(currentHeaderIngestion)
        };
        console.log(`Injecting the following headers to the contract: ${Object.keys(currentHeaderIngestion)}`);
        await sendVSCTx(DID_SECRET, "processHeaders", JSON.stringify(payload));
    }, DELAY_BETWEEN_INJECTIONS);
}
async function startFailedRequestNotifier() {
    setInterval(() => {
        if (failedRequests.length > 0) {
            console.error("Failed requests:", failedRequests);
        }
    }, FAILED_REQUESTS_NOTIFICATION_INTERVAL);
}
async function fetchMissingBlocks() {
    const blocksToFetch = await getContractBlockProgress();
    console.log(`Fetching ${blocksToFetch.missingHeights.length} blocks that are missing from the contract`);
    for (let missingHeight of blocksToFetch.missingHeights) {
        const blockRaw = await fetchBlockRaw(missingHeight);
        await sleep(DELAY_BETWEEN_BTC_RPC_CALLS);
        headersIngestQueue.push({ [missingHeight]: blockRaw });
    }
}
async function sleep(milliseconds) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}
async function getTopBlockHeight() {
    return (await rpcBitcoinCall('getblockchaininfo', [])).result.blocks;
}
(async () => {
    await startFailedRequestNotifier();
    await startContractInjector();
    let topBlock = await getTopBlockHeight();
    const blockProgress = await getContractBlockProgress();
    let currentBlock = blockProgress.lastHeight;
    let live = currentBlock === topBlock;
    while (true) {
        const lastBlock = currentBlock;
        try {
            if (live) {
                while (currentBlock === topBlock) {
                    console.log(`Waiting for new block to be mined (current block: ${currentBlock})`);
                    await sleep(LIVE_BLOCK_CHECK_INTERVAL);
                    topBlock = await getTopBlockHeight();
                    if (currentBlock !== topBlock) {
                        console.log(`New block mined! (current block: ${currentBlock})`);
                        break;
                    }
                }
            }
            currentBlock++;
            const blockRaw = await fetchBlockRaw(currentBlock);
            await sleep(DELAY_BETWEEN_BTC_RPC_CALLS);
            headersIngestQueue.push({ [currentBlock]: blockRaw });
            // check if we are live
            if (currentBlock >= topBlock) {
                topBlock = await getTopBlockHeight();
                live = currentBlock === topBlock;
            }
            // every x blocks do a state check for missing blocks and inject them to the contract
            if (currentBlock % MISSING_BLOCKS_CHECKS_BLOCK_INTERVAL === 0) {
                await fetchMissingBlocks();
            }
            // if header queue full wait until queue is processed and has x free entries
            while (headersIngestQueue.length > QUEUE_MAX_SIZE) {
                await sleep(DELAY_BETWEEN_INJECTIONS);
            }
        }
        catch (e) {
            // to make sure that if something failed we retry with the same block
            if (currentBlock !== lastBlock) {
                currentBlock = lastBlock;
            }
            console.error("Error while processing block, retrying...", e);
        }
    }
})();
