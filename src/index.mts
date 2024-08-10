import axios from "axios";
import { CID, create } from 'kubo-rpc-client';
import { hexToUint8Array, vClient, vTransaction } from '@vsc.eco/client';
import { DID } from "dids";
import { Ed25519Provider } from "key-did-provider-ed25519";
import KeyResolver from 'key-did-resolver';
import winston from 'winston';
import DailyRotateFile from 'winston-daily-rotate-file';

// Configure Winston logger with daily rotate file
const logger = winston.createLogger({
    level: 'info',
    format: winston.format.combine(
        winston.format.timestamp({
            format: 'YYYY-MM-DD HH:mm:ss'
        }),
        winston.format.printf(info => `${info.timestamp} ${info.level}: ${info.message}`)
    ),
    transports: [
        new winston.transports.Console(),
        new DailyRotateFile({
            filename: 'application-%DATE%.log',
            dirname: 'logs',
            datePattern: 'YYYY-MM-DD',
            maxSize: '20m',
            maxFiles: '14d'
        })
    ],
});

// pla: parameterize
const VSC_API = 'localhost:1337'
const IPFS_OPTIONS = { url: 'http://localhost:5101' }
const BTC_RPC = 'https://bitcoin-mainnet.public.blastapi.io'
const CONTRACT_ID = 'vs41q9c3yg9af6z8ptpc29pujuc9lj99qkwha2vdmwx7ketnyxtlpgv0d97pguchqe9s'
const DID_SECRET = "8b9226616064fbcd75711e67ba854e565a0278e94842ebfe52ee615f91a445f9"
const QUEUE_MAX_SIZE = 5000; // how many btc block headers we can store in the queue
const DELAY_BETWEEN_INJECTIONS = 1000 * 60 * 3; // supposed to represent the time we can be sure a transaction is finalized
const LIVE_BLOCK_CHECK_INTERVAL = 1000 * 60 * 5; // when we are live we check every x minutes for new blocks
const FAILED_REQUESTS_NOTIFICATION_INTERVAL = 1000 * 60 * 5; // how often to notify about failed requests
const DELAY_BETWEEN_BTC_RPC_CALLS = 1000; // how much time at minimum we wait between btc rpc calls
const DELAY_BETWEEN_VSC_API_CALLS = 1500; // how much time at minimum we wait between vsc api calls
const MAX_REQUEST_RETRIES = 5; // how many times we retry a btc rpc request
const MAX_REQUEST_FAIL_COUNTER = 10; // how often we can fail a btc rpc request before we stop the program
const NO_PROGRESS_SHUTDOWN_THRESHOLD = 10; // how many times we can't make progress (contract state doesnt update) until we stop the program
const NO_PROGRESS_RESET_THRESHOLD = 3; // how many times we can't make progress (contract state doesnt update) until we reset the state (every x tries)
const HEALTH_CHECK_INTERVAL = 300; // how many blocks can pass until we check if the state is actually updated

const SUBMIT_AMOUNT = 100; // max blocks per tx
const PARALLEL_TX_INJECT_AMOUNT = 1; // tx simultaneous injected

const BLOCK_ZERO_HEADER_HASH = "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49ffff001d1dac2b7c";
const CONTRACT_INIT_PARAMS = {
    startHeader: BLOCK_ZERO_HEADER_HASH,
    height: 0,
    previousDifficulty: "1",
    validityDepth: 6,
    lastDifficultyPeriodRetargetBlock: BLOCK_ZERO_HEADER_HASH
}

const headersIngestQueue: Array<Block> = []
let failCounter = 0;
const failedRequests: { name: string, params: any }[] = [];
let contractHeightCache;
let noProgressCounter: number = 0;
let client: vClient;
let nextBlockOverride: number | undefined = undefined;

async function rpcBitcoinCall(name: string, params: any) {
    let retries = 0;
    let waitTime = DELAY_BETWEEN_BTC_RPC_CALLS;

    while (retries < MAX_REQUEST_RETRIES) {
        try {
            const data = await axios.post(BTC_RPC, {
                "jsonrpc": "1.0", "id": "curltest", "method": name, "params": params
            });
            return data.data;
        } catch (error: any) {
            if (error.response && error.response.status === 429) {
                await sleep(waitTime);
                waitTime *= 1.5;
            } else {
                retries++;
                if (retries >= MAX_REQUEST_RETRIES) {
                    failCounter++;
                    failedRequests.push({ name, params });
                    if (failCounter >= MAX_REQUEST_FAIL_COUNTER) {
                        logger.error("Maximum fail counter reached. Aborting program.");
                        process.exit(1);
                    }
                }
            }
        }
    }

    throw new Error(`Failed to execute ${name} after ${MAX_REQUEST_RETRIES} retries`);
}

async function fetchBlockRaw(height: number) {
    const blockHash = (await rpcBitcoinCall('getblockhash', [height])).result
    const blockDataRaw = (await rpcBitcoinCall('getblockheader', [blockHash, false])).result

    return blockDataRaw
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
    `

    const { data } = await axios.post(`http://${VSC_API}/api/v1/graphql`, {
        query: STATE_GQL,
        variables: {
            contractId: CONTRACT_ID,
        },
    })

    return data.data.findContractOutput.outputs[0]?.id
}

async function getContractState(lastOutputTx) {
    const STATE_GQL = `
        query MyQuery($outputId: String) {
            contractState(id: $outputId) {
                state
            }
        }
    `

    const { data } = await axios.post(`http://${VSC_API}/api/v1/graphql`, {
        query: STATE_GQL,
        variables: {
            outputId: lastOutputTx,
        },
    })

    return data.data.contractState.state
}

interface Header {
    height: number;
    [key: string]: any;
}

function sortByHeight(obj: { [key: string]: Header }): Header[] {
    // Extract all object values from the main object
    const items: Header[] = Object.values(obj);

    // Sort these objects based on the 'height' property
    items.sort((a, b) => a.height - b.height);

    // Return the sorted array of objects
    return items;
}

function createRange(start: number, end: number): number[] {
    return Array.from({ length: end - start + 1 }, (_, i) => i + start);
}

interface BlocksToFetch {
    lastHeight: number;
}

interface Block {
    [key: string]: string;
}

function parseRange(range: string): [number, number] {
    const [start, end] = range.split('-').map(Number);
    return [start, end];
}

function sortByRangeProperty(objects: Array<any>): Array<any> {
    return objects.sort((a, b) => {
        const [aStart, aEnd] = parseRange(a['Name']);
        const [bStart, bEnd] = parseRange(b['Name']);
        return aStart - bStart || aEnd - bEnd;
    });
}

function getHighestKey(obj: { [key: string]: any }): number {
    const keys = Object.keys(obj).map(Number);
    return Math.max(...keys);
}

async function getContractBlockProgress(): Promise<number> {
    try {
        const ipfs = await create(IPFS_OPTIONS);
    
        const txId = await getLastOutputTransaction()
        if (!txId) {
            return undefined
        }
    
        const state = await getContractState(txId)
        const headers = state.headers.Links
    
        // pla: getting the last height of the last confirmed header, NOT preheaders!
        const sortedHeaders = sortByRangeProperty(Object.values(headers))
        const lastHeaderHash = sortedHeaders[sortedHeaders.length - 1].Hash["/"]
        const lastHeaderStore = (await ipfs.dag.get(CID.parse(lastHeaderHash)) as any).value
        const lastConfirmedHeight = getHighestKey(lastHeaderStore)
        return lastConfirmedHeight
    } catch (e) {
        logger.error("Failed to get contract block progress", e)
        return undefined
    }
}

async function getClient() {
    const client = new vClient({
        api: `http://${VSC_API}`,
        loginType: 'offchain'
    })
    const secret = hexToUint8Array(DID_SECRET)
    const keyPrivate = new Ed25519Provider(secret)
    const did = new DID({ provider: keyPrivate, resolver: KeyResolver.getResolver() })
    await did.authenticate()
    
    await client.login(did)

    return client;
}

async function sendVSCTx(action, payload) {
    if (!client) {
        client = await getClient()
    }
    const tx = new vTransaction()
    tx.setTx({
        op: 'call_contract',
        action: action,
        contract_id: CONTRACT_ID,
        payload: payload
    })
    await tx.broadcast(client);
}

async function startContractInjector() {
    setInterval(async () => {
        let health = true;
        for (let x = 0; x < PARALLEL_TX_INJECT_AMOUNT; x++) {

            await sleep(DELAY_BETWEEN_VSC_API_CALLS)
            let currentHeaderIngestion = {}
            
            for (let x = 0; x < SUBMIT_AMOUNT; x++) {
                const block = headersIngestQueue.shift()
                const blockHeight: number = +Object.keys(block)[0]

                // every x blocks do a health check to validate if we make progress and reset the state if necessary
                if (blockHeight % HEALTH_CHECK_INTERVAL === 0) {
                    health = await checkHealth(blockHeight)
                }
                
                currentHeaderIngestion = { ...currentHeaderIngestion, ...block }
            }

            if (!health) {
                nextBlockOverride = await getContractBlockProgress()
                headersIngestQueue.length = 0;
                logger.error(`Resetting the insertion queue and starting at block height ${nextBlockOverride} again`);
                break;
            }

            if (Object.keys(currentHeaderIngestion).length !== 0) {
                const payload = {
                    headers: Object.values(currentHeaderIngestion)
                }

                logger.info(`Injecting the following headers to the contract: ${Object.keys(currentHeaderIngestion)}`)
                await silenceConsoleLogAsync(sendVSCTx, "processHeaders", payload)
            }
        }
    }, DELAY_BETWEEN_INJECTIONS)
}

async function silenceConsoleLogAsync(func: Function, ...args) {
    const originalConsoleLog = console.log;
    console.log = function () { };
    try {
        await func(...args);
        console.log = originalConsoleLog;
    }
    catch (e) {
        console.log = originalConsoleLog;
        logger.error(e)
    }
}

async function startFailedRequestNotifier() {
    setInterval(() => {
        if (failedRequests.length > 0) {
            logger.error("Failed requests:", failedRequests);
        }
    }, FAILED_REQUESTS_NOTIFICATION_INTERVAL);
}

async function sleep(milliseconds) {
    return new Promise(resolve => setTimeout(resolve, milliseconds));
}

async function getTopBlockHeight(): Promise<number> {
    return (await rpcBitcoinCall('getblockchaininfo', [])).result.blocks
}

async function checkHealth(currentHeight): Promise<boolean> {
    // wait for a little bit, so the last injections are confirmed
    await sleep(DELAY_BETWEEN_INJECTIONS);
    const blockProgress = await getContractBlockProgress()

    if ((contractHeightCache === undefined || contractHeightCache === null)  
        || contractHeightCache !== blockProgress) {
        contractHeightCache = blockProgress;
        noProgressCounter = 0;
        logger.info(`Health check passed at block height ${currentHeight}`)
    } else {
        logger.error("No new blocks were ingested since last check!")
        noProgressCounter++;
        
        if (noProgressCounter >= NO_PROGRESS_SHUTDOWN_THRESHOLD) {
            logger.error(`No progress was made for the last ${NO_PROGRESS_SHUTDOWN_THRESHOLD} checks. Exiting program.`)
            process.exit(1)
        } else if (noProgressCounter % NO_PROGRESS_RESET_THRESHOLD === 0) {
            logger.error(`No progress was made for the last ${noProgressCounter} checks.`)
            return false;
        }
    }
    
    return true;
}

(async () => {
    let blockProgress = await getContractBlockProgress()

    // need to initialize the contract first
    if (blockProgress === undefined && CONTRACT_INIT_PARAMS !== undefined) {
        await sendVSCTx("initializeAtSpecificBlock", CONTRACT_INIT_PARAMS);
        await sleep(DELAY_BETWEEN_INJECTIONS * 2)
        blockProgress = await getContractBlockProgress()
        if (blockProgress === undefined) {
            logger.error("Failed to initialize contract. Exiting program.")
            process.exit(1)
        }
    }

    await startFailedRequestNotifier()
    await startContractInjector()
    let topBlock = await getTopBlockHeight()
    let currentBlock = blockProgress;
    let live = currentBlock === topBlock;
    while (true) {
        if (nextBlockOverride !== undefined) {
            currentBlock = nextBlockOverride;
            nextBlockOverride = undefined;
        }

        const lastBlock = currentBlock
        try {
            if (live) {
                while (currentBlock === topBlock) {
                    logger.info(`Waiting for new block to be mined (current block: ${currentBlock})`)
                    await sleep(LIVE_BLOCK_CHECK_INTERVAL)
                    topBlock = await getTopBlockHeight()
                    if (currentBlock !== topBlock) {
                        logger.info(`New block mined! (current block: ${currentBlock})`)
                        break;
                    }
                }
            }

            currentBlock++;
            const blockRaw = await fetchBlockRaw(currentBlock);
            await sleep(DELAY_BETWEEN_BTC_RPC_CALLS)
            headersIngestQueue.push({ [currentBlock]: blockRaw })

            // check if we are live
            if (currentBlock >= topBlock) {
                topBlock = await getTopBlockHeight()
                live = currentBlock === topBlock
            }

            // if header queue full wait until queue is processed and has x free entries
            while (headersIngestQueue.length > QUEUE_MAX_SIZE) {
                await sleep(DELAY_BETWEEN_INJECTIONS)
            }
        } catch (e) {
            // to make sure that if something failed we retry with the same block
            if (currentBlock !== lastBlock) {
                currentBlock = lastBlock
            }
            logger.error("Error while processing block, retrying...", e)
        }
    }
})();
