import redis
import sys
import time
import asyncio
import aiohttp
import json
import re
from datetime import datetime, timezone
from multiprocessing import shared_memory, Queue, Process
import redis.asyncio as aioredis  # Async Redis for the consumer

# --- Configuration ---
REDIS_CMD_HOST = '20.46.50.39' # Listener (Remote Orchestrator)
REDIS_DATA_HOST = '20.46.50.39' # Writer (Local Data Storage)
REDIS_PORT = 6379

# --- NEW: HTTP Server Config ---
# Ensure this matches the port your Flask server is running on (5000 or 8080)
POOL_SERVER_URL = "http://20.46.50.39:8080/pool_update"

CHANNEL_NAME = 'start-work'
PUBLISH_CHANNEL = 'pool-monitor'

RPC_URL = "https://api.mainnet-beta.solana.com"
RAYDIUM_API_URL = "https://api-v3.raydium.io/pools/key/ids"
NUM_WORKERS = 6

# --- Shared Memory Config ---
SHM_NAME = "solana_json_shm"
SHM_SIZE = 10 * 1024 * 1024  # 10MB
FLAG_OFFSET = 0
SIZE_OFFSET = 1
DATA_OFFSET = 9

# --- Filter Logic (For Pool Detector) ---
TARGETS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": ["initialize2"],
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": ["Initialize", "InitializeWithPermission"],
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": ["CreatePool"]
}

# ==========================================
# PART 1: POOL DETECTOR (CONSUMER PROCESS)
# ==========================================

async def check_raydium_api(session, account_keys, worker_id):
    if not account_keys: return

    ids_param = ",".join(account_keys)

    try:
        async with session.get(f"{RAYDIUM_API_URL}?ids={ids_param}", timeout=5) as resp:
            data = await resp.json()

            if data.get('data'):
                print("[Raydium API] Pools found, processing...")
                # --- 1. CONNECT TO REDIS FOR WRITING ---
                r_write = aioredis.Redis(host=REDIS_DATA_HOST, port=REDIS_PORT, decode_responses=True)
                
                for pool in data['data']:
                    if pool is None: continue
                    p_id = pool.get('id')
                    # Age Check
                    print(f"[🆔 POOL ID] Found: {p_id}", flush=True)
                    open_time = int(pool.get('openTime', 0))
                    age_seconds = int(time.time()) - open_time
                    if age_seconds > 300 and open_time != 0: continue

                    # Extract Data
                    mint_a = pool.get('mintA', {})
                    mint_b = pool.get('mintB', {})
                    vaults = pool.get('vault', {})
                    
                    
                    q_mint = mint_a.get('address')
                    b_mint = mint_b.get('address')
                    b_vault = vaults.get('B')
                    q_vault = vaults.get('A')

                    payload = {
                        "pool_address": p_id,
                        "base_mint": b_mint,
                        "quote_mint": q_mint,
                        "base_vault": b_vault,
                        "quote_vault": q_vault
                    }
                    print(f"[Pool Detected] {json.dumps(payload)}")
                    # --- A. WRITE TO REDIS (Keep Existing Logic) ---
                    try:
                        print("[✅ Redis] Writing to Redis...")
                        pipeline = r_write.pipeline()
                        if b_vault: pipeline.sadd("BASE_VAULTS", b_vault)
                        if q_vault: pipeline.sadd("QUOTE_VAULTS", q_vault)
                        if b_mint:  pipeline.sadd("BASE_MINTS", b_mint)
                        if q_mint:  pipeline.sadd("QUOTE_MINTS", q_mint)
                        if p_id:    pipeline.sadd("PAIR_ADDRESSES", p_id)
                        pipeline.publish(PUBLISH_CHANNEL, json.dumps(payload))
                        await pipeline.execute()
                        print(f"\n[✅ REDIS] {b_mint} / {q_mint}")
                    except Exception as e:
                        print(f"[Redis Write Error] {e}")

                    # --- B. SEND TO HTTP SERVER (New Logic) ---
                    try:
                        # Construct Header: "proxy1", "proxy2", etc.
                        machine_name = f"proxy{worker_id}" 
                        headers = {"X-Machine-Name": machine_name}
                        
                        # We reuse the existing 'session' which is efficient
                        async with session.post(POOL_SERVER_URL, json=payload, headers=headers) as post_resp:
                            if post_resp.status == 200:
                                print(f"[✅ HTTP] Sent to server as {machine_name}")
                            else:
                                print(f"[❌ HTTP] Failed: {post_resp.status}")
                    except Exception as e:
                        print(f"[HTTP Send Error] {e}")
                
                await r_write.aclose()

    except Exception as e:
        print(f"[Raydium API Error] {e}")

async def process_block(session, block_data, worker_id):
    if not block_data or 'transactions' not in block_data:
        return

    tasks = []
    for tx in block_data['transactions']:
        meta = tx.get('meta')
        if not meta or not meta.get('logMessages'): continue
        
        log_str = " ".join(meta['logMessages'])
        found = False
        
        for prog_id, instrs in TARGETS.items():
            if prog_id in log_str:
                for instr in instrs:
                    pattern = rf"Instruction: {instr}\b"
                    if re.search(pattern, log_str):
                        found = True
                        print(f"[Worker {worker_id}] Found {instr} in tx")
                        break
            if found: break
        
        if found:
            # Handle Raw JSON Format (List of strings)
            keys = tx['transaction']['message']['accountKeys']
            tasks.append(check_raydium_api(session, keys, worker_id))

    if tasks:
        await asyncio.gather(*tasks)

async def async_consumer_loop(mp_queue, worker_id):
    print(f"[Consumer] 🚀 Pool Detector Started for Worker {worker_id}...")
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                json_string = mp_queue.get_nowait()
                block_data = json.loads(json_string)
                
                if "result" in block_data:
                    block_data = block_data["result"]
                
                await process_block(session, block_data, worker_id)
                
            except Exception:
                await asyncio.sleep(0.01)

def consumer_entry_point(mp_queue, worker_id):
    try:
        asyncio.run(async_consumer_loop(mp_queue, worker_id))
    except KeyboardInterrupt:
        pass

# ==========================================
# PART 2: SUBSCRIBER (PRODUCER PROCESS)
# ==========================================
# ... (No changes needed in fetch_block or run_worker_inline, 
#      they just put data into the queue) ...

async def fetch_block(session, slot_num, request_id, worker_id, shm_buf, mp_queue):
    # Standard Raw JSON request
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "getBlock",
        "params": [slot_num, {
            "transactionDetails": "full",
            "maxSupportedTransactionVersion": 0,
        }]
    }

    try:
        start = asyncio.get_event_loop().time()
        async with session.post(RPC_URL, json=payload) as response:
            elapsed = (asyncio.get_event_loop().time() - start) * 1000
            now = datetime.now(timezone.utc)
            ts = now.strftime("%H:%M:%S") + f":{int(now.microsecond/1000):03d}"

            if response.status == 200:
                json_string = await response.text()
                result = json.loads(json_string)

                # 1. QUEUE WRITE
                try:
                    mp_queue.put_nowait(json_string)
                except Exception:
                    pass

                # 2. SHM WRITE
                if result.get("result") is not None:
                    print(f"[W {worker_id} | {ts}] Got slot {slot_num} ({elapsed:.1f} ms)")

                    if shm_buf is not None:
                        json_bytes = json_string.encode('utf-8')
                        data_size = len(json_bytes)

                        if data_size <= (SHM_SIZE - DATA_OFFSET):
                            retries = 0
                            while shm_buf[FLAG_OFFSET] == 1:
                                if retries > 0 and retries % 100 == 0:
                                    print(f"[W {worker_id}] Waiting for SHM to clear...")
                                await asyncio.sleep(0.005)
                                retries += 1
                            
                            shm_buf[SIZE_OFFSET:DATA_OFFSET] = data_size.to_bytes(8, 'little')
                            shm_buf[DATA_OFFSET:DATA_OFFSET + data_size] = json_bytes
                            shm_buf[FLAG_OFFSET] = 1 
                        else:
                            print(f"[W {worker_id}] Error: JSON size {data_size} > SHM limit")
                else:
                    pass 
            else:
                print(f"[W {worker_id} | {ts}] HTTP {response.status}")

    except Exception as e:
        print(f"[W {worker_id}] Error: {e}")

async def run_worker_inline(worker_id, slot, shm_buf, mp_queue):
    print(f"[Worker {worker_id}] Subscriber Loop Started at slot {slot}")
    async with aiohttp.ClientSession() as session:
        request_id = 0
        while True:
            asyncio.create_task(
                fetch_block(session, slot, request_id, worker_id, shm_buf, mp_queue)
            )
            slot += NUM_WORKERS
            request_id += 1
            await asyncio.sleep(2.4)

# ==========================================
# PART 3: MAIN ENTRY POINT
# ==========================================

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 main_orchestrator.py <worker_id>")
        sys.exit(1)

    WORKER_ID = int(sys.argv[1])
    
    mp_queue = Queue(maxsize=1000)

    # --- CHANGED: Pass WORKER_ID to consumer so it knows its name ---
    detector_proc = Process(target=consumer_entry_point, args=(mp_queue, WORKER_ID), daemon=True)
    detector_proc.start()

    shm = None
    try:
        shm = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=SHM_SIZE)
        shm.buf[FLAG_OFFSET] = 0
    except FileExistsError:
        try:
            shm = shared_memory.SharedMemory(name=SHM_NAME, create=False)
        except:
            pass

    shm_buf = shm.buf if shm else None

    # Wait for Redis Start Signal
    try:
        r = redis.Redis(host=REDIS_CMD_HOST, port=REDIS_PORT)
        p = r.pubsub()
        p.subscribe(CHANNEL_NAME)
        print(f"[Worker {WORKER_ID}] Waiting for start signal on {REDIS_CMD_HOST}...")

        for message in p.listen():
            if message['type'] == 'message':
                try:
                    data = json.loads(message['data'])
                    if isinstance(data, int):
                        starting_slot = data
                        start_time = time.time()
                    elif isinstance(data, dict):
                        starting_slot = int(data['slot'])
                        start_time = float(data['timestamp'])
                    else:
                        parts = message['data'].decode().split(',')
                        starting_slot = int(parts[0])
                        start_time = float(parts[1])
                except:
                    print("Error parsing start signal, defaulting to now")
                    starting_slot = 0
                    start_time = time.time()

                my_slot = starting_slot + WORKER_ID
                wait_time = (start_time + WORKER_ID * 0.4) - time.time()

                if wait_time > 0:
                    print(f"Sleeping {wait_time:.2f}s...")
                    time.sleep(wait_time)

                asyncio.run(run_worker_inline(
                    WORKER_ID, my_slot, shm_buf, mp_queue
                ))
                break

    finally:
        if shm:
            shm.close()

if __name__ == "__main__":
    main()