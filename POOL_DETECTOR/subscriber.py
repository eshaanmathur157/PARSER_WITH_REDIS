import asyncio
import aiohttp
import redis.asyncio as redis
import time
import json
import sys
import argparse
import re

# --- Configuration ---
REDIS_HOST = '20.46.50.39'
REDIS_PORT = 6379
CHANNEL_NAME = 'start-work'
PUBLISH_CHANNEL = 'pool-monitor'  # <--- Channel to send results to

SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"
RAYDIUM_API_URL = "https://api-v3.raydium.io/pools/key/ids"

# --- Filter Logic ---
TARGETS = {
    "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8": ["initialize2"],
    "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C": ["Initialize", "InitializeWithPermission"],
    "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK": ["CreatePool"]
}

async def get_block(session, slot):
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "getBlock",
        "params": [
            slot,
            {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0, "transactionDetails": "full", "rewards": False}
        ]
    }
    try:
        async with session.post(SOLANA_RPC_URL, json=payload, timeout=10) as resp:
            data = await resp.json()
            return data.get('result')
    except Exception as e:
        print(f"RPC Error on slot {slot}: {e}")
        return None

async def check_raydium_api(session, account_keys):
    if not account_keys: return

    ids_param = ",".join(account_keys)

    # print(f"[Raydium] 🔎 checking {len(account_keys)} keys...")

    try:
        async with session.get(f"{RAYDIUM_API_URL}?ids={ids_param}", timeout=5) as resp:
            data = await resp.json()

            if data.get('data'):
                # Create a temporary Redis connection just for publishing results
                r_pub = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
                
                for pool in data['data']:
                    # 1. Skip NULL entries
                    if pool is None:
                        continue

                    # 2. AGE CHECK (Filter out old pools/Swaps)
                    open_time = int(pool.get('openTime', 0))
                    current_time = int(time.time())
                    age_seconds = current_time - open_time

                    if age_seconds > 300: # Skip if older than 5 mins
                        continue

                    # 3. Extract Specific Data
                    mint_a_data = pool.get('mintA', {})
                    mint_b_data = pool.get('mintB', {})
                    vault_data = pool.get('vault', {})

                    payload = {
                        "base_mint": mint_a_data.get('address'),
                        "quote_mint": mint_b_data.get('address'),
                        "base_vault": vault_data.get('A'),
                        "quote_vault": vault_data.get('B')
                    }

                    # 4. Publish to Redis
                    try:
                        await r_pub.publish(PUBLISH_CHANNEL, json.dumps(payload))
                        print(f"\n[✅ SENT TO REDIS] {payload['base_mint']} / {payload['quote_mint']}")
                    except Exception as e:
                        print(f"[Redis Publish Error] {e}")
                
                await r_pub.aclose()

    except Exception as e:
        print(f"[Raydium] ❌ API Request Failed: {e}")

async def process_block(session, block_data, slot):
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
                    # Strict Regex Check
                    pattern = rf"Instruction: {instr}\b"
                    
                    if re.search(pattern, log_str):
                        # print(f"[!] HIT found in Slot {slot} ({instr})")
                        found = True
                        break
            if found: break
        
        if found:
            keys = [k['pubkey'] for k in tx['transaction']['message']['accountKeys']]
            tasks.append(check_raydium_api(session, keys))

    if tasks:
        await asyncio.gather(*tasks)

async def worker(vm_id):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    pubsub = r.pubsub()
    await pubsub.subscribe(CHANNEL_NAME)
    
    print(f"[VM-{vm_id}] Listening for start signal...")

    start_slot = 0
    start_ts = 0.0

    async for message in pubsub.listen():
        if message['type'] == 'message':
            try:
                data = json.loads(message['data'])
                start_slot = int(data['slot'])
                start_ts = float(data['timestamp'])
                print(f"[VM-{vm_id}] Received! Slot: {start_slot} | Start Time: {start_ts}")
                await pubsub.unsubscribe(CHANNEL_NAME)
                break
            except Exception as e:
                print(f"Error parsing Redis message: {e}")

    offset_seconds = vm_id * 0.4
    my_start_time = start_ts + offset_seconds
    current_slot = start_slot + vm_id
    
    now = time.time()
    sleep_duration = my_start_time - now
    
    if sleep_duration > 0:
        print(f"[VM-{vm_id}] Syncing... Sleeping {sleep_duration:.3f}s")
        await asyncio.sleep(sleep_duration)
    
    print(f"[VM-{vm_id}] STARTING LOOP at Slot {current_slot}")

    async with aiohttp.ClientSession() as session:
        while True:
            loop_start = time.time()
            
            print(f"[VM-{vm_id}] 📡 Requesting Block: {current_slot}") 
            
            asyncio.create_task(process_logic(session, current_slot))
            
            current_slot += 6
            
            elapsed = time.time() - loop_start
            sleep_time = max(0, 2.4 - elapsed)
            await asyncio.sleep(sleep_time)

async def process_logic(session, slot):
    block = await get_block(session, slot)
    await process_block(session, block, slot)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("vm_id", type=int, help="ID of this VM (0-5)")
    args = parser.parse_args()
    
    try:
        asyncio.run(worker(args.vm_id))
    except KeyboardInterrupt:
        pass
