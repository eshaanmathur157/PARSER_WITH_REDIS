# import redis

# def initialize_redis_data():
#     # --- Configuration ---
#     REDIS_HOST = 'localhost'
#     REDIS_PORT = 6379
#     REDIS_DB = 0

#     # Key names
#     KEYS = {
#         # Existing Sets
#         "base_vaults": "BASE_VAULTS",
#         "quote_vaults": "QUOTE_VAULTS",
#         "base_mints": "BASE_MINTS",
#         "quote_mints": "QUOTE_MINTS",
        
#         # NEW: Hash Maps
#         "pair_to_base": "PAIR_TO_BASE_VAULT",   # Map: Pair Address -> Base Vault
#         "pair_to_quote": "PAIR_TO_QUOTE_VAULT"  # Map: Pair Address -> Quote Vault
#     }

#     # --- SOURCE DATA FROM YOUR JSON ---
#     # Structure:
#     # id = Pair Address
#     # vault.B = Base Vault (holds the volatile token like Unipcs, ROCK, FKH)
#     # vault.A = Quote Vault (holds USD1)
#     # mintB = Base Mint
#     # mintA = Quote Mint
    
#     pools_data = [
#         # Pool 1: Unipcs / USD1
#         {
#             "pair_id": "8P2kKPp3s38CAek2bxALLzFcooZH46X8YyLckYp6UkVt",
#             "base_vault": "5BCZRRPXi41SdzdvhghxG7NHw5SiaZsSdLwT7n6CAMt3",
#             "quote_vault": "DVLdDa689zwWCHVBZHmaVqaKM7LyfuMEeEuQ1QsauqZT",
#             "base_mint": "2orNgazHWM1f2g2KKKsLqGZEnH4vtJ2iMhAYCW6M5SnV", 
#             "quote_mint": "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB"
#         },
#         # Pool 2: ROCK / USD1
#         {
#             "pair_id": "CTDpCZejs8oi4dwGNqYZgHxr8GRj86PSMGsAz3cgKPYq",
#             "base_vault": "HLp1oNYmEXUoEzxrG9Cxxtg74WLALZgquk29W9haps8L",
#             "quote_vault": "13FcrjpmAftTfhAUqCpU82FvDbthgBWXyQvFWT2McQ8K",
#             "base_mint": "D8FYTqJGSmJx2cchFDUgzMYEe4VDvUyGWAYCRJ4Xbonk",
#             "quote_mint": "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB"
#         },
#         # Pool 3: FKH / USD1
#         {
#             "pair_id": "8Lq7gz2aEzkMQNfLpYmjv3V8JbD26LRbFd11SnRicCE6",
#             "base_vault": "FcdVJiinzBd7s3nBh6v7hNduweeRirRGHXrD1rQQP89",
#             "quote_vault": "Cunr3MeYP28JqyNcQTD3e2Kjz7FJH7E3mUFxKob47JmH",
#             "base_mint": "BCXpjsHYmgVpRKdv4EQv1RARhYagnnwPkJjYbvM6bonk",
#             "quote_mint": "USD1ttGY1N17NEEHLmELoaybftRBUSErhqYiQzvEmuB"
#         }
#     ]

#     try:
#         # Connect to Redis
#         r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
#         print(f"✓ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

#         pipe = r.pipeline()

#         # 1. Clear ALL existing keys to ensure a fresh start
#         print("→ Cleaning old keys...")
#         for key_name in KEYS.values():
#             pipe.delete(key_name)

#         # 2. Prepare Data Structures
#         base_vaults_set = set()
#         quote_vaults_set = set()
#         base_mints_set = set()
#         quote_mints_set = set()
        
#         # Dictionaries for the new Hash Maps
#         pair_to_base_map = {}
#         pair_to_quote_map = {}

#         # 3. Iterate through source data
#         for pool in pools_data:
#             # Add to Sets
#             base_vaults_set.add(pool["base_vault"])
#             quote_vaults_set.add(pool["quote_vault"])
#             base_mints_set.add(pool["base_mint"])
#             quote_mints_set.add(pool["quote_mint"])
            
#             # Add to Maps (Pair ID -> Vault)
#             pair_to_base_map[pool["pair_id"]] = pool["base_vault"]
#             pair_to_quote_map[pool["pair_id"]] = pool["quote_vault"]

#         # 4. Pipeline Commands
        
#         # Populate Sets (Existing functionality)
#         if base_vaults_set: pipe.sadd(KEYS["base_vaults"], *base_vaults_set)
#         if quote_vaults_set: pipe.sadd(KEYS["quote_vaults"], *quote_vaults_set)
#         if base_mints_set: pipe.sadd(KEYS["base_mints"], *base_mints_set)
#         if quote_mints_set: pipe.sadd(KEYS["quote_mints"], *quote_mints_set)

#         # Populate Hash Maps (New functionality)
#         if pair_to_base_map: pipe.hset(KEYS["pair_to_base"], mapping=pair_to_base_map)
#         if pair_to_quote_map: pipe.hset(KEYS["pair_to_quote"], mapping=pair_to_quote_map)

#         # 5. Execute
#         pipe.execute()
#         print("✓ Data populated successfully.")

#         # --- Verification ---
#         print("\n--- Current Redis State ---")
        
#         # Verify Sets
#         set_keys = ["base_vaults", "quote_vaults"]
#         for k in set_keys:
#             key_name = KEYS[k]
#             count = r.scard(key_name)
#             print(f"[{key_name}] Count: {count}")

#         # Verify Hash Maps
#         map_keys = ["pair_to_base", "pair_to_quote"]
#         for k in map_keys:
#             key_name = KEYS[k]
#             data = r.hgetall(key_name)
#             print(f"\n[{key_name}] Items: {len(data)}")
#             for pair, vault in data.items():
#                 print(f"  Pair: {pair[:15]}... -> Vault: {vault[:15]}...")

#     except redis.ConnectionError:
#         print("✗ Could not connect to Redis. Is the server running?")
#     except Exception as e:
#         print(f"✗ An error occurred: {e}")

# if __name__ == "__main__":
#     initialize_redis_data()
import json
import requests
import redis
import time
import sys

# --- Configuration ---
REDIS_HOST = '20.46.50.39'
REDIS_PORT = 6379

# HTTP Queue Config
QUEUE_SERVER_URL = "http://20.46.50.39:8080/new_pools"
MY_QUEUE_NAME = "ingest_price_event_queue"  # Must match the Flask server config

# Redis Keys
KEY_PAIR_TO_BASE = "PAIR_TO_BASE_VAULT"
KEY_PAIR_TO_QUOTE = "PAIR_TO_QUOTE_VAULT"
KEY_BASE_PRICE_MAP = "BASE_VAULT_TO_PRICE"
KEY_QUOTE_PRICE_MAP = "QUOTE_VAULT_TO_PRICE"

# Local State
# Tracks price history to calculate % change: { 'pool_addr': (last_base_price, last_quote_price) }
price_history_map = {} 

# Tracks metadata so we know which vault belongs to which pool: 
# { 'pool_addr': { 'base_vault': '...', 'quote_vault': '...' } }
pool_metadata_map = {
    # Pre-seeding with your hardcoded examples if needed, otherwise empty
    "8P2kKPp3s38CAek2bxALLzFcooZH46X8YyLckYp6UkVt": {"base_vault": None, "quote_vault": None},
    "CTDpCZejs8oi4dwGNqYZgHxr8GRj86PSMGsAz3cgKPYq": {"base_vault": None, "quote_vault": None},
    "8Lq7gz2aEzkMQNfLpYmjv3V8JbD26LRbFd11SnRicCE6": {"base_vault": None, "quote_vault": None}
}

# Initialize Redis (Sync)
try:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True, health_check_interval=30)
    print("✅ Connected to Redis (Map Storage)")
except Exception as e:
    print(f"Failed to connect to Redis: {e}")
    sys.exit(1)


def poll_queue_for_new_pools():
    """
    Hits the HTTP Queue to get new pools to track.
    """
    try:
        headers = {"X-Machine-Name": MY_QUEUE_NAME}
        response = requests.get(QUEUE_SERVER_URL, headers=headers, timeout=3)
        
        if response.status_code == 200:
            new_pools = response.json()
            if new_pools:
                print(f"📩 [Queue] Received {len(new_pools)} new pools to track.")
                
                for pool_data in new_pools:
                    p_addr = pool_data.get('pool_address')
                    b_vault = pool_data.get('base_vault')
                    q_vault = pool_data.get('quote_vault')
                    
                    if p_addr:
                        # 1. Update Local Metadata (so we can track prices for vaults)
                        pool_metadata_map[p_addr] = {
                            "base_vault": b_vault,
                            "quote_vault": q_vault
                        }

                        # 2. Update Redis Maps Immediately (Link Pair -> Vaults)
                        # We use nx=True (Not Exist) to avoid overwriting if already set, 
                        # or just overwrite if you prefer. Here we just set it.
                        if b_vault: r.hset(KEY_PAIR_TO_BASE, p_addr, b_vault)
                        if q_vault: r.hset(KEY_PAIR_TO_QUOTE, p_addr, q_vault)
                        
                        print(f"   + Tracking {p_addr} (Vaults: {b_vault[:6]}... / {q_vault[:6]}...)")

    except Exception as e:
        print(f"⚠️ Queue Poll Error: {e}")


def check_prices():
    """
    Iterates over all tracked pools, checks DexScreener, 
    and updates Redis Price Maps if change > 10%.
    """
    # Create a copy to iterate safely
    current_pools = list(pool_metadata_map.keys())
    
    for pair in current_pools:
        url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair}"
        
        try:
            response = requests.get(url, timeout=5)
            if response.status_code != 200:
                continue
                
            data = response.json()
            pairs_data = data.get("pairs", [])
            
            if not pairs_data:
                continue

            pair_data = pairs_data[0]
            
            # Parse Prices
            try:
                base_price_usd = float(pair_data.get("priceUsd", "0") or 0)
                price_native = float(pair_data.get("priceNative", "0") or 0)
                # Calculate Quote Price (SOL Price) = Base / Native
                quote_price_usd = (base_price_usd / price_native) if price_native > 0 else 0.0
            except ValueError:
                continue

            # Logic: Significant Change Check
            should_update = False
            
            if pair not in price_history_map:
                # First time seeing this pair -> Update
                should_update = True
                price_history_map[pair] = (base_price_usd, quote_price_usd)
                # print(f"✨ [Init] {pair} @ ${base_price_usd}")
            else:
                old_base, old_quote = price_history_map[pair]
                
                # Calc % change
                base_change = abs(base_price_usd - old_base) / old_base if old_base > 0 else 1.0
                quote_change = abs(quote_price_usd - old_quote) / old_quote if old_quote > 0 else 1.0
                
                if base_change > 0.10 or quote_change > 0.10:
                    should_update = True
                    price_history_map[pair] = (base_price_usd, quote_price_usd)
                    print(f"⚡ [Volatility] {pair} shifted >10%. New: ${base_price_usd}")

            # Update Redis if needed
            if should_update:
                metadata = pool_metadata_map[pair]
                b_vault = metadata.get("base_vault")
                q_vault = metadata.get("quote_vault")

                # Update Base Price Map
                if b_vault:
                    r.hset(KEY_BASE_PRICE_MAP, b_vault, str(base_price_usd))
                
                # Update Quote Price Map
                if q_vault:
                    r.hset(KEY_QUOTE_PRICE_MAP, q_vault, str(quote_price_usd))

        except Exception as e:
            # print(f"Price check error for {pair}: {e}")
            pass
        
        # Polite delay between DexScreener calls to avoid rate limits
        time.sleep(0.5)

# --- Main Loop ---
if __name__ == "__main__":
    print(f"🚀 Ingest Prices Started. Queue: {MY_QUEUE_NAME}")
    print(f"📊 Tracking {len(pool_metadata_map)} initial pools.")

    while True:
        # 1. Poll for new pools to track
        poll_queue_for_new_pools()
        
        # 2. Check prices for all tracked pools
        check_prices()
        
        # 3. Sleep briefly to not hammer APIs
        time.sleep(0.5)