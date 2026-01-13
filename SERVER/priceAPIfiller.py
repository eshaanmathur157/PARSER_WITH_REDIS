import requests
import redis
import time
import itertools

# --- CONFIGURATION ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# The Redis keys we need to read from and write to
KEY_PAIR_TO_BASE = "PAIR_TO_BASE_VAULT"
KEY_PAIR_TO_QUOTE = "PAIR_TO_QUOTE_VAULT"
KEY_BASE_PRICE_MAP = "BASE_VAULT_TO_PRICE"
KEY_QUOTE_PRICE_MAP = "QUOTE_VAULT_TO_PRICE"

# Your specific list of pairs (Pool IDs)
# 1. Unipcs/USD1, 2. ROCK/USD1, 3. FKH/USD1
TARGET_PAIRS = [
    "8P2kKPp3s38CAek2bxALLzFcooZH46X8YyLckYp6UkVt",
    "CTDpCZejs8oi4dwGNqYZgHxr8GRj86PSMGsAz3cgKPYq",
    "8Lq7gz2aEzkMQNfLpYmjv3V8JbD26LRbFd11SnRicCE6"
]

def fetch_prices_round_robin():
    # 1. Connect to Redis
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        print(f"✓ Connected to Redis. Fetching vault info...")
    except Exception as e:
        print(f"✗ Redis Connection Error: {e}")
        return

    print("→ Starting Round-Robin Price Fetcher (1s delay)...")
    print("-" * 60)

    # 2. Cycle indefinitely through the list (Round Robin)
    for pair_id in itertools.cycle(TARGET_PAIRS):
        try:
            # --- A. Get Vault Info from Redis ---
            # We need to know WHICH vault to assign this price to
            base_vault = r.hget(KEY_PAIR_TO_BASE, pair_id)
            quote_vault = r.hget(KEY_PAIR_TO_QUOTE, pair_id)

            if not base_vault or not quote_vault:
                print(f"[Warn] No vault mapping found in Redis for pair {pair_id}. Skipping update.")
                # We still fetch price to show it works, but can't save to Redis without keys
                # continue 

            # --- B. Call DexScreener API ---
            url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair_id}"
            response = requests.get(url, timeout=5)
            
            if response.status_code == 200:
                data = response.json()
                pairs = data.get("pairs", [])
                
                if pairs:
                    pair_data = pairs[0]
                    base_token = pair_data.get("baseToken", {})
                    quote_token = pair_data.get("quoteToken", {})
                    
                    # --- C. Extract Prices ---
                    # priceUsd = Price of 1 Base Token in USD
                    base_price_usd_str = pair_data.get("priceUsd", "0")
                    base_price_usd = float(base_price_usd_str) if base_price_usd_str else 0.0

                    # priceNative = Price of 1 Base Token in terms of Quote Token
                    # Therefore: Quote Price (USD) = Base Price (USD) / Price Native
                    price_native_str = pair_data.get("priceNative", "0")
                    price_native = float(price_native_str) if price_native_str else 0.0

                    quote_price_usd = 0.0
                    if price_native > 0:
                        quote_price_usd = base_price_usd / price_native

                    # --- D. Update Redis ---
                    if base_vault:
                        r.hset(KEY_BASE_PRICE_MAP, base_vault, base_price_usd)
                    
                    if quote_vault:
                        r.hset(KEY_QUOTE_PRICE_MAP, quote_vault, quote_price_usd)

                    # --- E. Logging ---
                    print(f"[{time.strftime('%H:%M:%S')}] Pair: {pair_data.get('pairAddress', pair_id)[:8]}...")
                    print(f"   Base ({base_token.get('symbol')}): ${base_price_usd:.6f} -> Vault: {base_vault[:8]}...")
                    print(f"   Quote ({quote_token.get('symbol')}): ${quote_price_usd:.6f} -> Vault: {quote_vault[:8]}...")
                    print("-" * 40)
                else:
                    print(f"✗ No pair data returned for {pair_id}")
            else:
                print(f"✗ API Error {response.status_code} for {pair_id}")

        except Exception as e:
            print(f"✗ Error in loop: {e}")

        # --- F. Wait 1 Second ---
        time.sleep(1)

if __name__ == "__main__":
    fetch_prices_round_robin()
