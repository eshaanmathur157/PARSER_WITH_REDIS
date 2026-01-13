import redis
import json
import sys
import time

# --- CONFIGURATION ---
REDIS_HOST = '20.46.50.39'
REDIS_PORT = 6379
REDIS_DB = 0
CHANNEL_NAME = 'prices_channel'

# Redis Keys (Must match Flight Server)
KEY_PAIR_TO_BASE = "PAIR_TO_BASE_VAULT"
KEY_PAIR_TO_QUOTE = "PAIR_TO_QUOTE_VAULT"
KEY_BASE_PRICE_MAP = "BASE_VAULT_TO_PRICE"
KEY_QUOTE_PRICE_MAP = "QUOTE_VAULT_TO_PRICE"

def start_subscriber():
    print(f"Starting subscriber for channel: '{CHANNEL_NAME}'...")
    
    # Outer loop for reconnection
    while True:
        try:
            # 1. Connect to Redis
            r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True, health_check_interval=2)
            p = r.pubsub()
            p.subscribe(CHANNEL_NAME)
            print(f"✓ Connected to Redis. Listening on channel: '{CHANNEL_NAME}'...")
            print("→ Waiting for price updates...")
            print("-" * 60)

            # 2. Listen for Messages
            for message in p.listen():
                try:
                    # Redis sends different message types (subscribe, message, etc.)
                    if message['type'] == 'message':
                        # Decode the JSON payload
                        data_str = message['data']
                        event = json.loads(data_str)
                        
                        # Extract data (Using keys from your publisher script)
                        # Supports both 'base_price' and 'baseprice' just in case
                        pair_id = event.get('pair')
                        base_price = event.get('base_price') or event.get('baseprice')
                        quote_price = event.get('quote_price') or event.get('quoteprice')

                        if not pair_id:
                            print(f"[Warn] Received event without pair ID: {event}")
                            continue
                        
                        # --- A. Lookup Vaults ---
                        # We need to find which vaults correspond to this pair
                        # Using a pipeline for speed
                        pipe_lookup = r.pipeline()
                        pipe_lookup.hget(KEY_PAIR_TO_BASE, pair_id)
                        pipe_lookup.hget(KEY_PAIR_TO_QUOTE, pair_id)
                        base_vault, quote_vault = pipe_lookup.execute()

                        if not base_vault or not quote_vault:
                            print(f"[Skip] No vault mapping found for pair: {pair_id}")
                            continue

                        # --- B. Update Price Maps ---
                        pipe_update = r.pipeline()
                        
                        if base_price is not None:
                            pipe_update.hset(KEY_BASE_PRICE_MAP, base_vault, base_price)
                        
                        if quote_price is not None:
                            pipe_update.hset(KEY_QUOTE_PRICE_MAP, quote_vault, quote_price)
                        
                        pipe_update.execute()

                        # --- C. Logging ---
                        print(f"[{time.strftime('%H:%M:%S')}] Update for {pair_id[:8]}...")
                        print(f"   Base Vault ({base_vault[:8]}...): ${base_price}")
                        print(f"   Quote Vault ({quote_vault[:8]}...): ${quote_price}")
                        print("-" * 40)

                except json.JSONDecodeError:
                    print(f"✗ Failed to decode JSON: {message['data']}")
                except Exception as e:
                    print(f"✗ Error processing message: {e}")
                    
        except redis.ConnectionError as e:
            print(f"✗ Redis Connection Lost: {e}")
            print("→ Reconnecting in 5 seconds...")
            time.sleep(5)
        except Exception as e:
            print(f"✗ Unexpected Error: {e}")
            print("→ Reconnecting in 5 seconds...")
            time.sleep(5)

if __name__ == "__main__":
    try:
        start_subscriber()
    except KeyboardInterrupt:
        print("\n→ Shutting down gracefully...")
        sys.exit(0)