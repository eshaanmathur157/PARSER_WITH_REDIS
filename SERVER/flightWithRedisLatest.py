import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import sys
import redis

class SolanaFlightServer(flight.FlightServerBase):
    def __init__(self, location, **kwargs):
        super(SolanaFlightServer, self).__init__(location, **kwargs)

        # --- CONFIGURATION: Redis Connection ---
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

        # Redis Key Names
        self.REDIS_KEYS = {
            "base_vaults": "BASE_VAULTS",
            "quote_vaults": "QUOTE_VAULTS",
            "base_mints": "BASE_MINTS",
            "quote_mints": "QUOTE_MINTS",
            # New Price Maps
            "base_prices": "BASE_VAULT_TO_PRICE",
            "quote_prices": "QUOTE_VAULT_TO_PRICE"
        }

        print(f"Server running at: {location}")
        print(f"Connected to Redis at localhost:6379")

    def _get_redis_data(self):
        """
        Fetches Sets (Watchlists) AND Hashes (Prices) in a single pipeline.
        Returns: (base_v_set, quote_v_set, base_m_set, quote_m_set, base_price_map, quote_price_map)
        """
        try:
            pipe = self.redis_client.pipeline()
            
            # 1. Fetch Watchlist Sets
            pipe.smembers(self.REDIS_KEYS["base_vaults"])
            pipe.smembers(self.REDIS_KEYS["quote_vaults"])
            pipe.smembers(self.REDIS_KEYS["base_mints"])
            pipe.smembers(self.REDIS_KEYS["quote_mints"])
            
            # 2. Fetch Price Maps (Hash Maps)
            pipe.hgetall(self.REDIS_KEYS["base_prices"])
            pipe.hgetall(self.REDIS_KEYS["quote_prices"])

            # Execute all at once
            results = pipe.execute()

            return results[0], results[1], results[2], results[3], results[4], results[5]
        except Exception as e:
            print(f" ✗ Redis connection failed: {e}")
            return set(), set(), set(), set(), {}, {}

    def do_put(self, context, descriptor, reader, writer):
        print(f"\n[NEW STREAM] Path: {descriptor.path}")
        sys.stdout.flush()

        try:
            print("  → Starting to read chunks...")
            sys.stdout.flush()

            chunk_count = 0
            while True:
                try:
                    # Reading the chunk
                    batch, metadata = reader.read_chunk()

                    if batch is None:
                        print("  → Batch is None, breaking")
                        break

                    chunk_count += 1

                    # --- STEP 1: Extract Timestamp ---
                    ts_val = 0
                    if metadata:
                        try:
                            metadata_str = metadata.to_pybytes().decode('utf-8')
                            if metadata_str.startswith("timestamp:"):
                                ts_val = int(metadata_str.split(":", 1)[1])
                        except Exception:
                            pass 

                    # --- STEP 2: Convert to Pandas ---
                    df = batch.to_pandas()
                    df['timestamp'] = ts_val

                    # --- STEP 3: Derived Logic (Redis) ---
                    # Fetch Sets and Price Maps
                    base_v_set, quote_v_set, base_m_set, quote_m_set, base_p_map, quote_p_map = self._get_redis_data()

                    if 'wallet' in df.columns:
                        # A. Tag Vaults (Existing Logic)
                        mask_base_v = df['wallet'].isin(base_v_set)
                        df['baseVault'] = df['wallet'].where(mask_base_v, None)

                        mask_quote_v = df['wallet'].isin(quote_v_set)
                        df['quoteVault'] = df['wallet'].where(mask_quote_v, None)

                        # B. Attach Prices (New Logic)
                        # Pandas .map() is efficient: 
                        # If wallet is in the map (it's a vault), it gets the price.
                        # If wallet is NOT in the map (regular user), it gets NaN/None.
                        df['base_price'] = df['wallet'].map(base_p_map)
                        df['quote_price'] = df['wallet'].map(quote_p_map)

                    if 'mint' in df.columns:
                        mask_base_m = df['mint'].isin(base_m_set)
                        df['baseMint'] = df['mint'].where(mask_base_m, None)

                        mask_quote_m = df['mint'].isin(quote_m_set)
                        df['quoteMint'] = df['mint'].where(mask_quote_m, None)

                    # --- STEP 4: Print the Data ---
                    final_cols = [
                        'timestamp', 'wallet', 'signature', 'mint', 
                        'pre_balance', 'post_balance', 
                        'baseVault', 'quoteVault', 
                        'base_price', 'quote_price'  # <--- Added for display
                    ]

                    # Select existing columns only to avoid key errors if schema changes slightly
                    print_cols = [c for c in final_cols if c in df.columns]
                    print_df = df[print_cols]

                    print("-" * 60)
                    print(f"Chunk {chunk_count} | Rows: {len(print_df)} | Timestamp: {ts_val}")
                    # Debug: Show we have prices loaded
                    print(f"Redis Cache -> Vaults: {len(base_v_set)} | Base Prices: {len(base_p_map)}")
                    print("-" * 60)

                    # Printing first 10 rows
                    pd.set_option('display.max_columns', None) # Ensure all cols show
                    pd.set_option('display.width', 1000)
                    print(print_df.head(10).to_string(index=False))
                    print("-" * 60)
                    sys.stdout.flush()

                except StopIteration:
                    print("  → StopIteration caught, ending stream")
                    break
                except Exception as inner_e:
                    print(f"  ✗ Error reading chunk: {inner_e}")
                    import traceback
                    traceback.print_exc()
                    break

            print(f"  → Finished reading stream, total chunks: {chunk_count}")

        except Exception as e:
            print(f"✗ OUTER Error during do_put: {e}")
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    location = "grpc+tcp://0.0.0.0:8815"
    server = SolanaFlightServer(location)
    server.serve()
