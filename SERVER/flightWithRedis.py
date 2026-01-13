import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import sys
import redis  # <--- IMPORT REDIS

class SolanaFlightServer(flight.FlightServerBase):
    def __init__(self, location, **kwargs):
        super(SolanaFlightServer, self).__init__(location, **kwargs)

        # --- CONFIGURATION: Redis Connection ---
        # decode_responses=True ensures we get Strings, not Bytes, from Redis
        self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        
        # We define the Redis Key names we want to watch
        self.REDIS_KEYS = {
            "base_vaults": "BASE_VAULTS",
            "quote_vaults": "QUOTE_VAULTS",
            "base_mints": "BASE_MINTS",
            "quote_mints": "QUOTE_MINTS"
        }

        print(f"Server running at: {location}")
        print(f"Connected to Redis at localhost:6379")

    def _get_active_watchlists(self):
        """
        Fetches the current state of all 4 sets from Redis in a single network round-trip.
        """
        try:
            pipe = self.redis_client.pipeline()
            pipe.smembers(self.REDIS_KEYS["base_vaults"])
            pipe.smembers(self.REDIS_KEYS["quote_vaults"])
            pipe.smembers(self.REDIS_KEYS["base_mints"])
            pipe.smembers(self.REDIS_KEYS["quote_mints"])
            
            # execute() returns a list of results in the order commands were queued
            results = pipe.execute()
            
            return results[0], results[1], results[2], results[3]
        except Exception as e:
            print(f" ✗ Redis connection failed: {e}")
            # Return empty sets on failure so the server doesn't crash
            return set(), set(), set(), set()

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
                            pass # Suppress metadata errors for cleanliness

                    # --- STEP 2: Convert to Pandas ---
                    df = batch.to_pandas()
                    df['timestamp'] = ts_val

                    # --- STEP 3: Derived Logic (UPDATED FOR REDIS) ---
                    # We fetch the LATEST sets from Redis for every single chunk.
                    # This ensures if you add a coin to Redis, it is detected immediately in the next chunk.
                    
                    base_v_set, quote_v_set, base_m_set, quote_m_set = self._get_active_watchlists()

                    if 'wallet' in df.columns:
                        # We use the fetched sets for standard Pandas filtering
                        mask_base_v = df['wallet'].isin(base_v_set)
                        df['baseVault'] = df['wallet'].where(mask_base_v, None)

                        mask_quote_v = df['wallet'].isin(quote_v_set)
                        df['quoteVault'] = df['wallet'].where(mask_quote_v, None)

                    if 'mint' in df.columns:
                        mask_base_m = df['mint'].isin(base_m_set)
                        df['baseMint'] = df['mint'].where(mask_base_m, None)

                        mask_quote_m = df['mint'].isin(quote_m_set)
                        df['quoteMint'] = df['mint'].where(mask_quote_m, None)

                    # --- STEP 4: Print the Data ---
                    final_cols = [
                        'timestamp', 'wallet', 'signature', 'mint', 
                        'pre_balance', 'post_balance', 
                        'baseVault', 'quoteVault', 'baseMint', 'quoteMint'
                    ]

                    print_df = df[[c for c in final_cols if c in df.columns]]

                    # Optional: Only print rows that actually matched something to reduce noise
                    # matched_df = print_df.dropna(subset=['baseVault', 'quoteVault', 'baseMint', 'quoteMint'], how='all')
                    
                    print("-" * 60)
                    print(f"Chunk {chunk_count} | Rows: {len(print_df)} | Timestamp: {ts_val}")
                    print(f"Active Watchlists (Redis) -> BaseVaults: {len(base_v_set)} | BaseMints: {len(base_m_set)}")
                    print("-" * 60)
                    
                    # Printing first 5 rows to avoid spamming console if chunk is huge
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
