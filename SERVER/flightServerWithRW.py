import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd
import sys
import redis
import psycopg2
import psycopg2.extras
import numpy as np
import traceback

class SolanaFlightServer(flight.FlightServerBase):
    def __init__(self, location, **kwargs):
        super(SolanaFlightServer, self).__init__(location, **kwargs)

        # --- CONFIGURATION: Redis Connection ---
        try:
            self.redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
            self.redis_client.ping() # Test connection
            print("✅ Connected to Redis at localhost:6379")
        except Exception as e:
            print(f"✗ Failed to connect to Redis: {e}")
            self.redis_client = None

        # Redis Key Names
        self.REDIS_KEYS = {
            "base_vaults": "BASE_VAULTS",
            "quote_vaults": "QUOTE_VAULTS",
            "base_mints": "BASE_MINTS",
            "quote_mints": "QUOTE_MINTS",
            "base_prices": "BASE_VAULT_TO_PRICE",
            "quote_prices": "QUOTE_VAULT_TO_PRICE"
        }

        # --- CONFIGURATION: RisingWave Connection ---
        self.rw_conn = None
        try:
            self.rw_conn = psycopg2.connect(
                host="localhost",
                port="4566",
                user="root",
                dbname="dev"
            )
            self.rw_conn.autocommit = True # Critical for streaming performance
            print("✅ Connected to RisingWave at localhost:4566")
            
            # Ensure the table exists immediately upon connection
            self._ensure_table_exists()
            
        except Exception as e:
            print(f"✗ Failed to connect to RisingWave: {e}")

        print(f"🚀 Server running at: {location}")

    def _ensure_table_exists(self):
        """
        Checks if the 'transactions' table exists in RisingWave. If not, creates it.
        """
        if self.rw_conn is None:
            return

        # Matches the schema you provided
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS transactions (
            timestamp BIGINT,
            wallet VARCHAR,
            signature VARCHAR,
            mint VARCHAR,
            pre_balance DOUBLE PRECISION,
            post_balance DOUBLE PRECISION,
            base_vault VARCHAR,
            quote_vault VARCHAR,
            base_price DOUBLE PRECISION,
            quote_price DOUBLE PRECISION
        );
        """
        try:
            with self.rw_conn.cursor() as cur:
                cur.execute(create_table_sql)
            print("  ✅ Table 'transactions' validated/created.")
        except Exception as e:
            print(f"  ✗ Failed to validate table: {e}")

    def _get_redis_data(self):
        """
        Fetches Sets (Watchlists) AND Hashes (Prices) in a single pipeline.
        Returns: (base_v_set, quote_v_set, base_m_set, quote_m_set, base_price_map, quote_price_map)
        """
        if self.redis_client is None:
             return set(), set(), set(), set(), {}, {}

        try:
            pipe = self.redis_client.pipeline()
            pipe.smembers(self.REDIS_KEYS["base_vaults"])
            pipe.smembers(self.REDIS_KEYS["quote_vaults"])
            pipe.smembers(self.REDIS_KEYS["base_mints"])
            pipe.smembers(self.REDIS_KEYS["quote_mints"])
            pipe.hgetall(self.REDIS_KEYS["base_prices"])
            pipe.hgetall(self.REDIS_KEYS["quote_prices"])
            results = pipe.execute()
            return results[0], results[1], results[2], results[3], results[4], results[5]
        except Exception as e:
            print(f" ✗ Redis read failed: {e}")
            return set(), set(), set(), set(), {}, {}

    def _write_to_risingwave(self, df):
        """
        Writes a Pandas DataFrame to the 'transactions' table in RisingWave.
        """
        if self.rw_conn is None or df.empty:
            return

        try:
            # --- FIX 1: Safety Copy ---
            # Create an explicit copy to avoid SettingWithCopyWarning
            df = df.copy()

            # --- FIX 2: Type Enforcement ---
            # Redis returns strings. RisingWave expects Doubles.
            if 'base_price' in df.columns:
                df['base_price'] = pd.to_numeric(df['base_price'], errors='coerce')
            
            if 'quote_price' in df.columns:
                df['quote_price'] = pd.to_numeric(df['quote_price'], errors='coerce')

            # Convert Pandas NaNs (which SQL hates) to Python None (which SQL loves as NULL)
            df_clean = df.where(pd.notnull(df), None)

            # Extract Columns and Values
            columns = list(df_clean.columns)
            values = df_clean.values.tolist()

            # Construct SQL: "INSERT INTO transactions (col1, col2) VALUES %s"
            cols_str = ",".join(columns)
            sql = f"INSERT INTO transactions ({cols_str}) VALUES %s"

            with self.rw_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, values)
                
            # Note: No commit() needed because autocommit=True

        except Exception as e:
            print(f" ✗ RisingWave Write Failed: {e}")
            # traceback.print_exc() # Uncomment if you need deep debugging

    def do_put(self, context, descriptor, reader, writer):
        print(f"\n[NEW STREAM] Path: {descriptor.path}")
        sys.stdout.flush()

        try:
            chunk_count = 0
            while True:
                try:
                    batch, metadata = reader.read_chunk()
                    if batch is None:
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

                    # --- STEP 3: Redis Enrichment ---
                    base_v_set, quote_v_set, base_m_set, quote_m_set, base_p_map, quote_p_map = self._get_redis_data()

                    if 'wallet' in df.columns:
                        mask_base_v = df['wallet'].isin(base_v_set)
                        df['baseVault'] = df['wallet'].where(mask_base_v, None)

                        mask_quote_v = df['wallet'].isin(quote_v_set)
                        df['quoteVault'] = df['wallet'].where(mask_quote_v, None)

                        # Efficient Map (Pandas automatically handles missing keys as NaN)
                        df['base_price'] = df['wallet'].map(base_p_map)
                        df['quote_price'] = df['wallet'].map(quote_p_map)

                    if 'mint' in df.columns:
                        mask_base_m = df['mint'].isin(base_m_set)
                        df['baseMint'] = df['mint'].where(mask_base_m, None)

                        mask_quote_m = df['mint'].isin(quote_m_set)
                        df['quoteMint'] = df['mint'].where(mask_quote_m, None)

                    # --- STEP 4: PREPARE FOR DATABASE (CRITICAL FIXES) ---
                    
                    # 1. Rename Columns to match SQL (snake_case)
                    # We map the Pandas names (Left) to DB names (Right)
                    rename_map = {
                        'baseVault': 'base_vault',
                        'quoteVault': 'quote_vault',
                        # If you have these columns in DB, rename them too. 
                        # Based on your CREATE TABLE, you don't have 'base_mint', 
                        # so we won't rename/include them to avoid errors.
                    }
                    df.rename(columns=rename_map, inplace=True)

                    # 2. Define the exact columns your DB expects (snake_case)
                    final_db_cols = [
                        'timestamp', 'wallet', 'signature', 'mint',
                        'pre_balance', 'post_balance',
                        'base_vault', 'quote_vault',  # Correct SQL names
                        'base_price', 'quote_price'
                    ]

                    # 3. Filter Columns safely
                    valid_cols = [c for c in final_db_cols if c in df.columns]
                    
                    # 4. Create a CLEAN COPY (Fixes SettingWithCopyWarning)
                    final_df = df[valid_cols].copy()

                    # --- STEP 5: Write to RisingWave ---
                    if not final_df.empty:
                        self._write_to_risingwave(final_df)
                        print(f"Chunk {chunk_count} | Rows: {len(final_df)} | RW Write: ✅")
                    else:
                        print(f"Chunk {chunk_count} | Rows: 0 | Skipped")

                except StopIteration:
                    print("  → Stream finished by client.")
                    break
                except Exception as inner_e:
                    print(f"  ✗ Error reading chunk: {inner_e}")
                    traceback.print_exc()
                    break

            print(f"  → Stream closed. Total chunks: {chunk_count}")

        except Exception as e:
            print(f"✗ CRITICAL SERVER ERROR: {e}")
            traceback.print_exc()

if __name__ == '__main__':
    location = "grpc+tcp://0.0.0.0:8815"
    server = SolanaFlightServer(location)
    server.serve()
