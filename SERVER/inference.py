# import asyncio
# import json
# import time
# import redis.asyncio as redis
# import psycopg
# import torch
# import torch.nn as nn
# import numpy as np
# from psycopg.rows import dict_row

# # --- Configuration ---
# REDIS_HOST = '20.46.50.39'
# REDIS_PORT = 6379
# CHANNEL_NAME = 'pool-monitor'

# # RisingWave (Postgres) Connection Details
# RW_HOST = "127.0.0.1" # Change if RW is on a different IP
# RW_PORT = "4566"
# RW_USER = "root"
# RW_DB   = "dev"

# # Model Config
# NUM_FEATURES = 18
# HIDDEN_DIM = 64
# SEQUENCE_LENGTH = 3  # 15 mins / 5 min windows = 3 steps
# KEY_PAIR_TO_BASE = "PAIR_TO_BASE_VAULT"
# KEY_PAIR_TO_QUOTE = "PAIR_TO_QUOTE_VAULT"
# # --- 1. The Model Definition ---
# class EnhancedDiscriminator(nn.Module):
#     def __init__(self, num_features, hidden_dim, intermediate_dim=32):
#         super(EnhancedDiscriminator, self).__init__()
#         self.lstm = nn.LSTM(num_features, hidden_dim, num_layers=2, batch_first=True, dropout=0.25)
#         self.classification_head = nn.Sequential(
#             nn.Linear(hidden_dim * 2, intermediate_dim),
#             nn.LeakyReLU(0.2),
#             nn.Dropout(0.3),
#             nn.Linear(intermediate_dim, 1)
#         )
#         self.sigmoid = nn.Sigmoid()

#     def forward(self, x):
#         # x shape: (batch, seq_len, features)
#         lstm_out, (h_n, _) = self.lstm(x)
#         last_hidden_state = h_n[-1]
#         avg_pooled_states = torch.mean(lstm_out, dim=1)
#         combined_features = torch.cat((last_hidden_state, avg_pooled_states), dim=1)
#         out = self.classification_head(combined_features)
#         score = self.sigmoid(out)
#         return score

# # Initialize Model (and load weights if you have them)
# device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
# model = EnhancedDiscriminator(NUM_FEATURES, HIDDEN_DIM).to(device)
# model.eval() # Set to inference mode

# # --- 2. Data Fetching Logic ---
# async def fetch_pool_data(base_vault):
#     """
#     Queries RisingWave for the last 15 minutes of data (3 windows of 5 mins).
#     """
#     conn_str = f"host={RW_HOST} port={RW_PORT} user={RW_USER} dbname={RW_DB}"
    
#     # We look for data generated in the last 20 mins to be safe, ordered by time
#     query_price = """
#         SELECT window_time, price_delta, price_std, buy_price_std, sell_price_std, 
#                buy_price_min, buy_price_max, min_liquidity_base, max_liquidity_base
#         FROM pool_price 
#         WHERE poolIdentifier = %s 
#         ORDER BY window_time DESC 
#         LIMIT 3;
#     """

#     query_buys = """
#         SELECT time, number_of_buys, number_of_sells, number_of_unique_buyers, 
#                number_of_unique_sellers, buy_perc, sell_perc
#         FROM pool_buys 
#         WHERE poolIdentifierBaseVault = %s 
#         ORDER BY time DESC 
#         LIMIT 3;
#     """

#     try:
#         async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
#             async with aconn.cursor(row_factory=dict_row) as cur:
#                 # Fetch Price Data
#                 await cur.execute(query_price, (base_vault,))
#                 price_rows = await cur.fetchall()

#                 # Fetch Buy/Sell Data
#                 await cur.execute(query_buys, (base_vault,))
#                 buy_rows = await cur.fetchall()
                
#                 return price_rows, buy_rows
#     except Exception as e:
#         print(f"[DB Error] Failed to fetch data for {base_vault}: {e}")
#         return [], []

# # --- 3. Feature Engineering ---
# def build_tensor(price_data, buy_data):
#     """
#     Merges SQL rows into a (1, 3, 18) Tensor.
#     Note: RisingWave views might not perfectly align timestamps if activity is sparse.
#     We assume the last 3 rows correspond to the sequence.
#     """
#     features = []
    
#     # We need exactly 3 time steps. If we have less (e.g. pool died), we pad with zeros.
#     # We iterate backwards to align the most recent data.
#     max_steps = SEQUENCE_LENGTH
    
#     # Combine data based on index (assuming sort DESC)
#     # Ideally, you join on timestamp, but for this snippet we assume alignment by window index
#     for i in range(max_steps):
#         p_row = price_data[i] if i < len(price_data) else {}
#         b_row = buy_data[i] if i < len(buy_data) else {}
        
#         # Extract features (Default to 0.0 if missing)
#         # 1. Timestmap (Not used in model logic usually, but placeholder)
#         ts = 0.0 
        
#         # 2-4. Volumes (Calculated proxies or 0 if not in view)
#         # View doesn't have explicit Volume USD, using proxies or 0
#         agg_buy_vol = 0.0 
#         agg_sell_vol = 0.0
#         total_vol = 0.0
        
#         # 5-6. Liquidity
#         max_liq = float(p_row.get('max_liquidity_base', 0) or 0)
#         min_liq = float(p_row.get('min_liquidity_base', 0) or 0)
        
#         # 7-8. Price Min/Max
#         p_max = float(p_row.get('buy_price_max', 0) or 0)
#         p_min = float(p_row.get('buy_price_min', 0) or 0)
        
#         # 9-12. Price Stats
#         p_delta = float(p_row.get('price_delta', 0) or 0)
#         p_std   = float(p_row.get('price_std', 0) or 0)
#         b_std   = float(p_row.get('buy_price_std', 0) or 0)
#         s_std   = float(p_row.get('sell_price_std', 0) or 0)
        
#         # 13-16. Counts
#         n_buys  = float(b_row.get('number_of_buys', 0) or 0)
#         n_sells = float(b_row.get('number_of_sells', 0) or 0)
#         u_buys  = float(b_row.get('number_of_unique_buyers', 0) or 0)
#         u_sells = float(b_row.get('number_of_unique_sellers', 0) or 0)
        
#         # 17-18. Percentages
#         b_perc = float(b_row.get('buy_perc', 0) or 0)
#         s_perc = float(b_row.get('sell_perc', 0) or 0)

#         # Feature Vector (Size 18)
#         row_vector = [
#             ts, agg_buy_vol, agg_sell_vol, total_vol,
#             max_liq, min_liq, p_max, p_min,
#             p_delta, p_std, b_std, s_std,
#             n_buys, n_sells, u_buys, u_sells,
#             b_perc, s_perc
#         ]
#         features.append(row_vector)
    
#     # Reverse to get chronological order (Time T-2, T-1, T) because SQL was DESC
#     features.reverse()
    
#     # Convert to Tensor (Batch Size 1, Seq 3, Features 18)
#     tensor_x = torch.tensor([features], dtype=torch.float32).to(device)
#     return tensor_x

# # --- 4. The Orchestrator ---
# async def process_pool_lifecycle(pool_data):
#     """
#     The Main Lifecycle: Wait 15m -> Fetch -> Predict -> Print
#     """
#     pool_addr = pool_data.get('pool_address')
#     base_vault = pool_data.get('base_vault')
    
#     print(f"⏳ [Timer Started] Pool: {pool_addr} (Waiting 15 mins...)")
    
#     # --- FIX START ---
#     # We use 'async with' to ensure the connection closes properly
#     # HSET automatically creates the hash map if it doesn't exist.
#     async with redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, health_check_interval=2) as r:
#         await r.hset(KEY_PAIR_TO_BASE, pool_addr, base_vault)
#         await r.hset(KEY_PAIR_TO_QUOTE, pool_addr, pool_data.get('quote_vault'))
#     # --- FIX END ---

#     # 1. Wait 15 Minutes
#     await asyncio.sleep(15 * 60) 
    
#     print(f"🔄 [Querying DB] Pool: {pool_addr}")
    
#     # 2. Query Data
#     price_rows, buy_rows = await fetch_pool_data(base_vault)
    
#     if not price_rows and not buy_rows:
#         print(f"⚠️ [Data Missing] No trade data found for {pool_addr} after 15m.")
#         return

#     # 3. Build Tensor
#     input_tensor = build_tensor(price_rows, buy_rows)
    
#     # 4. Inference
#     with torch.no_grad():
#         score = model(input_tensor).item()
    
#     # 5. Output Result
#     result = {
#         "pool_address": pool_addr,
#         "sigmoid_output": round(score, 6)
#     }
    
#     # Print exactly as requested
#     print(json.dumps(result, indent=4))


# async def main_listener():
#     # Outer reconnection loop
#     while True:
#         try:
#             async with redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True, health_check_interval=2) as r:
#                 pubsub = r.pubsub()
#                 await pubsub.subscribe(CHANNEL_NAME)
                
#                 print(f"🎧 Inference Service listening on channel: {CHANNEL_NAME}")
                
#                 async for message in pubsub.listen():
#                     if message['type'] == 'message':
#                         try:
#                             data = json.loads(message['data'])
#                             # Fire and forget: Create a background task for this pool
#                             # This ensures we don't block the listener while waiting 15 mins
#                             asyncio.create_task(process_pool_lifecycle(data))
                            
#                         except Exception as e:
#                             print(f"Error parsing Redis message: {e}")
                            
#         except Exception as e:
#             print(f"❌ Connection lost: {e}. Reconnecting in 5s...")
#             await asyncio.sleep(5)

# if __name__ == "__main__":
#     try:
#         asyncio.run(main_listener())
#     except KeyboardInterrupt:
#         print("\n→ Shutting down gracefully...")
import asyncio
import json
import time
import aiohttp  # Added for HTTP Polling
import redis.asyncio as redis
import psycopg
import torch
import torch.nn as nn
import numpy as np
from psycopg.rows import dict_row

# --- Configuration ---
REDIS_HOST = '20.46.50.39'
REDIS_PORT = 6379

# HTTP Queue Config
QUEUE_SERVER_URL = "http://20.46.50.39:8080/new_pools"
MY_QUEUE_NAME = "inference_queue"

# RisingWave (Postgres) Connection Details
RW_HOST = "127.0.0.1" 
RW_PORT = "4566"
RW_USER = "root"
RW_DB   = "dev"

# Model Config
NUM_FEATURES = 18
HIDDEN_DIM = 64
SEQUENCE_LENGTH = 3  

# --- REDIS KEYS (Synced with Flight Server) ---
# Maps (For Inference/Ingest logic)
KEY_PAIR_TO_BASE = "PAIR_TO_BASE_VAULT"
KEY_PAIR_TO_QUOTE = "PAIR_TO_QUOTE_VAULT"

# Sets (For Flight Server Filtering)
KEY_BASE_VAULTS = "BASE_VAULTS"
KEY_QUOTE_VAULTS = "QUOTE_VAULTS"
KEY_BASE_MINTS = "BASE_MINTS"
KEY_QUOTE_MINTS = "QUOTE_MINTS"

# --- 1. The Model Definition ---
class EnhancedDiscriminator(nn.Module):
    def __init__(self, num_features, hidden_dim, intermediate_dim=32):
        super(EnhancedDiscriminator, self).__init__()
        self.lstm = nn.LSTM(num_features, hidden_dim, num_layers=2, batch_first=True, dropout=0.25)
        self.classification_head = nn.Sequential(
            nn.Linear(hidden_dim * 2, intermediate_dim),
            nn.LeakyReLU(0.2),
            nn.Dropout(0.3),
            nn.Linear(intermediate_dim, 1)
        )
        self.sigmoid = nn.Sigmoid()

    def forward(self, x):
        lstm_out, (h_n, _) = self.lstm(x)
        last_hidden_state = h_n[-1]
        avg_pooled_states = torch.mean(lstm_out, dim=1)
        combined_features = torch.cat((last_hidden_state, avg_pooled_states), dim=1)
        out = self.classification_head(combined_features)
        score = self.sigmoid(out)
        return score

# Initialize Model
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = EnhancedDiscriminator(NUM_FEATURES, HIDDEN_DIM).to(device)
model.eval()

# --- 2. Data Fetching Logic ---
async def fetch_pool_data(base_vault):
    conn_str = f"host={RW_HOST} port={RW_PORT} user={RW_USER} dbname={RW_DB}"
    
    query_price = """
        SELECT window_time, price_delta, price_std, buy_price_std, sell_price_std, 
               buy_price_min, buy_price_max, min_liquidity_base, max_liquidity_base
        FROM pool_price 
        WHERE poolIdentifier = %s 
        ORDER BY window_time DESC 
        LIMIT 3;
    """

    query_buys = """
        SELECT time, number_of_buys, number_of_sells, number_of_unique_buyers, 
               number_of_unique_sellers, buy_perc, sell_perc
        FROM pool_buys 
        WHERE poolIdentifierBaseVault = %s 
        ORDER BY time DESC 
        LIMIT 3;
    """

    try:
        async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
            async with aconn.cursor(row_factory=dict_row) as cur:
                await cur.execute(query_price, (base_vault,))
                price_rows = await cur.fetchall()

                await cur.execute(query_buys, (base_vault,))
                buy_rows = await cur.fetchall()
                
                return price_rows, buy_rows
    except Exception as e:
        print(f"[DB Error] Failed to fetch data for {base_vault}: {e}")
        return [], []

# --- 3. Feature Engineering ---
def build_tensor(price_data, buy_data):
    features = []
    max_steps = SEQUENCE_LENGTH
    
    for i in range(max_steps):
        p_row = price_data[i] if i < len(price_data) else {}
        b_row = buy_data[i] if i < len(buy_data) else {}
        
        ts = 0.0 
        agg_buy_vol = 0.0 
        agg_sell_vol = 0.0
        total_vol = 0.0
        
        max_liq = float(p_row.get('max_liquidity_base', 0) or 0)
        min_liq = float(p_row.get('min_liquidity_base', 0) or 0)
        p_max = float(p_row.get('buy_price_max', 0) or 0)
        p_min = float(p_row.get('buy_price_min', 0) or 0)
        p_delta = float(p_row.get('price_delta', 0) or 0)
        p_std   = float(p_row.get('price_std', 0) or 0)
        b_std   = float(p_row.get('buy_price_std', 0) or 0)
        s_std   = float(p_row.get('sell_price_std', 0) or 0)
        n_buys  = float(b_row.get('number_of_buys', 0) or 0)
        n_sells = float(b_row.get('number_of_sells', 0) or 0)
        u_buys  = float(b_row.get('number_of_unique_buyers', 0) or 0)
        u_sells = float(b_row.get('number_of_unique_sellers', 0) or 0)
        b_perc = float(b_row.get('buy_perc', 0) or 0)
        s_perc = float(b_row.get('sell_perc', 0) or 0)

        row_vector = [
            ts, agg_buy_vol, agg_sell_vol, total_vol,
            max_liq, min_liq, p_max, p_min,
            p_delta, p_std, b_std, s_std,
            n_buys, n_sells, u_buys, u_sells,
            b_perc, s_perc
        ]
        features.append(row_vector)
    
    features.reverse()
    tensor_x = torch.tensor([features], dtype=torch.float32).to(device)
    return tensor_x

# --- 4. The Orchestrator (Worker Task) ---
async def process_pool_lifecycle(pool_data):
    """
    1. Update Redis Maps AND Sets (Instant)
    2. Wait 15 Minutes
    3. Query DB & Predict
    """
    pool_addr = pool_data.get('pool_address')
    base_vault = pool_data.get('base_vault')
    quote_vault = pool_data.get('quote_vault')
    base_mint = pool_data.get('base_mint')
    quote_mint = pool_data.get('quote_mint')
    
    print(f"⏳ [Timer Started] Pool: {pool_addr} (Waiting 15 mins...)")
    
    # --- REDIS UPDATE (Maps + Sets) ---
    try:
        async with redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True) as r:
            pipeline = r.pipeline()
            
            # 1. Update Maps (For internal logic)
            pipeline.hset(KEY_PAIR_TO_BASE, pool_addr, base_vault)
            pipeline.hset(KEY_PAIR_TO_QUOTE, pool_addr, quote_vault)
            
            # 2. Update Sets (For Flight Server Filtering)
            if base_vault: pipeline.sadd(KEY_BASE_VAULTS, base_vault)
            if quote_vault: pipeline.sadd(KEY_QUOTE_VAULTS, quote_vault)
            if base_mint:  pipeline.sadd(KEY_BASE_MINTS, base_mint)
            if quote_mint: pipeline.sadd(KEY_QUOTE_MINTS, quote_mint)
            
            await pipeline.execute()
            print(f"✅ [Redis] Linked {pool_addr} & populated Flight Server sets.")

    except Exception as e:
        print(f"❌ [Redis Error] Failed to update Redis: {e}")
    # ------------------------

    # 1. Wait 15 Minutes
    await asyncio.sleep(15 * 60) 
    
    print(f"🔄 [Querying DB] Pool: {pool_addr}")
    
    # 2. Query Data
    price_rows, buy_rows = await fetch_pool_data(base_vault)
    
    if not price_rows and not buy_rows:
        print(f"⚠️ [Data Missing] No trade data found for {pool_addr} after 15m.")
        return

    # 3. Inference
    input_tensor = build_tensor(price_rows, buy_rows)
    
    with torch.no_grad():
        score = model(input_tensor).item()
    
    result = {
        "pool_address": pool_addr,
        "sigmoid_output": round(score, 6)
    }
    
    print(json.dumps(result, indent=4))


# --- 5. Main Poller ---
async def main_poller():
    print(f"🚀 Inference Service Polling: {QUEUE_SERVER_URL}")
    print(f"🏷️  Queue Name: {MY_QUEUE_NAME}")
    
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # Poll the HTTP Server
                headers = {"X-Machine-Name": MY_QUEUE_NAME}
                async with session.get(QUEUE_SERVER_URL, headers=headers, timeout=5) as resp:
                    if resp.status == 200:
                        pools = await resp.json()
                        
                        if pools:
                            print(f"📩 [Received] {len(pools)} new pools from queue.")
                            
                            for pool in pools:
                                asyncio.create_task(process_pool_lifecycle(pool))
                        
                    elif resp.status != 200:
                        print(f"⚠️ Server returned {resp.status}")

                await asyncio.sleep(1)

            except aiohttp.ClientError as e:
                print(f"❌ [HTTP Error] {e}. Retrying in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"❌ [Error] {e}")
                await asyncio.sleep(1)

if __name__ == "__main__":
    try:
        asyncio.run(main_poller())
    except KeyboardInterrupt:
        print("\n→ Shutting down gracefully...")