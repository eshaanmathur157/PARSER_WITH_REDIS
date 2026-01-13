import asyncio
import json
import time
import aiohttp
import redis.asyncio as redis
import psycopg
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, MaxAbsScaler
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

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
MODEL_PATH = "/home/proxy1/UBUNTUPARSER/SERVER/gan_discriminator_final3.pth"
NUM_FEATURES = 17
HIDDEN_DIM = 64
SEQUENCE_LENGTH = 3  

# --- REDIS KEYS ---
KEY_PAIR_TO_BASE = "PAIR_TO_BASE_VAULT"
KEY_PAIR_TO_QUOTE = "PAIR_TO_QUOTE_VAULT"
KEY_BASE_VAULTS = "BASE_VAULTS"
KEY_QUOTE_VAULTS = "QUOTE_VAULTS"
KEY_BASE_MINTS = "BASE_MINTS"
KEY_QUOTE_MINTS = "QUOTE_MINTS"

# --- Feature Engineering Config ---
FEATURE_ORDER = [
    "aggregated_buy_volume_usd", "aggregated_sell_volume_usd", "total_volume_usd",
    "max_liquidity", "min_liquidity", "price_max", "price_min",
    "price_delta", "price_std", "buy_price_std", "sell_price_std",
    "number_of_buys", "number_of_sells", "number_of_unique_buyers", "number_of_unique_sellers",
    "buy_perc", "sell_perc"
]

SCALING_MAP = {
    "standard": ["price_delta", "price_std", "buy_price_std", "sell_price_std"],
    "minmax": ["aggregated_buy_volume_usd", "aggregated_sell_volume_usd", "total_volume_usd", 
               "max_liquidity", "min_liquidity", "price_max", "price_min"],
    "robust": ["number_of_buys", "number_of_sells", "number_of_unique_buyers", "number_of_unique_sellers"],
    "maxabs": ["buy_perc", "sell_perc"]
}

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

# Load Weights
try:
    state_dict = torch.load(MODEL_PATH, map_location=device)
    model.load_state_dict(state_dict)
    model.eval()
    print(f"✅ Model loaded from {MODEL_PATH}")
except Exception as e:
    print(f"❌ Failed to load model: {e}")

# --- 2. Helper Functions (Scaling & Tensor) ---

def scale_features(df):
    """Applies specific scalers to specific columns (Synchronous CPU op)."""
    scaled_df = df.copy()
    
    if SCALING_MAP["standard"]:
        scaler = StandardScaler()
        scaled_df[SCALING_MAP["standard"]] = scaler.fit_transform(df[SCALING_MAP["standard"]].fillna(0))

    if SCALING_MAP["minmax"]:
        scaler = MinMaxScaler()
        scaled_df[SCALING_MAP["minmax"]] = scaler.fit_transform(df[SCALING_MAP["minmax"]].fillna(0))

    if SCALING_MAP["robust"]:
        scaler = RobustScaler()
        scaled_df[SCALING_MAP["robust"]] = scaler.fit_transform(df[SCALING_MAP["robust"]].fillna(0))

    if SCALING_MAP["maxabs"]:
        scaler = MaxAbsScaler()
        scaled_df[SCALING_MAP["maxabs"]] = scaler.fit_transform(df[SCALING_MAP["maxabs"]].fillna(0))
        
    return scaled_df

def prepare_tensor(df):
    """Reorders columns and converts to Tensor."""
    try:
        # Ensure we only pick the columns we trained on, in the right order
        ordered_data = df[FEATURE_ORDER].values
    except KeyError as e:
        print(f"Column missing: {e}")
        return None

    tensor = torch.tensor(ordered_data, dtype=torch.float32)
    # Add batch dimension: (1, Seq_Len, Features)
    tensor = tensor.unsqueeze(0).to(device)
    return tensor

# --- 3. Async Data Fetching ---

async def fetch_pool_data_as_df(base_vault):
    """
    Queries 'pool_master_5min' and returns a Pandas DataFrame.
    """
    conn_str = f"host={RW_HOST} port={RW_PORT} user={RW_USER} dbname={RW_DB}"
    
    # Using the master view as requested
    query = f"""
        SELECT * FROM pool_master_5min
        WHERE poolIdentifier = %s
        ORDER BY time ASC
        LIMIT {SEQUENCE_LENGTH};
    """
    
    try:
        async with await psycopg.AsyncConnection.connect(conn_str) as aconn:
            async with aconn.cursor() as cur:
                await cur.execute(query, (base_vault,))
                rows = await cur.fetchall()
                
                if not rows:
                    return None
                
                # Get column names dynamically from the cursor description
                col_names = [desc.name for desc in cur.description]
                
                # Create DataFrame
                df = pd.DataFrame(rows, columns=col_names)
                return df

    except Exception as e:
        print(f"[DB Error] Failed to fetch data for {base_vault}: {e}")
        return None

async def fetch_raydium_metadata(session, pool_addr):
    """
    Queries Raydium API asynchronously.
    Maps Mint B -> Base, Mint A -> Quote.
    """
    url = f"https://api-v3.raydium.io/pools/key/ids?ids={pool_addr}"
    try:
        async with session.get(url, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("success") and data.get("data"):
                    pool_info = data["data"][0]
                    
                    mint_a = pool_info.get("mintA", {})
                    mint_b = pool_info.get("mintB", {})
                    vaults = pool_info.get("vault", {})

                    return {
                        "base_mint": mint_b.get("address"),
                        "base_name": mint_b.get("name"),
                        "base_symbol": mint_b.get("symbol"),
                        "base_logo": mint_b.get("logoURI"),
                        "base_vault": vaults.get("B"),
                        
                        "quote_mint": mint_a.get("address"),
                        "quote_name": mint_a.get("name"),
                        "quote_symbol": mint_a.get("symbol"),
                        "quote_logo": mint_a.get("logoURI"),
                        "quote_vault": vaults.get("A")
                    }
    except Exception:
        # Silently fail for API glitches, handled in main logic
        pass
    return None

# --- 4. The Orchestrator (Worker Task) ---
async def process_pool_lifecycle(pool_data, http_session):
    """
    1. Update Redis Maps AND Sets (Instant)
    2. Wait 15 Minutes
    3. Query 'pool_master_5min' -> Scale -> Predict
    4. Fetch Metadata -> Print JSON
    """
    pool_addr = pool_data.get('pool_address')
    base_vault = pool_data.get('base_vault')
    quote_vault = pool_data.get('quote_vault')
    base_mint = pool_data.get('base_mint')
    quote_mint = pool_data.get('quote_mint')
    
    print(f"⏳ [Timer Started] Pool: {pool_addr} (Waiting 15 mins...)")
    
    # --- REDIS UPDATE ---
    try:
        async with redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True) as r:
            pipeline = r.pipeline()
            pipeline.hset(KEY_PAIR_TO_BASE, pool_addr, base_vault)
            pipeline.hset(KEY_PAIR_TO_QUOTE, pool_addr, quote_vault)
            
            if base_vault: pipeline.sadd(KEY_BASE_VAULTS, base_vault)
            if quote_vault: pipeline.sadd(KEY_QUOTE_VAULTS, quote_vault)
            if base_mint:  pipeline.sadd(KEY_BASE_MINTS, base_mint)
            if quote_mint: pipeline.sadd(KEY_QUOTE_MINTS, quote_mint)
            
            await pipeline.execute()
            print(f"✅ [Redis] Linked {pool_addr} -> {base_vault}")

    except Exception as e:
        print(f"❌ [Redis Error] Failed to update Redis: {e}")

    # 1. Wait 15 Minutes
    await asyncio.sleep(15 * 60) 
    
    print(f"🔄 [Processing] Pool: {pool_addr}")
    
    # 2. Query Data (pool_master_5min)
    raw_df = await fetch_pool_data_as_df(base_vault)
    
    if raw_df is None or len(raw_df) < SEQUENCE_LENGTH:
        print(f"⚠️ [Data Insufficient] {pool_addr} (Rows: {len(raw_df) if raw_df is not None else 0})")
        return

    # 3. Scale and Prepare
    try:
        scaled_df = scale_features(raw_df)
        input_tensor = prepare_tensor(scaled_df)
        
        if input_tensor is None:
            print(f"⚠️ [Tensor Error] Could not build tensor for {pool_addr}")
            return

        # 4. Inference
        with torch.no_grad():
            score = model(input_tensor).item()

        # 5. Fetch Raydium Metadata
        metadata = await fetch_raydium_metadata(http_session, pool_addr)

        if metadata:
            result = {
                "pool_address": pool_addr,
                "confidence_score": round(score, 6),
                "metadata": metadata
            }
            # The Final JSON Print
            print(json.dumps(result, indent=4))
            try:
                async with http_session.post(
                    "http://20.46.50.39:8080/confidence_update", 
                    json=result,
                    timeout=5
                ) as ui_resp:
                    if ui_resp.status == 200:
                        print("✅ Sent to UI Queue")
            except Exception as e:
                print(f"⚠️ Failed to send to UI: {e}")

    except Exception as e:
        print(f"❌ [Processing Error] {pool_addr}: {e}")

# --- 5. Main Poller ---
async def main_poller():
    print(f"🚀 Inference Service Polling: {QUEUE_SERVER_URL}")
    print(f"🏷️  Queue Name: {MY_QUEUE_NAME}")
    
    # Shared session for both Polling AND Raydium Metadata fetching
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                # Poll the HTTP Queue Server
                headers = {"X-Machine-Name": MY_QUEUE_NAME}
                async with session.get(QUEUE_SERVER_URL, headers=headers, timeout=5) as resp:
                    if resp.status == 200:
                        pools = await resp.json()
                        
                        if pools:
                            print(f"📩 [Received] {len(pools)} new pools from queue.")
                            for pool in pools:
                                # Pass the session so the worker can fetch Raydium data
                                asyncio.create_task(process_pool_lifecycle(pool, session))
                        
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