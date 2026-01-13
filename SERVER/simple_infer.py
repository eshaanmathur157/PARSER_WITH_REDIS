import psycopg2
import pandas as pd
import torch
import torch.nn as nn
import numpy as np
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, MaxAbsScaler
import warnings
import sys
import redis
import requests
import json
import time

# Suppress warnings
warnings.filterwarnings('ignore')

# --- CONFIGURATION ---
DB_CONFIG = {
    "host": "localhost",
    "port": "4566",
    "user": "root",
    "dbname": "dev"
}
REDIS_HOST = '20.46.50.39'
REDIS_PORT = 6379
KEY_PAIR_TO_BASE = "PAIR_TO_BASE_VAULT" 

MODEL_PATH = "/home/proxy1/UBUNTUPARSER/SERVER/gan_discriminator_final3_2.pth"
SEQ_LEN = 3 
NUM_FEATURES = 17
HIDDEN_DIM = 64 

# Feature Ordering
FEATURE_ORDER = [
    "aggregated_buy_volume_usd", "aggregated_sell_volume_usd", "total_volume_usd",
    "max_liquidity", "min_liquidity", "price_max", "price_min",
    "price_delta", "price_std", "buy_price_std", "sell_price_std",
    "number_of_buys", "number_of_sells", "number_of_unique_buyers", "number_of_unique_sellers",
    "buy_perc", "sell_perc"
]

# Scaling Logic
SCALING_MAP = {
    "standard": ["price_delta", "price_std", "buy_price_std", "sell_price_std"],
    "minmax": ["aggregated_buy_volume_usd", "aggregated_sell_volume_usd", "total_volume_usd", 
               "max_liquidity", "min_liquidity", "price_max", "price_min"],
    "robust": ["number_of_buys", "number_of_sells", "number_of_unique_buyers", "number_of_unique_sellers"],
    "maxabs": ["buy_perc", "sell_perc"]
}

# --- MODEL DEFINITION ---
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

# --- HELPER FUNCTIONS ---

def get_redis_client():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        return r
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        sys.exit(1)

def build_reverse_map(r_client):
    """
    Fetches PAIR_TO_BASE_VAULT and creates a reverse map:
    { 'BaseVault': 'PoolAddress' }
    """
    try:
        standard_map = r_client.hgetall(KEY_PAIR_TO_BASE)
        reverse_map = {v: k for k, v in standard_map.items()}
        print(f"✅ Built reverse map with {len(reverse_map)} entries.")
        return reverse_map
    except Exception as e:
        print(f"⚠️ Failed to build reverse map: {e}")
        return {}

def get_all_base_vaults():
    """Fetches list of distinct poolIdentifiers (Base Vaults) from DB."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = "SELECT DISTINCT poolIdentifier FROM pool_master_5min;"
        df = pd.read_sql(query, conn)
        conn.close()
        return df['poolidentifier'].tolist() 
    except Exception as e:
        print(f"✗ Failed to fetch pool list: {e}")
        return []

def get_pool_data(base_vault):
    """Fetches 3 rows for a specific Base Vault."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        query = f"""
            SELECT * FROM pool_master_5min
            WHERE poolIdentifier = '{base_vault}'
            ORDER BY time ASC
            LIMIT {SEQ_LEN};
        """
        df = pd.read_sql(query, conn)
        conn.close()
        
        if len(df) < SEQ_LEN:
            return None
        return df
    except Exception:
        return None

def scale_features(df):
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
    try:
        ordered_data = df[FEATURE_ORDER].values
    except KeyError:
        return None
    tensor = torch.tensor(ordered_data, dtype=torch.float32)
    tensor = tensor.unsqueeze(0) 
    return tensor

def load_model():
    model = EnhancedDiscriminator(num_features=NUM_FEATURES, hidden_dim=HIDDEN_DIM)
    try:
        state_dict = torch.load(MODEL_PATH, map_location=torch.device('cpu'))
        model.load_state_dict(state_dict)
        model.eval()
        return model
    except Exception as e:
        print(f"✗ Error loading model: {e}")
        return None

def fetch_raydium_metadata(pool_addr):
    """
    Queries Raydium API for pool metadata.
    Maps Mint B -> Base, Mint A -> Quote (as requested).
    """
    url = f"https://api-v3.raydium.io/pools/key/ids?ids={pool_addr}"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            if data.get("success") and data.get("data"):
                pool_info = data["data"][0]
                
                mint_a = pool_info.get("mintA", {})
                mint_b = pool_info.get("mintB", {})
                vaults = pool_info.get("vault", {})

                # Mapping based on user request:
                # Base -> Mint B
                # Quote -> Mint A
                
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
    except Exception as e:
        # print(f"⚠️ Raydium API Error for {pool_addr}: {e}")
        pass
    
    return None

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    
    # 1. Connect to Redis & Build Reverse Map
    print("⏳ Connecting to Redis...")
    r = get_redis_client()
    vault_to_pool_map = build_reverse_map(r)

    # 2. Load Model
    print("⏳ Loading Model...")
    model = load_model()
    if not model:
        sys.exit(1)

    # 3. Get All Base Vaults from DB
    print("🔍 Fetching list of all Base Vaults from DB...")
    base_vaults = get_all_base_vaults()
    print(f"✅ Found {len(base_vaults)} vaults in DB. Starting inference...\n")
    print("-" * 60)

    # 4. Iterate and Predict
    valid_count = 0
    skipped_count = 0

    for b_vault in base_vaults:
        if not b_vault: continue

        # Fetch Data using Base Vault (poolIdentifier in DB)
        raw_df = get_pool_data(b_vault)
        
        if raw_df is not None:
            scaled_df = scale_features(raw_df)
            input_tensor = prepare_tensor(scaled_df)
            
            if input_tensor is not None:
                # Predict
                with torch.no_grad():
                    prediction = model(input_tensor)
                    score = prediction.item()
                
                # --- REVERSE LOOKUP ---
                pool_address = vault_to_pool_map.get(b_vault, None)
                
                if pool_address:
                    # --- FETCH RAYDIUM METADATA ---
                    metadata = fetch_raydium_metadata(pool_address)
                    
                    if metadata:
                        # Construct Final JSON
                        result_json = {
                            "pool_address": pool_address,
                            "confidence_score": round(score, 6),
                            "metadata": metadata
                        }
                        
                        # Print JSON
                        print(json.dumps(result_json, indent=4))
                        print("-" * 20)
                        valid_count += 1
                        
                        # Polite delay to avoid rate limiting Raydium
                        time.sleep(0.2)
                    else:
                        # If API fails, maybe print basic info?
                        print(f"⚠️ Metadata not found for {pool_address}")
                        skipped_count += 1
                else:
                    # print(f"⚠️ Unknown Pool Address for Vault: {b_vault}")
                    skipped_count += 1
            else:
                skipped_count += 1
        else:
            skipped_count += 1

    print("-" * 60)
    print(f"✅ Finished.")
    print(f"Processed: {valid_count}")
    print(f"Skipped:   {skipped_count} (Rows < 3, Missing Data, or API Fail)")