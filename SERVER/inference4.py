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
import joblib 
from sklearn.preprocessing import StandardScaler, MinMaxScaler, RobustScaler, MaxAbsScaler
import warnings

# Suppress warnings
warnings.filterwarnings('ignore')

# ==============================================================================
# --- ⚙️ CONFIGURATION ---
# ==============================================================================
REDIS_HOST = '20.46.50.39'
REDIS_PORT = 6379

# HTTP Queue Config
QUEUE_SERVER_URL = "http://20.46.50.39:8080/new_pools"
UI_ENDPOINT = "http://20.46.50.39:8080/confidence_update"
MY_QUEUE_NAME = "inference_queue"

# RisingWave (Postgres) Connection Details
RW_HOST = "127.0.0.1" 
RW_PORT = "4566"
RW_USER = "root"
RW_DB   = "dev"

# --- MODEL 1: GAN DISCRIMINATOR ---
GAN_MODEL_PATH = "/home/proxy1/UBUNTUPARSER/SERVER/gan_discriminator_final3.pth"
GAN_SCALER_PATH = "/home/proxy1/UBUNTUPARSER/SERVER/scalers_t3.pkl"

# --- MODEL 2: AUTOENCODER LSTM ---
AE_MODEL_PATH = "/home/proxy1/UBUNTUPARSER/SERVER/AUTOENCODER/autoencoder_t3.pth"
AE_SCALER_PATH = "/home/proxy1/UBUNTUPARSER/SERVER/AUTOENCODER/scalers_t3.pkl"
AE_THRESHOLD = 0.195866
AE_RESULTS_FILE = "autoencoder_results.json"

# --- RUGCHECK CONFIG ---
RUGCHECK_BASE_URL = "http://api.rugcheck.xyz/v1/tokens/"
RUGCHECK_RESULTS_FILE = "rugcheckflags.json"

# --- SHARED DATA PARAMS ---
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

# --- FEATURE ENGINEERING ---
FEATURE_ORDER = [
    "aggregated_buy_volume_usd", "aggregated_sell_volume_usd", "total_volume_usd",
    "max_liquidity", "min_liquidity", "price_max", "price_min",
    "price_delta", "price_std", "buy_price_std", "sell_price_std",
    "number_of_buys", "number_of_sells", "number_of_unique_buyers", "number_of_unique_sellers",
    "buy_perc", "sell_perc"
]

# ==============================================================================
# --- 🧠 MODEL DEFINITIONS ---
# ==============================================================================

# --- 1. GAN Discriminator ---
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

# --- 2. Autoencoder LSTM ---
class AutoencoderLSTM(nn.Module):
    def __init__(self, input_size, hidden_size, output_size, dropout=0.25):
        super(AutoencoderLSTM, self).__init__()
        self.encoder = nn.LSTM(input_size, hidden_size, num_layers=2, batch_first=True, dropout=dropout)
        
        # Decoder input is just hidden_size (no skip connections)
        decoder_input_size = hidden_size
        self.decoder = nn.LSTM(decoder_input_size, hidden_size, num_layers=2, batch_first=True, dropout=dropout)
        self.fc = nn.Linear(hidden_size, output_size)

    def forward(self, x):
        # Encoder
        encoder_output, (hidden, cell) = self.encoder(x)
        # Global Context
        global_context = encoder_output[:, -1, :].unsqueeze(1).repeat(1, x.size(1), 1)
        # Decoder
        decoder_input = global_context
        op, _ = self.decoder(decoder_input, (hidden, cell))
        op = self.fc(op)
        return op

# ==============================================================================
# --- 🛠️ HELPER FUNCTIONS ---
# ==============================================================================

def load_resources():
    """Loads GAN model/scalers AND Autoencoder model/scalers."""
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    print(f"🔧 Loading resources on {device}...")

    # 1. Load GAN
    gan_model = EnhancedDiscriminator(NUM_FEATURES, HIDDEN_DIM).to(device)
    gan_scalers = None
    try:
        gan_model.load_state_dict(torch.load(GAN_MODEL_PATH, map_location=device))
        gan_model.eval()
        gan_scalers = joblib.load(GAN_SCALER_PATH)
        print("✅ GAN Model & Scalers loaded.")
    except Exception as e:
        print(f"❌ Failed to load GAN resources: {e}")
        return None, None, None, None

    # 2. Load Autoencoder
    ae_model = AutoencoderLSTM(NUM_FEATURES, HIDDEN_DIM, NUM_FEATURES).to(device)
    ae_scalers = None
    try:
        ae_model.load_state_dict(torch.load(AE_MODEL_PATH, map_location=device))
        ae_model.eval()
        ae_scalers = joblib.load(AE_SCALER_PATH)
        print("✅ Autoencoder Model & Scalers loaded.")
    except Exception as e:
        print(f"❌ Failed to load Autoencoder resources: {e}")
        # We proceed even if AE fails, just to keep GAN running
        return gan_model, gan_scalers, None, None

    return gan_model, gan_scalers, ae_model, ae_scalers

def apply_saved_scalers(df, scalers):
    """Generic function to apply a specific list of scalers to a DF."""
    try:
        # Fill missing cols
        for col in FEATURE_ORDER:
            if col not in df.columns:
                df[col] = 0.0
        
        # Convert to numpy
        data_np = df[FEATURE_ORDER].values.astype(np.float32)
        
        # Handle Inf/NaN
        if not np.all(np.isfinite(data_np)):
            data_np = np.nan_to_num(data_np, nan=0.0, posinf=1e30, neginf=-1e30)

        # Apply Transform
        scaled_data = data_np.copy()
        for scaler, indices in scalers:
            scaled_data[:, indices] = scaler.transform(data_np[:, indices])
            
        return scaled_data
    except Exception as e:
        print(f"⚠️ Scaling Error: {e}")
        return None

def prepare_tensor(data_np, device):
    if data_np is None: return None
    tensor = torch.tensor(data_np, dtype=torch.float32)
    return tensor.unsqueeze(0).to(device)

def append_to_json_file(filename, data_dict):
    """Thread-safe-ish append to JSON lines file."""
    try:
        with open(filename, "a") as f:
            f.write(json.dumps(data_dict) + "\n")
    except Exception as e:
        print(f"❌ File Write Error ({filename}): {e}")

# ==============================================================================
# --- 🌐 ASYNC FETCHERS ---
# ==============================================================================

async def fetch_pool_data_as_df(base_vault):
    conn_str = f"host={RW_HOST} port={RW_PORT} user={RW_USER} dbname={RW_DB}"
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
                if not rows: return None
                col_names = [desc.name for desc in cur.description]
                return pd.DataFrame(rows, columns=col_names)
    except Exception as e:
        print(f"[DB Error] {e}")
        return None

async def fetch_raydium_metadata(session, pool_addr):
    url = f"https://api-v3.raydium.io/pools/key/ids?ids={pool_addr}"
    try:
        async with session.get(url, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("success") and data.get("data"):
                    p = data["data"][0]
                    return {
                        "base_mint": p.get("mintB", {}).get("address"),
                        "base_name": p.get("mintB", {}).get("name"),
                        "base_symbol": p.get("mintB", {}).get("symbol"),
                        "base_logo": p.get("mintB", {}).get("logoURI"),
                        "base_vault": p.get("vault", {}).get("B"),
                        "quote_mint": p.get("mintA", {}).get("address"),
                        "quote_name": p.get("mintA", {}).get("name"),
                        "quote_symbol": p.get("mintA", {}).get("symbol"),
                        "quote_logo": p.get("mintA", {}).get("logoURI"),
                        "quote_vault": p.get("vault", {}).get("A")
                    }
    except Exception:
        pass
    return None

async def check_rugcheck_api(session, base_mint, pool_address):
    """Queries RugCheck.xyz and saves result to rugcheckflags.json."""
    if not base_mint: return
    
    url = f"{RUGCHECK_BASE_URL}{base_mint}/report"
    try:
        async with session.get(url, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                risks = data.get('risks', [])
                
                # Logic: Empty risks list = SAFE, else RUG_PULL
                status = "SAFE" if not risks else "RUG_PULL"
                
                # Write to file
                result_entry = {
                    "pool_address": pool_address,
                    "base_mint": base_mint,
                    "status": status,
                    "risk_count": len(risks),
                    "risks": risks 
                }
                append_to_json_file(RUGCHECK_RESULTS_FILE, result_entry)
                print(f"🔍 [RugCheck] {pool_address} -> {status}")
            else:
                print(f"⚠️ [RugCheck] API returned {resp.status} for {base_mint}")
    except Exception as e:
        print(f"❌ [RugCheck Error] {e}")

# ==============================================================================
# --- ⚙️ WORKER LOGIC ---
# ==============================================================================

async def process_pool_lifecycle(pool_data, http_session, resources):
    """
    Orchestrates the lifecycle: Redis -> Wait -> DB -> GAN -> Autoencoder -> RugCheck
    """
    gan_model, gan_scalers, ae_model, ae_scalers = resources
    
    pool_addr = pool_data.get('pool_address')
    base_vault = pool_data.get('base_vault')
    quote_vault = pool_data.get('quote_vault')
    base_mint = pool_data.get('base_mint')
    quote_mint = pool_data.get('quote_mint')
    
    # 1. Update Redis
    try:
        async with redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True) as r:
            pipe = r.pipeline()
            pipe.hset(KEY_PAIR_TO_BASE, pool_addr, base_vault)
            pipe.hset(KEY_PAIR_TO_QUOTE, pool_addr, quote_vault)
            if base_vault: pipe.sadd(KEY_BASE_VAULTS, base_vault)
            if quote_vault: pipe.sadd(KEY_QUOTE_VAULTS, quote_vault)
            if base_mint:  pipe.sadd(KEY_BASE_MINTS, base_mint)
            if quote_mint: pipe.sadd(KEY_QUOTE_MINTS, quote_mint)
            await pipe.execute()
    except Exception as e:
        print(f"❌ [Redis Error] {e}")

    # 2. Timer Log
    print(f"⏳ [Timer Started] Pool: {pool_addr} (Waiting 15 mins...)")
    
    # Wait
    await asyncio.sleep(15 * 60) 
    print(f"🔄 [Processing] Pool: {pool_addr}")
    
    # 3. Fetch Data
    raw_df = await fetch_pool_data_as_df(base_vault)
    if raw_df is None or len(raw_df) < SEQUENCE_LENGTH:
        print(f"⚠️ [Data Insufficient] {pool_addr}")
        return

    # --- EXECUTE MODELS ---
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    # A. GAN DISCRIMINATOR FLOW
    if gan_model and gan_scalers:
        try:
            gan_scaled = apply_saved_scalers(raw_df, gan_scalers)
            gan_tensor = prepare_tensor(gan_scaled, device)
            
            with torch.no_grad():
                gan_score = gan_model(gan_tensor).item()
            
            # Fetch Metadata & Post to UI
            metadata = await fetch_raydium_metadata(http_session, pool_addr)
            if metadata:
                gan_result = {
                    "pool_address": pool_addr,
                    "confidence_score": round(gan_score, 6),
                    "metadata": metadata
                }
                # Post to UI Endpoint (GAN Only)
                try:
                    async with http_session.post(UI_ENDPOINT, json=gan_result, timeout=5) as resp:
                        if resp.status == 200: 
                            print(f"✅ [GAN] Sent to UI: {gan_score:.4f}")
                        else:
                            print(f"⚠️ [UI Post Fail] Status: {resp.status}")
                except Exception as e:
                    print(f"⚠️ [UI Post Fail] {e}")
        except Exception as e:
            print(f"❌ [GAN Error] {e}")

    # B. AUTOENCODER FLOW
    if ae_model and ae_scalers:
        try:
            ae_scaled = apply_saved_scalers(raw_df, ae_scalers)
            ae_tensor = prepare_tensor(ae_scaled, device)
            
            with torch.no_grad():
                reconstructed = ae_model(ae_tensor)
                # MSE Loss Calculation
                loss = nn.MSELoss(reduction='none')(reconstructed, ae_tensor)
                # Mean over features and sequence (dim 1, 2)
                sample_loss = torch.mean(loss).item()
            
            # User Logic: Loss > Threshold = Safe (Anomaly), Else Rug Pull
            status = "SAFE" if sample_loss > AE_THRESHOLD else "RUG_PULL"
            
            ae_result = {
                "pool_address": pool_addr,
                "reconstruction_loss": sample_loss,
                "threshold": AE_THRESHOLD,
                "status": status
            }
            # Write to file (Do NOT post to UI)
            append_to_json_file(AE_RESULTS_FILE, ae_result)
            print(f"✅ [Autoencoder] Saved result: {status} (Loss: {sample_loss:.6f})")

        except Exception as e:
            print(f"❌ [AE Error] {e}")

    # C. RUGCHECK API FLOW
    if base_mint:
        # We fire this asynchronously but await it so it completes before task death
        await check_rugcheck_api(http_session, base_mint, pool_address=pool_addr)

# ==============================================================================
# --- 🚀 MAIN POLLER ---
# ==============================================================================

async def main_poller():
    print(f"🚀 Inference Service Started")
    print(f"   GAN Model: {GAN_MODEL_PATH}")
    print(f"   AE Model:  {AE_MODEL_PATH}")
    
    # Load Models Once
    resources = load_resources()
    if resources[0] is None:
        print("🚨 Critical: GAN Model failed to load. Exiting.")
        return

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                headers = {"X-Machine-Name": MY_QUEUE_NAME}
                async with session.get(QUEUE_SERVER_URL, headers=headers, timeout=5) as resp:
                    if resp.status == 200:
                        pools = await resp.json()
                        if pools:
                            print(f"📩 [Queue] Received {len(pools)} new pools.")
                            for pool in pools:
                                asyncio.create_task(process_pool_lifecycle(pool, session, resources))
                    else:
                        print(f"⚠️ Queue Server returned {resp.status}")

                await asyncio.sleep(1)
            except Exception as e:
                print(f"❌ [Poller Error] {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main_poller())
    except KeyboardInterrupt:
        print("\n→ Shutting down...")
