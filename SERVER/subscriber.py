import redis
import subprocess
import sys
import time
import asyncio
import aiohttp
import json # We'll need this to get the text
from datetime import datetime, timezone
from multiprocessing import shared_memory


# --- Shared Memory Configuration ---
SHM_NAME = "solana_json_shm"  # Same name must be used in C++
SHM_SIZE = 10 * 1024 * 1024  # 10MB
FLAG_OFFSET = 0
SIZE_OFFSET = 1
DATA_OFFSET = 9
# -----------------------------------

REDIS_HOST = '20.46.50.39'
REDIS_PORT = 6379
CHANNEL_NAME = 'start-work'

NUM_WORKERS = 6
RPC_URL = "https://api.mainnet-beta.solana.com"

# -------------------------
# MODIFIED WORKER LOGIC
# -------------------------

async def fetch_block(session, slot_num, request_id, worker_id, shm_buf):
    """
    Fetches the block and, on success, writes the data to shared memory.
    """
    payload = {
        "jsonrpc": "2.0",
        "id": request_id,
        "method": "getBlock",
        # Ask for full data to get a large response
        "params": [slot_num, {
            "transactionDetails": "full",
            "maxSupportedTransactionVersion": 0
        }]
    }

    try:
        start = asyncio.get_event_loop().time()
        async with session.post(RPC_URL, json=payload) as response:
            elapsed = (asyncio.get_event_loop().time() - start) * 1000
            now = datetime.now(timezone.utc)
            ts = now.strftime("%H:%M:%S") + f":{int(now.microsecond/1000):03d}"

            if response.status == 200:
                # Use response.text() to get the raw string
                json_string = await response.text()
                result = json.loads(json_string) # Check if it's a valid result

                if result.get("result") is not None:
                    print(f"[W {worker_id} | {ts}] Got slot {slot_num} ({elapsed:.1f} ms)")

                    # --- WRITE TO SHARED MEMORY ---
                    json_bytes = json_string.encode('utf-8')
                    data_size = len(json_bytes)

                    if data_size > (SHM_SIZE - DATA_OFFSET):
                        print(f"Error: JSON size ({data_size}) > SHM size")
                        return

                    # 1. Wait for consumer to clear the flag (flag == 0)
                    while shm_buf[FLAG_OFFSET] == 1:
                        print(f"[W {worker_id}] Waiting for consumer to read...")
                        await asyncio.sleep(0.05) # Poll gently

                    # 2. Write size (8 bytes, little-endian)
                    shm_buf[SIZE_OFFSET:DATA_OFFSET] = data_size.to_bytes(8, 'little')

                    # 3. Write JSON data
                    shm_buf[DATA_OFFSET:DATA_OFFSET + data_size] = json_bytes

                    # 4. Set flag to 1 (unread)
                    shm_buf[FLAG_OFFSET] = 1

                    print(f"[W {worker_id}] Wrote {data_size} bytes to SHM.")
                    # --------------------------------

                else:
                    print(f"[W {worker_id} | {ts}] Slot {slot_num} not ready ({elapsed:.1f} ms)")
            else:
                print(f"[W {worker_id} | {ts}] HTTP {response.status} ({elapsed:.1f} ms)")

    except Exception as e:
        now = datetime.now(timezone.utc)
        ts = now.strftime("%H:%M:%S") + f":{int(now.microsecond/1000):03d}"
        print(f"[W {worker_id} | {ts}] Error for slot {slot_num}: {e}")


async def run_worker_inline(worker_id, slot, shm_buf):
    print(f"[Worker {worker_id}] Started at slot {slot}")

    async with aiohttp.ClientSession() as session:
        request_id = 0

        # Run 5 times for this example
        while (1):
            now = datetime.now(timezone.utc)
            ts = now.strftime("%H:%M:%S") + f":{int(now.microsecond/1000):03d}"
            print(f"[Worker {worker_id} | {ts}] Sending request for slot {slot}")

            # Create the task, passing the shared memory buffer
            asyncio.create_task(fetch_block(session, slot, request_id, worker_id, shm_buf))

            slot += NUM_WORKERS
            request_id += 1

            await asyncio.sleep(2.4) # Original sleep

        await asyncio.sleep(10) # Wait for tasks to finish
        print("Worker run finished.")


# -------------------------
# SUBSCRIBER MAIN LOGIC
# -------------------------
def main():
    if len(sys.argv) < 2:
        print("Usage: python3 subscriber_mod.py <worker_id>")
        sys.exit(1)

    WORKER_ID = int(sys.argv[1])
    print(f"Subscriber started for Worker {WORKER_ID}")

    # --- Create or connect to Shared Memory ---
    try:
        # Create the shared memory block
        shm = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=SHM_SIZE)
        print(f"Created shared memory '{SHM_NAME}' ({SHM_SIZE} bytes)")
        # Initialize flag to 0 (read/empty)
        shm.buf[FLAG_OFFSET] = 0
    except FileExistsError:
        # Or attach to it if it already exists
        shm = shared_memory.SharedMemory(name=SHM_NAME, create=False)
        print(f"Attached to existing shared memory '{SHM_NAME}'")

    try:
        print(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}...")
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        p = r.pubsub()
        p.subscribe(CHANNEL_NAME)
        print(f"Listening on channel '{CHANNEL_NAME}' for starting slot...")

        for message in p.listen():
            if message['type'] == 'message':
                try:
                    data = message['data'].decode().split(',')
                    starting_slot = int(data[0])
                    start_time = float(data[1])

                    my_slot = starting_slot + WORKER_ID
                    my_delay = WORKER_ID * 0.4
                    my_start_time = start_time + my_delay
                    current_time = time.time()
                    wait_time = my_start_time - current_time

                    print(f"\nReceived starting slot: {starting_slot}")
                    print(f"Worker {WORKER_ID} will start from slot {my_slot}")
                    print(f"Waiting {wait_time:.2f}s...")

                    if wait_time > 0:
                        time.sleep(wait_time + 0.21)

                    # --- Run the producer logic ---
                    # We run this special worker to write to SHM
                    if WORKER_ID <= 5:
                        print("Running worker inline, will write to SHM...")
                        # Pass the shm buffer to the async worker
                        asyncio.run(run_worker_inline(WORKER_ID, my_slot, shm.buf))
                        print("Worker finished!")
                        break # Exit after one run
                    else:
                        print(f"This script is for worker 5. Worker {WORKER_ID} not running.")
                        break

                except Exception as e:
                    print(f"Error processing message: {e}")

    finally:
        # Clean up
        print("Cleaning up and closing shared memory.")
        shm.close()
        shm.unlink() # Destroy the shared memory block

if __name__ == "__main__":
    main()
