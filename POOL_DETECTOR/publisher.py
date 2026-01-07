import redis
import requests
import time
import json

REDIS_HOST = '20.46.50.39'
REDIS_PORT = 6379
CHANNEL_NAME = 'start-work'
SOLANA_RPC_URL = "https://api.mainnet-beta.solana.com"

def get_latest_slot():
    try:
        resp = requests.post(SOLANA_RPC_URL, json={"jsonrpc":"2.0", "id":1, "method":"getSlot"}, timeout=5)
        return resp.json().get('result')
    except:
        return None

def publish():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    slot = get_latest_slot()
    if not slot:
        print("Failed to get slot.")
        return

    # Set start time to 6 seconds in the future
    start_time = time.time() + 9.0

    # We rewind 10 blocks just to be safe/catch up on recent history
    target_slot = slot + 10

    message = json.dumps({
        "slot": target_slot,
        "timestamp": start_time
    })

    count = r.publish(CHANNEL_NAME, message)

    print(f"🚀 COMMAND SENT")
    print(f"Target Start Slot: {target_slot}")
    print(f"Target Start Time: {start_time} (in 6.0s)")
    print(f"Workers Notified:  {count}")

if __name__ == "__main__":
    publish()
