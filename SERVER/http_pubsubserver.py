from flask import Flask, request, jsonify

app = Flask(__name__)

# --- Configuration ---
KNOWN_PROXIES = [f"proxy{i}" for i in range(1, 7)]  # proxy1...proxy6
# Added 'ui_queue' to the extra queues
EXTRA_QUEUES = ["inference_queue", "ingest_price_event_queue", "ui_queue"]

# Merge them so we can manage them all together
ALL_CLIENTS = KNOWN_PROXIES + EXTRA_QUEUES

# --- In-Memory Queues ---
# Dictionary mapping 'client_name' -> [list of payloads]
queues = {name: [] for name in ALL_CLIENTS}

# ---------------------------------------------------------
# 1. POOL UPDATES (From Pool Detector -> Broadcast to ALL)
# ---------------------------------------------------------
@app.route('/pool_update', methods=['POST'])
def pool_update():
    """
    Receives a new pool payload.
    Broadcasts this payload to EVERY queue (Proxies, Inference, Ingest, AND UI).
    """
    sender = request.headers.get('X-Machine-Name')
    payload = request.json
    
    if not payload:
        return jsonify({"error": "No JSON payload provided"}), 400

    print(f"[POST] Received Pool Update from {sender}. Broadcasting to {len(queues)} queues.")

    # Broadcast to ALL queues (including ui_queue)
    for client_name in queues:
        queues[client_name].append(payload)

    return jsonify({"status": "success", "message": "Broadcasted to all queues"}), 200


# ---------------------------------------------------------
# 2. CONFIDENCE UPDATES (From Inference.py -> UI Queue Only)
# ---------------------------------------------------------
@app.route('/confidence_update', methods=['POST'])
def confidence_update():
    """
    Receives an inference result (JSON with score & metadata).
    Appends this ONLY to the UI Queue.
    """
    payload = request.json
    if not payload:
        return jsonify({"error": "No JSON payload provided"}), 400

    # We only want the UI to see the confidence scores, 
    # the proxies/ingesters don't care about this.
    if "ui_queue" in queues:
        queues["ui_queue"].append(payload)
        print(f"[POST] Confidence Score received. Added to UI Queue.")
        return jsonify({"status": "success", "message": "Added to UI queue"}), 200
    else:
        return jsonify({"error": "UI Queue not configured"}), 500


# ---------------------------------------------------------
# 3. WORKER FETCH (For Proxies & Internal Services)
# ---------------------------------------------------------
@app.route('/new_pools', methods=['GET'])
def new_pools():
    """
    Standard fetch for proxies, inference_queue, and ingest_queue.
    """
    requester = request.headers.get('X-Machine-Name')

    if requester not in queues:
        return jsonify({"error": f"Unknown or missing client name: {requester}"}), 400

    data_to_send = list(queues[requester])
    queues[requester].clear()

    if len(data_to_send) > 0:
        print(f"[GET] {requester} retrieved {len(data_to_send)} items.")

    return jsonify(data_to_send), 200


# ---------------------------------------------------------
# 4. UI FETCH (Special Endpoint for the Frontend)
# ---------------------------------------------------------
@app.route('/rug_update', methods=['GET'])
def rug_update():
    """
    The UI calls this to get EVERYTHING in the 'ui_queue'.
    This will contain a mix of:
      1. New Pool JSONs (from pool_update)
      2. Inference/Confidence JSONs (from confidence_update)
    """
    target_queue = "ui_queue"
    
    if target_queue not in queues:
        return jsonify({"error": "UI Queue missing"}), 500

    # Retrieve and Clear
    data_to_send = list(queues[target_queue])
    queues[target_queue].clear()

    if len(data_to_send) > 0:
        print(f"[UI] Retrieved {len(data_to_send)} mixed updates.")

    return jsonify(data_to_send), 200


if __name__ == '__main__':
    print("Starting Queue Server on port 8080...")
    print(f"Managed Queues: {ALL_CLIENTS}")
    app.run(host='0.0.0.0', port=8080, threaded=False)