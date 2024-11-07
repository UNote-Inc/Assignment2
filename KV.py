import logging
from flask import Flask
import threading
from datetime import datetime
import json
import os
import hashlib
from HashRing import HashRing
import random

"""
Command Line

curl -X GET http://127.0.0.1:5980/get/<key>
curl -X GET http://127.0.0.1:5980/getall

curl -X POST http://127.0.0.1:5980/post/<key>/<value>

curl -X PUT http://127.0.0.1:5980/update/<key>/<value>

curl -X DELETE http://127.0.0.1:5980/delete/<key>
curl -X DELETE http://127.0.0.1:5980/deleteall
"""

app = Flask(__name__)
app.logger.setLevel(logging.DEBUG)
hash_ring = {}
num_stores = 3
kv_stores = [{} for _ in range(num_stores)]
kv_store_lock = threading.Lock() # Locking mechanism to handle concurrent requests
KV_LOG_FILE = 'kv_store_log.txt' # File to store data as backup in case of app failure

def add_store_to_ring(store_id, num_virtual_nodes=100):
    for i in range(num_virtual_nodes):
        hash_key = hashlib.sha256(f"{store_id}-{i}".encode()).hexdigest()
        hash_ring[hash_key] = store_id

# hash_ring = HashRing(num_virtual_nodes=100)
for store_id in range(num_stores):
    add_store_to_ring(store_id)

def lookup_store_from_key(key):
    key_hash = hashlib.sha256(key.encode()).hexdigest()
    sorted_hashes = sorted(hash_ring.keys())
    app.logger.info(f"Key '{key}' hashed to {key_hash}")
    for hash_key in sorted_hashes:
        if key_hash < hash_key:
            app.logger.info(f"Key '{key}' assigned to store {hash_ring[hash_key]}")
            return hash_ring[hash_key]
    app.logger.info(f"Key '{key}' wrapped around to store {hash_ring[sorted_hashes[0]]}")
    return hash_ring[sorted_hashes[0]]

def persist_to_file():
    with open(KV_LOG_FILE, 'w') as f:
        json.dump(kv_stores, f, indent=4) # Dump store elements into JSON
        f.flush()

def load_from_file():
    global kv_stores
    if os.path.exists(KV_LOG_FILE):
        with kv_store_lock:
            try:
                with open(KV_LOG_FILE, 'r') as f:
                    loaded_data = json.load(f)
                    if isinstance(loaded_data, list) and len(loaded_data) == num_stores: # Checks that the data loaded from JSON is of type dict
                        kv_stores = loaded_data
                        app.logger.info("Successfully loaded data from file")
                        return # Data successfully loaded
                    else:
                        app.logger.info("ERROR: Data loaded from JSON is not a valid list")
            except Exception as error:
                app.logger.info(f"ERROR: An error occurred when loading from {KV_LOG_FILE}: {error}")

@app.route('/get/<key>', methods=['GET'])
def get_value(key):
    if not key:
        return f"ERROR: must enter a nonempty key.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 400

    # store_id = hash_ring.get_store(key)
    store_id = lookup_store_from_key(key)
    with kv_store_lock:
        if key in kv_stores[store_id]:
            return f"Value: {kv_stores[store_id][key]}.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200
        else:
            return f"Key not Found.\n {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 404

@app.route('/post/<key>/<value>', methods=['POST'])
def add_value(key, value):
    if not key or not value:
        return f"ERROR: Key and value cannot be empty.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 400

    # store_id = hash_ring.get_store(key)
    store_id = lookup_store_from_key(key)
    
    with kv_store_lock:
        if key in kv_stores[store_id]:
            return f"ERROR: Key already exists.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 409
        kv_stores[store_id][key] = value
        persist_to_file()
    return f"Successfully created KV pair: {key} -> {kv_stores[store_id][key]}.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 201

@app.route('/update/<key>/<value>', methods=['PUT'])
def update_value(key, value):
    if not key or not value:
        return f"ERROR: key and value can't be empty.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    
    # store_id = hash_ring.get_store(key)
    store_id = lookup_store_from_key(key)
    with kv_store_lock:
        if key not in kv_stores[store_id]:
            return f"ERROR: Key: '{key}' not found.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 404
        
        kv_stores[store_id][key] = value
        persist_to_file()

    #app.logger.info statement to verify that the value was updated correctly
    return f"KV pair updated: {key} -> {kv_stores[store_id][key]}.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200

@app.route('/delete/<key>', methods=['DELETE'])
def delete_value(key):
    if not key:
        return f"ERROR: must enter a nonempty key.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 400

    # store_id = hash_ring.get_store(key)
    store_id = lookup_store_from_key(key)
    with kv_store_lock:
        if key in kv_stores[store_id]:
            del kv_stores[store_id][key]
            persist_to_file()
            return f"Successfully deleted key: {key}.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200
        else:
            return f"ERROR: Input key not in store.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 404
    return 

@app.route('/getall', methods=['GET'])
def get_all():
    data = []
    with kv_store_lock:
        for i, store in enumerate(kv_stores):
            store_data = ', '.join([f"{key}={value}" for key, value in store.items()])
            data.append(f"Store {i}: {store_data}")
    return "\n".join(data) + "\n" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n", 200

@app.route('/deleteall', methods=['DELETE'])
def delete_all():
    with kv_store_lock:
        for store in kv_stores:
            store.clear()
        persist_to_file()
    return f"Successfully deleted all key-values:\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200

if __name__ == '__main__':
    load_from_file()
    app.run("0.0.0.0", port=5980, debug=True)