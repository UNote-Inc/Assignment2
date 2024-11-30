import logging
from flask import Flask, request, jsonify
import threading
from datetime import datetime
import json
import os
from HashRing import HashRing
import socket
import requests
import hashlib


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

hash_ring = HashRing(num_virtual_nodes=100)
hash_ring.add_node("127.0.0.1:5980")
hash_ring.add_node("127.0.0.1:5981")
hash_ring.add_node("127.0.0.1:5982")
# for store_id in range(num_stores): 
#     # Currently our store ids are 0,1,2
#     # hash_ring.add_node(store_id + 5980)
#     hash_ring.add_node(store_id)

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

    store_id = hash_ring.get_store(key) # --> "127.0.0.1:{port_number}"
    local_store_id = f"127.0.0.1:{app.config['PORT']}"

    if store_id != local_store_id:
        try:
            response = requests.get(f"http://{store_id}/get/{key}")
            return response.content, response.status_code
        except requests.exceptions.RequestException as e:
            return f"ERROR: Unable to reach server {store_id}.\nDetails: {e}\n", 500

    dictionary_index = int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3

    with kv_store_lock:
        if key in kv_stores[dictionary_index]:
            value = kv_stores[dictionary_index][key]
            return f"Value: {value}\nTime: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200
            # return ""
        else:
            return f"Key not Found.\n {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 404
            # return ""

@app.route('/post/<key>/<value>', methods=['POST'])
def add_value(key, value):
    if not key or not value:
        return f"ERROR: Key and value cannot be empty.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 400

    store_id = hash_ring.get_store(key)
    local_store_id = f"127.0.0.1:{app.config['PORT']}"
    if store_id != local_store_id:
        try:
            response = requests.post(f"http://{store_id}/post/{key}/{value}")
            return response.content, response.status_code
        except requests.exceptions.RequestException as e:
            return f"ERROR: Unable to reach server {store_id}.\nDetails: {e}\n", 500

    dictionary_index = int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3

    with kv_store_lock:
        if key in kv_stores[dictionary_index]:
            return f"ERROR: Key already exists.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 409
        kv_stores[dictionary_index][key] = value
        persist_to_file()
    return "" # Key added successfully
    
@app.route('/put/<key>/<value>', methods=['PUT'])
def update_value(key, value):
    if not key or not value:
        return f"ERROR: key and value can't be empty.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    
    store_id = hash_ring.get_store(key)
    local_store_id = f"127.0.0.1:{app.config['PORT']}"

    if store_id != local_store_id:
        try:
            response = requests.put(f"http://{store_id}/put/{key}/{value}")
            return response.content, response.status_code
        except requests.exceptions.RequestException as e:
            return f"ERROR: Unable to reach server {store_id}.\nDetails: {e}\n", 500
    
    dictionary_index = int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3

    with kv_store_lock:
        if key not in kv_stores[dictionary_index]:
            return f"ERROR: Key: '{key}' not found.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 404
        kv_stores[dictionary_index][key] = value
        persist_to_file()

    #app.logger.info statement to verify that the value was updated correctly
    return ""

@app.route('/delete/<key>', methods=['DELETE'])
def delete_value(key):
    if not key:
        return f"ERROR: must enter a nonempty key.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 400

    store_id = hash_ring.get_store(key)
    local_store_id = f"127.0.0.1:{app.config['PORT']}"
    
    if store_id != local_store_id:
        try:
            response = requests.delete(f"http://{store_id}/delete/{key}")
            return response.content, response.status_code
        except requests.exceptions.RequestException as e:
            return f"ERROR: Unable to reach server {store_id}.\nDetails: {e}\n", 500

    dictionary_index = int(hashlib.sha256(key.encode()).hexdigest(), 16) % 3
    
    with kv_store_lock:
        if key in kv_stores[dictionary_index]:
            del kv_stores[dictionary_index][key]
            persist_to_file()
            return ""
        else:
            return f"ERROR: Input key not in store.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 404
    return 

@app.route('/getall', methods=['GET'])
def get_all():
    data = []
    parent_port = f"127.0.0.1:5980"
    if f"127.0.0.1:{app.config['PORT']}" == parent_port:
        for node in hash_ring.nodes:
            if node != parent_port:
                try:
                    response = requests.get(f"http://{node}/getall")
                    if response.status_code == 200:
                        data.append(f"{node}:\n{response.text}")
                    else:
                        data.append(f"Error retrieving data from {node}: {response.status_code}")
                except requests.exceptions.RequestException as e:
                    return f"ERROR: Could not retrieve data from {node}\n Details: {e}"
            
    with kv_store_lock:
        for i, store in enumerate(kv_stores):
            store_data = ', '.join([f"{key}={value}" for key, value in store.items()])
            data.append(f"Store {i}: {store_data}")
    return "\n".join(data) + "\n" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "\n", 200

@app.route('/deleteall', methods=['DELETE'])
def delete_all():
    errors = []
    # local_store_id = f"127.0.0.1:{app.config['PORT']}"
    
    # if local_store_id in hash_ring.nodes:
    #     hash_ring.nodes.remove(local_store_id) # Make sure we remove it from the list of nodes to call deleteall on

    parent_port = f"127.0.0.1:5980"
    if f"127.0.0.1:{app.config['PORT']}" == parent_port:
        for node in hash_ring.nodes:
            if node != parent_port:
                try:
                    response = requests.delete(f"http://{node}/deleteall") # Node = address??
                    if response.status_code != 200:
                        errors.append(f"Node {node}: {response.text}")
                except requests.RequestException as e:
                    errors.append(f"Node {node}: {str(e)}")
            
    with kv_store_lock:
        for store in kv_stores:
            store.clear()
        persist_to_file()
            
    if errors:
        return jsonify({"error": "Failed to del from som store", "details": errors}), 500
    
    return f"Successfully deleted all key-values:\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200

# @app.route('/<operation>/<key>', methods=['GET', 'POST', 'PUT', 'DELETE'])
# def forward_request(operation, key):
#     # forward req to the right store
#     target_port = hash_ring.get_node(key)
#     target_url = f"http://127.0.0.1:{target_port}/{operation}/{key}/{value}"
    
#     method = request.method
#     try:
#         if method == 'GET':
#             response = requests.get(target_url)
#         elif method == 'POST':
#             value = request.args.get('value')
#             response = requests.post(target_url, params={'value': value})
#         elif method == 'PUT':
#             value = request.args.get('value')
#             response = requests.dele
#         elif method == 'DELETE':
#             value = request.args.get('value')
#         else:
        
if __name__ == '__main__':
    import sys
    load_from_file()
    port = int(sys.argv[1])
    app.config['PORT'] = port
    app.run("0.0.0.0", port=port, debug=True)
    # python3 KV.py 5980
    # python3 KV.py 5981
    # python3 KV.py 5982    