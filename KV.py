from flask import Flask, request, flash
import threading
from datetime import datetime
import json
import os

"""
Command Line

curl -X GET http://127.0.0.1:5000/get/<key>
curl -X GET http://127.0.0.1:5000/getall

curl -X POST http://127.0.0.1:5000/post/<key>/<value>

curl -X PUT http://127.0.0.1:5000/update/<key>/<value>

curl -X DELETE http://127.0.0.1:5000/delete/<key>
curl -X DELETE http://127.0.0.1:5000/deleteall
"""



app = Flask(__name__)

kv_store = {} # The dicitonary that will serve as our "store"
kv_store_lock = threading.Lock() # Locking mechanism to handle concurrent requests
KV_LOG_FILE = 'kv_store_log.txt' # File to store data as backup in case of app failure

def persist_to_file():
    with open(KV_LOG_FILE, 'w') as f:
        json.dump(kv_store, f, indent=4) # Dump store elements into JSON
        f.flush()

def load_from_file():
    global kv_store
    if os.path.exists(KV_LOG_FILE):
        with kv_store_lock:
            try:
                with open(KV_LOG_FILE, 'r') as f:
                    loaded_data = json.load(f)
                    if isinstance(loaded_data, dict): # Checks that the data loaded from JSON is of type dict
                        kv_store = loaded_data
                        print("Successfully loaded data from file")
                        return # Data successfully loaded
                    else:
                        print("ERROR: Data loaded from JSON is not a valid dictionary")
            except Exception as error:
                print(f"ERROR: An error occurred when loading from {KV_LOG_FILE}: {error}")

@app.route('/get/<key>', methods=['GET'])
def get_value(key):
    if not key:
        return f"ERROR: must enter a nonempty key.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 400

    with kv_store_lock:
        if key in kv_store:
            return f"Value: {kv_store[key]}.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200
        else:
            return f"Key not Found.\n {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 404

@app.route('/post/<key>/<value>', methods=['POST'])
def add_value(key, value):
    if not key or not value:
        return f"ERROR: Key and value cannot be empty.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 400

    if key in kv_store:
        return f"ERROR: Key already exists.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 409
    with kv_store_lock:
        kv_store[key] = value
        persist_to_file()
    
    return f"Successfully created KV pair: {key} -> {kv_store[key]}.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 201

@app.route('/update/<key>/<value>', methods=['PUT'])
def update_value(key, value):
    if not key or not value:
        return f"ERROR: key and value can't be empty.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
    
    with kv_store_lock:
        kv_store[key] = value
        persist_to_file()

    #Print statement to verify that the value was updated correctly
    return f"KV pair updated: {key} -> {kv_store[key]}.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200

@app.route('/delete/<key>', methods=['DELETE'])
def delete_value(key):
    if not key:
        return f"ERROR: must enter a nonempty key.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 400

    with kv_store_lock:
        if key in kv_store:
            del kv_store[key]
            persist_to_file()
            return f"Successfully deleted key: {key}.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200
        else:
            # flash("Input key not in store", category='error')
            return f"ERROR: Input key not in store.\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 404
    return 

@app.route('/getall', methods=['GET'])
def get_all():
    return ', '.join([f"{key}={value}" for key, value in kv_store.items()]) + "\n"

@app.route('/deleteall', methods=['DELETE'])
def delete_all():
    kv_store.clear()
    return f"Successfully deleted all key-values:\n Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n", 200

if __name__ == '__main__':
    load_from_file()
    app.run("0.0.0.0", port=5000)