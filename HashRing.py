import hashlib
import json

class HashRing:
    def __init__(self, num_virtual_nodes=100):
        self.ring = {}
        self.nodes = set()
        self.num_virtual_nodes = num_virtual_nodes
    
    def add_node(self, node_id):
        if node_id in self.nodes:
            print("Node already in hash ring\n")
            return
        self.nodes.add(node_id)
        for i in range(self.num_virtual_nodes):
            hash_key = hashlib.sha256(f"{node_id}-{i}".encode()).hexdigest()
            self.ring[hash_key] = node_id # Map hash to physical node ID
    
    def remove_node(self, node_id):
        if node_id not in self.nodes:
            print("Non-existent node coudl not be removed from hash ring\n")
            return
        self.nodes.remove(node_id) # Remove from active nodes
        # Create new dict excluding all entries pointing to node_id
        for k, v in self.ring.items():
            if v != node_id:
                self.ring[k] = v

    def get_node(self, key):
        if not self.ring:
            print("ERROR: Hash ring is empty\n")
            return
        hash_key = hashlib.sha256(str(key).encode()).hexdigest()
        sorted_keys = sorted(self.ring.keys())
        # Find first node with hash greater than the key's hash
        for ring_key in sorted_keys:
            if hash_key <= ring_key:
                return self.ring[ring_key]
        return self.ring[sorted_keys[0]]
        
        
    def get_store(self, key):
        key_hash = hashlib.sha256(key.encode()).hexdigest()
        sorted_hashes = sorted(self.ring.keys())
        for hash_key in sorted_hashes:
            if key_hash < hash_key:
                return self.ring[hash_key]
        return self.ring[sorted_hashes[0]]
        
    '''
    def update_persist(self):
        with open(KV_LOG_FILE, 'w') as f:
            json.dump(kv_stores, f, indent=4) # Dump store elements into JSON
            f.flush()
    '''