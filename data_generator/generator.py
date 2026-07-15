import time
import random
import secrets
import json
import requests

# FastAPI target endpoint
INGEST_URL = "http://localhost:8000/ingest"

# Helper to generate mock EVM-like hex addresses
def generate_eth_address():
    return "0x" + secrets.token_hex(20)

# Set up some static "known" entities to test our detection logic
MIXER_ADDRESS = "0x8576aCC5C05D6CE88f4d47ec659082987a03091D"  # Mock Tornado Cash
CENTRAL_EXCHANGE = "0x28105954da1738e12122b7636d17276980d90716" # Mock Exchange Cold Wallet

# Pre-generate a pool of normal users
user_pool = [generate_eth_address() for _ in range(100)]

def generate_transaction():
    """Generates a normal random transaction between users"""
    sender = random.choice(user_pool)
    receiver = random.choice(user_pool)
    while receiver == sender:
        receiver = random.choice(user_pool)
        
    return {
        "tx_hash": "0x" + secrets.token_hex(32),
        "block_number": random.randint(19000000, 19500000),
        "sender": sender,
        "receiver": receiver,
        "amount_eth": round(random.expovariate(1.0 / 1.5), 4),
        "gas_price_gwei": round(random.uniform(15.0, 80.0), 2),
        "timestamp": int(time.time()),
        "is_smart_contract": False
    }

def generate_malicious_mix():
    """Generates a transaction directly interacting with a privacy mixer"""
    sender = random.choice(user_pool)
    return {
        "tx_hash": "0x" + secrets.token_hex(32),
        "block_number": random.randint(19000000, 19500000),
        "sender": sender,
        "receiver": MIXER_ADDRESS, 
        "amount_eth": round(random.choice([1.0, 10.0, 100.0]), 1), 
        "gas_price_gwei": round(random.uniform(50.0, 120.0), 2), 
        "timestamp": int(time.time()),
        "is_smart_contract": True
    }

def generate_structuring_burst():
    """Generates a burst of transactions depicting a a source structuring funds out"""
    source = random.choice(user_pool)
    burst_transactions = []
    
    for _ in range(5):
        burst_transactions.append({
            "tx_hash": "0x" + secrets.token_hex(32),
            "block_number": random.randint(19000000, 19500000),
            "sender": source,
            "receiver": generate_eth_address(),
            "amount_eth": round(random.uniform(0.05, 0.2), 4), 
            "gas_price_gwei": round(random.uniform(30.0, 45.0), 2),
            "timestamp": int(time.time()),
            "is_smart_contract": False
        })
    return burst_transactions

if __name__ == "__main__":
    print("🚀 Connecting to Ingestion Gateway at:", INGEST_URL)
    print("Press Ctrl+C to stop transmitting.")
    
    try:
        while True:
            roll = random.random()
            tx_events = []
            
            if roll < 0.85:
                tx_events.append(generate_transaction())
            elif roll < 0.95:
                tx_events.append(generate_malicious_mix())
            else:
                tx_events.extend(generate_structuring_burst())
            
            for tx in tx_events:
                try:
                    # POST to FastAPI
                    response = requests.post(INGEST_URL, json=tx)
                    if response.status_code == 202:
                        print(f"✅ Dispatched Tx: {tx['tx_hash'][:12]}... -> Status: {response.json()['status']}")
                    else:
                        print(f"❌ Server Error {response.status_code}: {response.text}")
                except requests.exceptions.ConnectionError:
                    print("⚠️ Ingestion Gateway down. Retrying in 2 seconds...")
                    time.sleep(2)
                    break
                    
                time.sleep(0.4) 
                
    except KeyboardInterrupt:
        print("\nGenerator transmission stopped.")