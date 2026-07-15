import os
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import asyncio

app = FastAPI(title="Crypto Transaction Ingestion Gateway")

# Configuration from environment variables, fallback to localhost
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "raw-transactions"

# Global state for asynchronous Kafka producer
producer = None

class Transaction(BaseModel):
    tx_hash: str
    block_number: int
    sender: str
    receiver: str
    amount_eth: float
    gas_price_gwei: float
    timestamp: int
    is_smart_contract: bool

@app.on_event("startup")
async def startup_event():
    global producer
    # Initialize the async producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    # Attempt connection with retry logic if Kafka is still starting up
    retries = 5
    for i in range(retries):
        try:
            await producer.start()
            print("🚀 Successfully connected to Kafka Broker!")
            break
        except Exception as e:
            if i == retries - 1:
                print("❌ Failed to connect to Kafka Broker. Exiting.")
                raise e
            print(f"⚠️ Connection to Kafka failed (attempt {i+1}/{retries}). Retrying in 3s...")
            await asyncio.sleep(3)

@app.on_event("shutdown")
async def shutdown_event():
    global producer
    if producer:
        await producer.stop()
        print("🛑 Stopped Kafka Producer.")

@app.post("/ingest", status_code=202)
async def ingest_transaction(tx: Transaction):
    """
    Ingests a single raw blockchain transaction and dispatches it
    asynchronously to the raw-transactions Kafka topic.
    """
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka producer is not initialized.")
    
    try:
        # Convert Pydantic object to dictionary
        payload = tx.dict()
        # Non-blocking publish using aiokafka
        await producer.send_and_wait(KAFKA_TOPIC, payload)
        return {"status": "accepted", "tx_hash": tx.tx_hash}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to produce message: {str(e)}")

@app.get("/health")
async def health_check():
    return {"status": "healthy", "kafka_connected": producer is not None}