import os
import json
import redis
import asyncio
from aiokafka import AIOKafkaConsumer
from graph_engine import BlockchainGraphEngine

# Constants matching our data generator
MIXER_ADDRESS = "0x8576aCC5C05D6CE88f4d47ec659082987a03091D"
RAW_TOPIC = "raw-transactions"

# Configuration environment fallbacks
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

async def main():
    print("🧠 Stream Processor starting up...")
    
    # 1. Initialize Redis Client for ultra-fast cache lookups
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        print("✅ Connected to Redis state cache.")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return

    # Seed Redis with our known high-risk mixer blacklist entry
    r.hset("blacklist_wallets", MIXER_ADDRESS, "Privacy Mixer (Tornado Cash Mock)")

    # 2. Initialize our CPU-efficient NetworkX Graph Engine
    graph_engine = BlockchainGraphEngine(mixer_address=MIXER_ADDRESS)

    # 3. Setup Async Kafka Consumer to read transaction stream
    try:
        consumer = AIOKafkaConsumer(
            RAW_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='aml-processor-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        await consumer.start()
        print(f"📥 Listening to Kafka topic: '{RAW_TOPIC}'...")
    except Exception as e:
        print(f"❌ Kafka consumer connection failed: {e}")
        return

    # 4. Continuous Real-Time Processing Loop
    try:
        async for message in consumer:
            tx = message.value
            sender = tx["sender"]
            receiver = tx["receiver"]
            amount = tx["amount_eth"]
            ts = tx["timestamp"]
            tx_hash = tx["tx_hash"]

            # Step A: Update the in-memory NetworkX Graph
            graph_engine.add_transaction(sender, receiver, amount, ts)

            # Step B: Fast-path Redis Blacklist evaluation
            is_blacklisted = r.hexists("blacklist_wallets", sender) or r.hexists("blacklist_wallets", receiver)

            # Step C: Graph structural analysis using BFS
            hops = graph_engine.calculate_distance_from_mixer(sender, max_hops=4)
            centrality = graph_engine.get_node_centrality(sender)

            # Step D: Behavioral feature metrics calculation (Simulating ML inputs)
            velocity_key = f"velocity:{sender}"
            r.rpush(velocity_key, ts)
            r.expire(velocity_key, 60) 
            
            tx_count_last_minute = r.llen(velocity_key)

            # Step E: Combine weights to form a Composite Compliance Risk Score (0-100)
            risk_score = 0
            reasons = []

            if is_blacklisted:
                risk_score = 100
                reasons.append("DIRECT_INTERACTION_WITH_BLACKLISTED_MIXER")
            else:
                if 0 < hops <= 2:
                    risk_score += 45
                    reasons.append(f"CLOSE_PROXIMITY_TO_MIXER ({hops} HOPS)")
                elif hops == 3:
                    risk_score += 25
                    reasons.append("MEDIUM_PROXIMITY_TO_MIXER (3 HOPS)")

                if tx_count_last_minute >= 4:
                    risk_score += 35
                    reasons.append(f"HIGH_VELOCITY_BURST_DETECTED ({tx_count_last_minute} tx/min)")
                
                if centrality > 0.05:
                    risk_score += 20
                    reasons.append("STRUCTURAL_NODE_DISTRIBUTION_HUB")

            risk_score = min(risk_score, 100)

            # Step F: Broadcast critical compliance system alerts if anomalous
            if risk_score >= 50:
                alert_payload = {
                    "tx_hash": tx_hash,
                    "sender": sender,
                    "receiver": receiver,
                    "amount_eth": amount,
                    "risk_score": risk_score,
                    "reasons": reasons,
                    "timestamp": ts
                }
                r.lpush("aml_alerts", json.dumps(alert_payload))
                r.ltrim("aml_alerts", 0, 99)
                
                print(f"⚠️  [ALERT] Risk Score: {risk_score}% | Tx: {tx_hash[:10]} | Reasons: {reasons}")
            else:
                print(f"ℹ️  [Processed] Risk Score: {risk_score}% | Tx: {tx_hash[:10]}")

    except KeyboardInterrupt:
        print("\n🛑 Stream Processor gracefully shutting down.")
    finally:
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())