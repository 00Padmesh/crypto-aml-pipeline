# Real-Time Crypto AML & Transaction Monitoring Pipeline

A high-performance, containerized, distributed stream-processing pipeline designed to monitor blockchain transactions in real-time, detect compliance risks, and identify financial crime anomalies—such as interaction with privacy mixers or token structuring velocity patterns.

## 🏗️ Architecture Overview

The system is engineered as a decoupled microservices network using the following real-time workflow:

\`\`\`
[Data Generator] ──(HTTP POST)──> [FastAPI Ingestion Engine]
                                               │
                                       (Async Producer)
                                               │
                                               ▼
                                      [Apache Kafka Topic]
                                               │
                                       (Async Consumer)
                                               │
                                               ▼
[Redis Cache] <──(Sub-ms State Check)──> [Stream Processor] ──> [Streamlit UI Dashboard]
                                        (NetworkX Engine)
\`\`\`

1. **Ingestion Layer:** A lightweight **FastAPI** gateway accepts incoming JSON transaction logs and immediately dispatches them asynchronously to a message broker.
2. **Streaming Event Backbone:** **Apache Kafka** processes high-throughput raw transaction events, decoupling data producers from state analysis workers to handle traffic volatility.
3. **In-Memory Cache Layer:** **Redis** tracks sub-millisecond wallet blacklist statuses and manages temporal transaction counters to evaluate activity frequency.
4. **Graph & Analysis Engine:** A standalone stream worker runs a **NetworkX** directed graph configuration in memory. It tracks real-time ledger relationships via Breadth-First Search (BFS) to identify multi-hop pathways tracking back to illicit sources within \$N\$ degrees of separation.
5. **Surveillance Interface:** A live **Streamlit** dashboard polls the processing state memory layer to deliver active compliance updates and tabular metrics display.

---

## 🛠️ Technical Stack

* **Core Programming:** Python, C++ (Optional Native Bindings)
* **API Ingestion Framework:** FastAPI, Uvicorn, Pydantic, Requests
* **Asynchronous Streaming Backbone:** Apache Kafka (KRaft mode Engine), AIOKafka
* **State Management & Caching:** Redis (In-Memory Structures)
* **Graph Modeling & Network Traversal:** NetworkX
* **Data Visualization Interface:** Streamlit, Pandas, Matplotlib
* **Container Orchestration:** Docker, Docker Compose

---

## 🚀 Installation & Running Locally

### 1. Pre-requisites
Ensure you have **Docker**, **Docker Compose**, and **Python 3.10+** installed on your system.

### 2. Environment Infrastructure Setup
Spin up the decoupled Apache Kafka broker and Redis cache cluster containers in the background:
\`\`\`bash
docker compose up -d
\`\`\`

### 3. Virtual Environment Installation
Initialize a isolated workspace context and pull down all runtime requirements:
\`\`\`bash
python -m venv venv
.\`venv\Scripts\activate  # On Windows
pip install -r requirements.txt
\`\`\`

### 4. Running the Application Components
Open separate terminal instances (ensuring your virtual environment is active in each) and launch the microservices concurrently:

* **Terminal 1 (FastAPI Server):**
  \`\`\`bash
  uvicorn ingestion_api.main:app --reload --port 8000
  \`\`\`
* **Terminal 2 (Surveillance Stream Worker):**
  \`\`\`bash
  python stream_processor/processor.py
  \`\`\`
* **Terminal 3 (Mock Client Driver):**
  \`\`\`bash
  python data_generator/generator.py
  \`\`\`
* **Terminal 4 (Streamlit Web Operations Console):**
  \`\`\`bash
  streamlit run dashboard/app.py
  \`\`\`

---

## 🛡️ Analytical Compliance Detection Matrix

The surveillance engine assigns composite compliance threat scores (\$0 - 100\%\$) based on the interlocking rules processed dynamically by the graph processor:

| Trigger Vector Identifier | Threat Impact Allocation | Algorithmic Strategy |
| :--- | :---: | :--- |
| \`DIRECT_INTERACTION_WITH_BLACKLISTED_MIXER\` | **\$100\%\$** | Rapid Redis Set evaluation against illegal contract identities |
| \`CLOSE_PROXIMITY_TO_MIXER (1-2 HOPS)\` | **\$45\%\$** | Directed Graph NetworkX BFS path traversal analysis |
| \`HIGH_VELOCITY_BURST_DETECTED\` | **\$35\%\$** | Redis sliding window value temporal distribution assessment |
| \`STRUCTURAL_NODE_DISTRIBUTION_HUB\` | **\$20\%\$** | NetworkX Out-Degree Centrality network scoring metrics |
"@
