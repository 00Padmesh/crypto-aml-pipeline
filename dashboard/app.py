import streamlit as st
import redis
import json
import time
import pandas as pd

# Page setup
st.set_page_config(page_title="Gemini AML Surveillance Dashboard", page_icon="🛡️", layout="wide")

st.title("🛡️ Real-Time Crypto AML & Transaction Monitoring Dashboard")
st.markdown("---")

# Initialize Redis Connection
@st.cache_resource
def get_redis_client():
    return redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

try:
    r = get_redis_client()
    r.ping()
    st.sidebar.success("✅ Connected to Real-Time Data Pipeline")
except Exception as e:
    st.sidebar.error(f"❌ Connection failed: {e}")

# Metric summary configuration
col1, col2, col3 = st.columns(3)
alert_count_slot = col1.empty()
high_risk_volume_slot = col2.empty()
status_slot = col3.empty()

st.subheader("🚨 Live Compliance Violations & Alerts")
table_slot = st.empty()

# Persistent tracking lists for aggregate visualization metrics
all_alerts = []

# Real-time polling UI refresh loop
while True:
    # Pull raw alert frames from Redis queue list
    raw_alerts = r.lrange("aml_alerts", 0, -1)
    
    if raw_alerts:
        current_alerts = [json.loads(a) for a in raw_alerts]
        
        # Calculate performance statistics
        total_alerts = len(current_alerts)
        total_eth = sum(tx['amount_eth'] for tx in current_alerts)
        
        # Render top dashboard metrics
        alert_count_slot.metric(label="Total Flagged Alerts", value=total_alerts)
        high_risk_volume_slot.metric(label="Flagged High-Risk Volume (ETH)", value=f"{total_eth:.2f} ETH")
        status_slot.metric(label="Pipeline Performance", value="Processing Stream Logs")
        
        # Transform into organized structure for interactive DataFrame
        display_data = []
        for a in current_alerts:
            display_data.append({
                "Timestamp": time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(a['timestamp'])),
                "Transaction Hash": a['tx_hash'][:16] + "...",
                "Risk Score": f"{a['risk_score']}%",
                "Flag Reason": ", ".join(a['reasons']),
                "Amount (ETH)": a['amount_eth'],
                "Origin Wallet": a['sender'][:12] + "...",
                "Destination Wallet": a['receiver'][:12] + "..."
            })
            
        df = pd.DataFrame(display_data)
        
        # Inject custom conditional styles into the view container
        def color_risk(val):
            color = '#ff4b4b' if '100' in str(val) else '#ffa500'
            return f'background-color: {color}; color: white; font-weight: bold;'
            
        styled_df = df.style.map(color_risk, subset=['Risk Score'])
        table_slot.dataframe(styled_df, use_container_width=True, hide_index=True)
        
    else:
        alert_count_slot.metric(label="Total Flagged Alerts", value=0)
        high_risk_volume_slot.metric(label="Flagged High-Risk Volume (ETH)", value="0.00 ETH")
        status_slot.metric(label="Pipeline Performance", value="Awaiting Data Blocks...")
        table_slot.info("Listening for streaming ledger anomalies...")
        
    time.sleep(1) # Frequency clock tick rate