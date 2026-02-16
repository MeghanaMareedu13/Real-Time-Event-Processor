import streamlit as st
import asyncio
import random
import time
import pandas as pd
from models import Event, EventType
from engine import RealTimeProcessor
import logging

# Configure Streamlit Page
st.set_page_config(page_title="Real-Time Event Engine", page_icon="⚡", layout="wide")

st.title("⚡ Real-Time Event Engine Monitor")
st.markdown("Visualizing high-performance asynchronous event processing in Python.")

# Persistent State Initialization
if 'processor' not in st.session_state:
    st.session_state.processor = RealTimeProcessor(queue_size=50)
if 'engine_active' not in st.session_state:
    st.session_state.engine_active = False
if 'pending_injections' not in st.session_state:
    st.session_state.pending_injections = []

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Engine Configuration")
    mode = st.radio("Select Mode", ["Simulation (Mock Data)", "Live Stream (Real Crypto Data)"])
    
    if mode == "Simulation (Mock Data)":
        num_events = st.slider("Number of Events to Simulate", 10, 200, 50)
    else:
        st.info("Live mode will stream real prices from CoinGecko every 10 seconds.")
        num_events = 9999 
        
    consumers = st.number_input("Consumer Workers", 1, 10, 4)
    
    col_start, col_stop = st.columns(2)
    if col_start.button("🚀 Start Engine", type="primary", use_container_width=True):
        st.session_state.engine_active = True
        st.session_state.processor.processed_count = 0 # Reset count on fresh start
    
    if col_stop.button("🛑 Stop Engine", use_container_width=True):
        st.session_state.engine_active = False
        st.rerun()

    st.divider()
    st.header("Manual Event Injector")
    with st.form("injection_form", clear_on_submit=True):
        inj_type = st.selectbox("Event Type", [et.value for et in EventType])
        inj_prio = st.slider("Priority", 1, 5, 3)
        inj_data = st.text_input("Payload Data", "Manual Entry")
        submitted = st.form_submit_button("💉 Inject Event")
        
        if submitted:
            new_event = Event(
                type=EventType(inj_type),
                payload={"message": inj_data, "source": "Manual Dashboard Injection"},
                priority=inj_prio
            )
            st.session_state.pending_injections.append(new_event)
            st.toast(f"Event buffered for injection!", icon="📥")

# UI Placeholders
metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
total_metric = metrics_col1.empty()
duration_metric = metrics_col2.empty()
throughput_metric = metrics_col3.empty()

progress_bar = st.progress(0)
status_text = st.empty()

log_col, chart_col = st.columns([2, 1])
with log_col:
    st.subheader("📝 Activity Log")
    log_area = st.empty()

with chart_col:
    st.subheader("📊 Statistics")
    chart_area = st.empty()

# Custom Class to capture logs for Streamlit
class StreamlitLogHandler(logging.Handler):
    def __init__(self, placeholder):
        super().__init__()
        self.placeholder = placeholder
        self.logs = []

    def emit(self, record):
        msg = self.format(record)
        self.logs.append(msg)
        if len(self.logs) > 15:
            self.logs.pop(0)
        self.placeholder.code("\n".join(self.logs))

# Main Simulation Function
async def run_simulation(num_events, consumers_count, mode):
    # Setup Logger for UI
    handler = StreamlitLogHandler(log_area)
    handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
    logger = logging.getLogger("EventEngine")
    if not any(isinstance(h, StreamlitLogHandler) for h in logger.handlers):
        logger.addHandler(handler)

    processor = st.session_state.processor
    start_time = time.perf_counter()
    
    # Run the Engine
    consumers = [asyncio.create_task(processor.consumer(f"C-{i}")) for i in range(consumers_count)]
    producer_tasks = []

    if mode == "Simulation (Mock Data)":
        mock_events = [
            Event(
                type=random.choice(list(EventType)),
                payload={"data": random.randint(100, 999)},
                priority=random.randint(1, 5)
            ) for _ in range(num_events)
        ]
        # Just use one fast producer for simulation
        producer_tasks.append(asyncio.create_task(processor.producer("SIM-PRODUCER", mock_events)))
    else:
        # Live Data Mode
        producer_tasks.append(asyncio.create_task(processor.real_data_producer("LIVE-COINGECKO")))

    # Monitor progress
    try:
        while st.session_state.engine_active:
            # CHECK FOR PENDING INJECTIONS
            if st.session_state.pending_injections:
                while st.session_state.pending_injections:
                    inj_event = st.session_state.pending_injections.pop(0)
                    await processor.queue.put(inj_event)
                    logger.info(f"Injecting Manual Event: {inj_event.event_id}")

            done_count = processor.processed_count
            dur = time.perf_counter() - start_time
            tp = done_count / dur if dur > 0 else 0
            
            total_metric.metric("Events Processed", done_count)
            duration_metric.metric("Duration", f"{dur:.1f}s")
            throughput_metric.metric("Throughput", f"{tp:.1f} ev/s")
            
            if mode == "Simulation (Mock Data)":
                prog = min(done_count / num_events, 1.0)
                progress_bar.progress(prog)
                status_text.text(f"Simulation Active... {done_count}/{num_events}")
                if done_count >= num_events and processor.queue.empty():
                    st.session_state.engine_active = False
                    break
            else:
                progress_bar.progress((done_count % 100) / 100)
                status_text.text(f"LIVE STREAMING ACTIVE... Total Processed: {done_count}")
            
            chart_area.bar_chart(pd.DataFrame({
                "Metric": ["Processed", "In Queue"],
                "Count": [done_count, processor.queue.qsize()]
            }).set_index("Metric"))

            await asyncio.sleep(0.5)
            
    except asyncio.CancelledError:
        pass
    finally:
        for c in consumers + producer_tasks:
            c.cancel()
        logger.removeHandler(handler)

if st.session_state.engine_active:
    asyncio.run(run_simulation(num_events, consumers, mode))
else:
    st.info("The engine is currently idle. Configure settings in the sidebar and click 'Start Engine' to begin.")
    st.image("https://img.shields.io/badge/Engine-Ready-blue?style=for-the-badge")
