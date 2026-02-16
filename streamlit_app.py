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

# Sidebar for configuration
with st.sidebar:
    st.header("⚙️ Engine Configuration")
    mode = st.radio("Select Mode", ["Simulation (Mock Data)", "Live Stream (Real Crypto Data)"])
    
    if mode == "Simulation (Mock Data)":
        num_events = st.slider("Number of Events to Simulate", 10, 200, 50)
    else:
        st.info("Live mode will stream real prices from CoinGecko every 10 seconds.")
        num_events = 9999 # Infinite-like for progress logic
        
    producers = st.number_input("Producer Tasks", 1, 5, 2 if mode == "Simulation" else 1)
    consumers = st.number_input("Consumer Workers", 1, 10, 4)
    
    st.divider()
    st.header("Manual Event Injector")
    with st.form("injection_form"):
        inj_type = st.selectbox("Event Type", [et.value for et in EventType])
        inj_prio = st.slider("Priority", 1, 5, 3)
        inj_data = st.text_input("Payload Data", "Manual Entry")
        inject_button = st.form_submit_button("💉 Inject Event")

    run_button = st.button("🚀 Start Engine", type="primary")

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

# Persistent Processor for manual injection
if 'processor' not in st.session_state:
    st.session_state.processor = RealTimeProcessor(queue_size=50)

# Implementation of Manual Injection
if inject_button:
    event = Event(
        type=EventType(inj_type),
        payload={"message": inj_data, "source": "Manual Dashboard Injection"},
        priority=inj_prio
    )
    # Since we can't easily wait for a running loop, we just drop it in the queue
    # This works if the engine is running in an async task
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            asyncio.run_coroutine_threadsafe(st.session_state.processor.queue.put(event), loop)
            st.toast(f"Event {event.event_id} Injected!", icon="✅")
        else:
            st.error("Engine is not running! Start the engine first to inject.")
    except Exception:
        # Fallback for sync context
        st.session_state.processor.queue.put_nowait(event)
        st.toast(f"Event Queued (Engine Offline)", icon="📥")

# Main Simulation Function
async def run_simulation(num_events, producers_count, consumers_count, mode):
    # Setup Logger for UI
    handler = StreamlitLogHandler(log_area)
    handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
    logger = logging.getLogger("EventEngine")
    # Avoid duplicate handlers
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
        chunk_size = max(1, len(mock_events) // producers_count)
        for i in range(producers_count):
            chunk = mock_events[i*chunk_size : (i+1)*chunk_size]
            producer_tasks.append(asyncio.create_task(processor.producer(f"P-{i}", chunk)))
    else:
        # Live Data Mode
        producer_tasks.append(asyncio.create_task(processor.real_data_producer("LIVE-COINGECKO")))

    # Monitor progress
    try:
        while True:
            done_count = processor.processed_count
            dur = time.perf_counter() - start_time
            tp = done_count / dur if dur > 0 else 0
            
            total_metric.metric("Events Processed", done_count)
            duration_metric.metric("Duration", f"{dur:.1f}s")
            throughput_metric.metric("Throughput", f"{tp:.1f} ev/s")
            
            if mode == "Simulation (Mock Data)":
                prog = min(done_count / num_events, 1.0)
                progress_bar.progress(prog)
                status_text.text(f"Processing... {done_count}/{num_events}")
                if done_count >= num_events and processor.queue.empty():
                    break
            else:
                progress_bar.progress((done_count % 100) / 100) # Cyclic progress for live
                status_text.text(f"Streaming Live Data... Total Processed: {done_count}")
            
            chart_area.bar_chart(pd.DataFrame({
                "Metric": ["Processed", "Queue Depth"],
                "Count": [done_count, processor.queue.qsize()]
            }).set_index("Metric"))

            await asyncio.sleep(0.5)
            
    except asyncio.CancelledError:
        pass
    finally:
        for c in consumers + producer_tasks:
            c.cancel()
        logger.removeHandler(handler)

if run_button:
    asyncio.run(run_simulation(num_events, producers, consumers, mode))
else:
    st.info("Adjust the settings in the sidebar and click 'Start Engine' to see the asynchronous processor in action.")
    st.image("https://img.shields.io/badge/Real--Time-Enabled-green?style=for-the-badge")

