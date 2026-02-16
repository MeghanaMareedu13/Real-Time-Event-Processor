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
    num_events = st.slider("Number of Events to Simulate", 10, 200, 50)
    producers = st.number_input("Producer Tasks", 1, 5, 2)
    consumers = st.number_input("Consumer Workers", 1, 10, 4)
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

# Main Simulation Function
async def run_simulation(num_events, producers_count, consumers_count):
    # Setup Logger for UI
    handler = StreamlitLogHandler(log_area)
    handler.setFormatter(logging.Formatter('%(asctime)s | %(message)s', datefmt='%H:%M:%S'))
    logger = logging.getLogger("EventEngine")
    logger.addHandler(handler)

    # Generate Mock Events
    mock_events = [
        Event(
            type=random.choice(list(EventType)),
            payload={"data": random.randint(100, 999)},
            priority=random.randint(1, 5)
        ) for _ in range(num_events)
    ]

    processor = RealTimeProcessor(queue_size=20)
    start_time = time.perf_counter()
    
    # Run the Engine
    # We use a wrapper around run to update the UI
    consumers = [asyncio.create_task(processor.consumer(f"C-{i}")) for i in range(consumers_count)]
    
    chunk_size = len(mock_events) // producers_count
    producer_tasks = []
    for i in range(producers_count):
        chunk = mock_events[i*chunk_size : (i+1)*chunk_size]
        producer_tasks.append(asyncio.create_task(processor.producer(f"P-{i}", chunk)))

    # Monitor progress while producers and consumers work
    while not processor.queue.empty() or any(not p.done() for p in producer_tasks):
        done_count = processor.processed_count
        dur = time.perf_counter() - start_time
        tp = done_count / dur if dur > 0 else 0
        
        total_metric.metric("Events Processed", done_count)
        duration_metric.metric("Duration", f"{dur:.1f}s")
        throughput_metric.metric("Throughput", f"{tp:.1f} ev/s")
        
        prog = min(done_count / num_events, 1.0)
        progress_bar.progress(prog)
        status_text.text(f"Processing... {done_count}/{num_events}")
        
        # Simple stats update
        chart_area.bar_chart(pd.DataFrame({
            "Metric": ["Processed", "Remaining"],
            "Count": [done_count, num_events - done_count]
        }).set_index("Metric"))

        await asyncio.sleep(0.5)

    # Wait for completion
    await processor.queue.join()
    for c in consumers:
        c.cancel()
    
    # Final Update
    total_metric.metric("Events Processed", processor.processed_count)
    progress_bar.progress(1.0)
    status_text.success(f"Successfully processed {num_events} events!")
    logger.removeHandler(handler)

if run_button:
    asyncio.run(run_simulation(num_events, producers, consumers))
else:
    st.info("Adjust the settings in the sidebar and click 'Start Engine' to see the asynchronous processor in action.")
    st.image("https://img.shields.io/badge/Asyncio-Architecture-orange?style=for-the-badge")
