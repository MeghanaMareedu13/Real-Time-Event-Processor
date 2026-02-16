import asyncio
import logging
import time
from models import Event, EventType
from typing import List

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger("EventEngine")

class RealTimeProcessor:
    """
    High-performance event processor using Asyncio for non-blocking I/O.
    Simulates a high-throughput ingestion system.
    """
    def __init__(self, queue_size: int = 100):
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.processed_count = 0
        self.start_time = None

    async def producer(self, name: str, events: List[Event]):
        """Simulates an upstream data source (e.g., Kafka, Webhooks)"""
        for event in events:
            await self.queue.put(event)
            logger.debug(f"Producer {name} ingested event {event.event_id}")
            # Simulate slight delay between incoming events
            await asyncio.sleep(0.1)
        
        logger.info(f"Producer {name} finished ingesting {len(events)} events.")

    async def real_data_producer(self, name: str):
        """Fetches REAL data from a public API and streams it into the queue"""
        import httpx
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,cardano&vs_currencies=usd"
        logger.info(f"Producer {name} started LIVE streaming from CoinGecko...")
        
        while True:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(url, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        for coin, price in data.items():
                            event = Event(
                                type=EventType.DATA_INGEST,
                                payload={"coin": coin, "price": price['usd'], "source": "CoinGecko API"},
                                priority=3
                            )
                            await self.queue.put(event)
                            logger.info(f"Live Ingest: {coin.upper()} is currently ${price['usd']}")
                    
                    # Wait for 10 seconds before next poll to avoid rate limits
                    await asyncio.sleep(10)
            except Exception as e:
                logger.error(f"Live Stream Error: {e}")
                await asyncio.sleep(5)


    async def consumer(self, name: str):
        """Simulates a worker processing events (e.g., writing to DB, alerting)"""
        while True:
            # Get an event from the queue
            event = await self.queue.get()
            
            # Simulate "Processing" work
            process_start = time.perf_counter()
            
            try:
                # Logic based on event type
                if event.type == EventType.SYSTEM_ALERT:
                    logger.warning(f"Worker {name} [CRITICAL]: Handling alert {event.event_id}")
                else:
                    logger.info(f"Worker {name} [OK]: Processing {event.type} {event.event_id}")
                
                # Simulate I/O bound task (database write)
                await asyncio.sleep(0.05)
                
                self.processed_count += 1
            except Exception as e:
                logger.error(f"Worker {name} [ERROR]: Failed to process event {event.event_id}: {e}")
            finally:
                # Notify the queue that the item is done
                self.queue.task_done()
                latency = (time.perf_counter() - process_start) * 1000
                logger.debug(f"Event {event.event_id} latency: {latency:.2f}ms")

    async def run(self, producers_count: int, consumers_count: int, mock_events: List[Event]):
        self.start_time = time.perf_counter()
        logger.info(f"Starting Engine with {producers_count} Producers and {consumers_count} Consumers...")

        # Create consumer tasks (workers running in background)
        consumers = [asyncio.create_task(self.consumer(f"C-{i}")) for i in range(consumers_count)]

        # Chunk events for producers
        chunk_size = len(mock_events) // producers_count
        producer_tasks = []
        for i in range(producers_count):
            events_chunk = mock_events[i*chunk_size : (i+1)*chunk_size]
            producer_tasks.append(asyncio.create_task(self.producer(f"P-{i}", events_chunk)))

        # Wait for all producers to finish
        await asyncio.gather(*producer_tasks)

        # Wait until the queue is fully processed
        await self.queue.join()

        # Stop consumers (optional here, since it's a demo script we just exit)
        for c in consumers:
            c.cancel()

        duration = time.perf_counter() - self.start_time
        throughput = self.processed_count / duration
        logger.info("=========================================")
        logger.info(f"Processing Complete!")
        logger.info(f"Total Processed: {self.processed_count} events")
        logger.info(f"Total Duration: {duration:.2f} seconds")
        logger.info(f"Throughput: {throughput:.2f} events/sec")
        logger.info("=========================================")
