import asyncio
import random
from models import Event, EventType
from engine import RealTimeProcessor

async def main():
    # 1. Generate a large batch of mock events
    mock_events = []
    types = list(EventType)
    
    for i in range(50):
        mock_events.append(Event(
            type=random.choice(types),
            payload={"data": random.randint(100, 999), "user_id": f"U{i}"},
            priority=random.randint(1, 5)
        ))

    # 2. Initialize and Run the Processor
    # Setting up 2 Producers and 4 Consumer Workers
    processor = RealTimeProcessor(queue_size=20)
    
    try:
        await processor.run(
            producers_count=2, 
            consumers_count=4, 
            mock_events=mock_events
        )
    except KeyboardInterrupt:
        print("\nStopping...")

if __name__ == "__main__":
    asyncio.run(main())
