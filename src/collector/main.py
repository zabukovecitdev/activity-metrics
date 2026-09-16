import asyncio

from collector.collector import Collector

endpoints = [
    "http://0.0.0.0:8080/metrics/"
]

async def main():
    async with Collector(endpoints) as collector:
        await collector.run()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
