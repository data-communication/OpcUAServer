import asyncio
from collections.abc import Awaitable, Callable

import aiomqtt

class MqttService:
    def __init__(self, host="localhost", port=1883, username=None, password=None):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

        self._handlers: dict[str, list[Callable[[str], Awaitable[None]]]] = {}
        self._topics: asyncio.Queue[str] = asyncio.Queue()

    def register(self, topic: str, handler: Callable[[str], Awaitable[None]]):
        if topic not in self._handlers:
            self._handlers[topic] = []
            self._topics.put_nowait(topic)

        self._handlers[topic].append(handler)

    async def _subscribe(self, client):
        while True:
            topic = await self._topics.get()
            await client.subscribe(topic)

    async def _receive(self, client):
        async for message in client.messages:
            topic = str(message.topic)
            payload = bytes(message.payload).decode("utf-8")

            for handler in self._handlers.get(topic, []):
                await handler(payload)

    async def run(self):
        async with aiomqtt.Client(hostname=self.host, port=self.port, username=self.username, password=self.password, identifier="opcua-gateway") as client:
            async with asyncio.TaskGroup() as tasks:
                tasks.create_task(self._subscribe(client))
                tasks.create_task(self._receive(client))