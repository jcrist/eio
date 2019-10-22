import asyncio
import itertools

import msgpack

from .raft import RaftNode
from .utils import cancel_task


class ServerProtocol(asyncio.Protocol):
    def __init__(self, server):
        super().__init__()
        self.server = server
        self._unpacker = msgpack.Unpacker(raw=False)

    def data_received(self, data):
        self._unpacker.feed(data)
        for msg in self._unpacker:
            self.server.on_message(msg)


class CommProtocol(asyncio.Protocol):
    def __init__(self, comm=None, *, loop=None):
        super().__init__()
        self.transport = None
        self.comm = comm
        self._loop = loop
        self._paused = False
        self._yield_cycler = itertools.cycle(range(50))
        self._drain_waiter = None

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc=None):
        if self._paused:
            waiter = self._drain_waiter
            if waiter is not None:
                self._drain_waiter = None
                if not waiter.done():
                    waiter.set_result(None)

        self.comm._maybe_reconnect()

    def pause_writing(self):
        self._paused = True

    def resume_writing(self):
        self._paused = False

        waiter = self._drain_waiter
        if waiter is not None:
            self._drain_waiter = None
            if not waiter.done():
                waiter.set_result(None)

    async def drain(self):
        if self.transport.is_closing():
            await asyncio.sleep(0, loop=self._loop)
        elif self._paused and self.comm._connected:
            self._drain_waiter = self._loop.create_future()
            await self._drain_waiter
        elif not next(self._yield_cycler):
            await asyncio.sleep(0, loop=self._loop)


class Comm(object):
    def __init__(self, address):
        self.address = address
        self._packer = msgpack.Packer(use_bin_type=True)
        self._transport = None
        self._protocol = None
        self._connect_task = None
        self._connected = False
        self.closed = False

    def connect(self):
        if self._connect_task and not self._connect_task.done():
            return
        self._connect_task = asyncio.ensure_future(
            self._connect()
        )

    def _maybe_reconnect(self):
        if not self.closed:
            self._connected = False
            self.connect()

    async def _connect(self):
        loop = asyncio.get_event_loop()

        def factory():
            return CommProtocol(loop=loop, comm=self)

        host, port = self.address.split(":")

        retry_interval = 0.5
        while True:
            try:
                transport, protocol = await loop.create_connection(
                    factory, host, port
                )
                break
            except (ConnectionRefusedError, OSError):
                await asyncio.sleep(retry_interval)
                retry_interval = min(15, 1.5 * retry_interval)

        self._transport = transport
        self._protocol = protocol
        self._connected = True

    async def send(self, msg):
        """Send a message"""
        if self._connected:
            self._transport.write(self._packer.pack(msg))
            await self._protocol.drain()

    def send_sync(self, msg):
        if self._connected:
            self._transport.write(self._packer.pack(msg))

    async def close(self):
        """Close the comm and release all resources."""
        if not self.closed:
            self.closed = True
            self._connected = False

            # Cancel the background connect task
            if self._connect_task is not None and not self._connect_task.done():
                await cancel_task(self._connect_task)

            # Close and cleanup the protocol and transport
            if self._transport is not None:
                self._transport.close()
                self._transport = None
            self._protocol = None


class Server(object):
    def __init__(self, address, peers=None, tick_period=0.1):
        self.address = address
        self.peers = peers
        self.raft = RaftNode(self.address, self.peers)
        self.tick_period = tick_period
        self.comms = {p: Comm(p) for p in self.peers}
        self._ticker = None
        self.server = None

    async def serve(self):
        for c in self.comms.values():
            c.connect()
        self._ticker = asyncio.ensure_future(self.tick_loop())
        loop = asyncio.get_running_loop()
        host, port = self.address.split(":")
        server = await loop.create_server(lambda: ServerProtocol(self), host, port)
        self.server = server

    async def stop(self):
        for c in self.comms.values():
            await c.close()
        if self._ticker is not None:
            await cancel_task(self._ticker)

    def on_message(self, msg):
        for node, m in self.raft.on_message(msg):
            self.comms[node].send_sync(m)

    async def send(self, node_id, msg):
        await self.comms[node_id].send(msg)

    async def tick_loop(self):
        while True:
            await asyncio.sleep(self.tick_period)
            msgs = self.raft.on_tick()
            for node, msg in msgs:
                await self.send(node, msg)

    async def serve_forever(self):
        try:
            await self.serve()
            await self.server.serve_forever()
        finally:
            await self.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("address", help="Address to serve at")
    parser.add_argument("--peer", help="Add a peer address", action="append")

    args = parser.parse_args()

    async def main(address, peers):
        server = Server(address, peers=peers)
        await server.serve_forever()

    asyncio.run(main(args.address, args.peer))
