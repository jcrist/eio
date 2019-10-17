from inspect import isawaitable

from .channel import start_server


class rpc(object):
    """Register a method as an RPC handler"""
    def __init__(self, fn):
        self.fn = fn

    def __set_name__(self, cls, name):
        cls.register_handler(name, self.fn)
        setattr(cls, name, self.fn)


class RPCServer(object):
    """A node in a cluster."""
    def __init__(self, address):
        self.address = address

    @classmethod
    def register_handler(cls, name, func):
        if not hasattr(cls, "_handlers"):
            cls._handlers = set()
        cls._handlers.add(name)

    async def handler(self, channel):
        async for req in channel:
            try:
                method, args, kwargs = req.content
                if method in self._handlers:
                    handler = getattr(self, method)
                else:
                    raise ValueError(f"Unknown RPC method {method}")
                result = handler(*(args or ()), **(kwargs or {}))
                if isawaitable(result):
                    result = await result
                if not req.one_way:
                    await req.reply(result)
            except Exception as exc:
                if not req.one_way:
                    await req.error(exception=exc)

    async def start(self):
        self.server = await start_server(self.address, self.handler)

    async def serve_forever(self):
        await self.server.serve_forever()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        self.server.close()
        await self.server.wait_closed()
