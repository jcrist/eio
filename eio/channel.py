import asyncio
import collections
import itertools
import time
import traceback

import msgpack


__all__ = ('new_channel', 'start_server', 'Request', 'Channel', 'RemoteException')


# Message types
REQUEST_REPLY = 0
REQUEST_NO_REPLY = 1
REPLY = 2
ERROR = 3


def _encode(data, _p=msgpack.Packer(use_bin_type=True)):
    return _p.pack(data)


class RemoteException(Exception):
    """A remote exception that occurs on a different machine"""
    def __init__(self, message, address):
        self.message = message
        self.address = address

    def __str__(self):
        return f"Remote exception at address {self.address}:\n\n{self.message}"


async def new_channel(addr, *, loop=None, timeout=0, **kwargs):
    """Create a new channel.

    Parameters
    ----------
    addr : tuple or str
        The address to connect to.
    loop : AbstractEventLoop, optional
        The event loop to use.
    timeout : float, optional
        Timeout for initial connection to the server.
    **kwargs
        All remaining arguments are forwarded to ``loop.create_connection``.

    Returns
    -------
    channel : Channel
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    if timeout is None:
        timeout = float('inf')

    def factory():
        return ChannelProtocol(loop=loop)

    if isinstance(addr, tuple):
        connect = loop.create_connection
        args = (factory,) + addr
        connect_errors = (ConnectionRefusedError, OSError)
    elif isinstance(addr, str):
        connect = loop.create_unix_connection
        args = (factory, addr)
        connect_errors = FileNotFoundError
    else:
        raise ValueError('Unknown address type: %s' % addr)

    retry_interval = 0.5
    start_time = time.monotonic()
    while True:
        try:
            _, p = await connect(*args, **kwargs)
            break
        except connect_errors:
            if (time.monotonic() - start_time) > timeout:
                raise
            await asyncio.sleep(retry_interval)
            retry_interval = min(30, 1.5 * retry_interval)

    return p.channel


async def start_server(addr, handler, *, loop=None, **kwargs):
    """Start a new server.

    Parameters
    ----------
    addr : tuple or str
        The address to listen at.
    handler : callable
        An async callable. When a new client connects, the handler will be
        called with its respective channel to handle all requests.
    loop : AbstractEventLoop, optional
        The event loop to use.
    **kwargs
        All remaining parameters will be forwarded to ``loop.create_server``.

    Returns
    -------
    server : Server
    """
    if loop is None:
        loop = asyncio.get_event_loop()

    def factory():
        return ChannelProtocol(handler, loop=loop)

    if isinstance(addr, tuple):
        return await loop.create_server(factory, *addr, **kwargs)
    elif isinstance(addr, str):
        return await loop.create_unix_server(factory, addr, **kwargs)
    else:
        raise ValueError('Unknown address type: %s' % addr)


class ChannelProtocol(asyncio.Protocol):
    def __init__(self, handler=None, *, loop=None):
        super().__init__()
        self.transport = None
        self.channel = None
        self._handler = handler
        self._loop = loop
        self._unpacker = msgpack.Unpacker(raw=False)
        self._paused = False
        self._drain_waiter = None
        self._connection_lost = None

    def connection_made(self, transport):
        self.transport = transport
        self.channel = Channel(self, transport, loop=self._loop)

        if self._handler is not None:
            res = self._handler(self.channel)
            if asyncio.iscoroutine(res):
                self._loop.create_task(res)

    def connection_lost(self, exc=None):
        if exc is None:
            exc = ConnectionResetError('Connection closed')
        self.channel._set_exception(exc)
        self._connection_lost = exc

        if self._paused:
            waiter = self._drain_waiter
            if waiter is not None:
                self._drain_waiter = None
                if not waiter.done():
                    waiter.set_exception(exc)

    def data_received(self, data):
        self._unpacker.feed(data)
        for msg in self._unpacker:
            msg_type, msg_id, content = msg
            try:
                self.channel._append_msg(msg_type, msg_id, content)
            except RuntimeError as exc:
                self.channel._set_exception(exc)

    def eof_received(self):
        self.channel._set_exception(ConnectionResetError())

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
        if self._paused and not self._connection_lost:
            self._drain_waiter = self._loop.create_future()
            await self._drain_waiter


class Channel(object):
    """A communication channel between two endpoints.

    Use ``new_channel`` to create a channel.
    """
    def __init__(self, protocol, transport, loop):
        self._protocol = protocol
        self._transport = transport
        self._loop = loop

        self._id_iter = itertools.count()
        self._active_reqs = {}
        self._queue = collections.deque()
        self._waiter = None
        self._exception = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, typ, value, traceback):
        await self.close()
        if isinstance(value, ConnectionResetError):
            return True

    async def _write_bytes(self, content):
        self._transport.write(content)
        await self._protocol.drain()

    async def send(self, msg, one_way=False):
        """Send a request and optionally wait for a response.

        Parameters
        ----------
        msg : object
            The message contents. Can be any msgpack compatible object.
        one_way : bool, optional
            If True, this is a one-way request and no response will be waited
            for. Default is False.
        """
        if self._exception is not None:
            raise self._exception

        if one_way:
            data = _encode((REQUEST_NO_REPLY, None, msg))
        else:
            msg_id = next(self._id_iter)
            data = _encode((REQUEST_REPLY, msg_id, msg))
            reply = self._active_reqs[msg_id] = self._loop.create_future()

        await self._write_bytes(data)

        if not one_way:
            return await reply

    async def __aiter__(self):
        try:
            while True:
                yield await self.recv()
        except ConnectionResetError:
            await self.close()

    async def recv(self):
        """Wait for the next request"""
        if self._exception is not None:
            raise self._exception

        if not self._queue:
            if self._waiter is not None:
                raise RuntimeError(
                    'Channel.recv may only be called by one coroutine at a time'
                )
            self._waiter = self._loop.create_future()
            try:
                await self._waiter
            finally:
                self._waiter = None

        return self._queue.popleft()

    def _close(self):
        if self._transport is not None:
            transport = self._transport
            self._transport = None
            return transport.close()

    async def close(self):
        """Close the channel and release all resources.

        It is invalid to use this channel after closing.

        This method is idempotent.
        """
        self._close()
        try:
            # TODO: drain here?
            futs = self._active_reqs.values()
            await asyncio.gather(*futs, return_exceptions=True)
        except asyncio.CancelledError:
            pass

    def _append_msg(self, msg_type, msg_id, content):
        if msg_type == REQUEST_REPLY or msg_type == REQUEST_NO_REPLY:
            message = Request(self, content, msg_id)
            self._queue.append(message)

            waiter = self._waiter
            if waiter is not None:
                self._waiter = None
                waiter.set_result(False)

        elif msg_type == REPLY or msg_type == ERROR:
            message = self._active_reqs.pop(msg_id)
            if message.done():
                errmsg = 'Request reply already set.'
                if message.cancelled():
                    errmsg = 'Request was cancelled.'
                raise RuntimeError(errmsg)

            if msg_type == REPLY:
                message.set_result(content)
            else:
                addr = self._transport.get_extra_info('peername')
                message.set_exception(RemoteException(content, addr))

        else:
            raise RuntimeError('Invalid message type %d' % msg_type)

    def _set_exception(self, exc):
        self._exception = exc

        waiter = self._waiter
        if waiter is not None:
            self._waiter = None
            if not waiter.cancelled():
                waiter.set_exception(exc)

        for msg in self._active_reqs.values():
            if not msg.done():
                msg.set_exception(exc)

        self._close()


class Request(object):
    """A client request."""
    __slots__ = ("_channel", "_content", "_msg_id")

    def __init__(self, channel, content, msg_id):
        self._channel = channel
        self._content = content
        self._msg_id = msg_id

    @property
    def content(self):
        """The content of the incoming message."""
        return self._content

    @property
    def one_way(self):
        """True if no reply is expected"""
        return self._msg_id is None

    async def reply(self, result):
        """Reply to the request with the provided result."""
        if self.one_way:
            raise ValueError("Request doesn't expect a reply")

        if self._channel._exception:
            raise self._channel._exception

        content = (REPLY, self._msg_id, result)
        try:
            content = _encode(content)
        except Exception as e:
            await self.error(e)
        else:
            await self._channel._write_bytes(content)

    async def error(self, message=None, exception=None):
        """Reply to the request with an exception.

        Parameters
        ----------
        message : str, optional
            An error message to return.
        exception : Exception, optional
            An exception ot use as the error message (including traceback).
        """
        if self.one_way:
            raise ValueError("Request doesn't expect a reply")

        if self._channel._exception:
            raise self._channel._exception

        if exception and message:
            raise ValueError("Cannot provide both message and exception")
        elif exception is not None:
            message = ''.join(
                traceback.format_exception(
                    exception.__class__,
                    exception,
                    exception.__traceback__
                )
            )
        elif message is None:
            raise ValueError("Must provide either message or exception")

        content = (ERROR, self._msg_id, message)
        content = _encode(content)
        await self._channel._write_bytes(content)
