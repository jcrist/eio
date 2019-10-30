import argparse
import logging
import pickle

from aiohttp import web

from eio.node import Server
from eio.raft import StateMachine


class KVStore(StateMachine):
    def __init__(self, logger):
        self.kv = {}
        self.logger = logger

    def apply(self, entry):
        cmd = entry[0]
        args = entry[1:]
        if cmd.upper() == "PUT":
            key, val = args
            self.logger.info("PUT: [key: %s, value: %s]", key, val)
            self.kv[key] = val
        elif cmd.upper() == "DEL":
            key, = args
            self.logger.info("DEL: [key: %s]", key)
            self.kv.pop(key, None)
        elif cmd.upper() == "GET":
            key, = args
            self.logger.info("GET(propose): [key: %s]", key)
            return self.kv.get(key)

    def apply_read(self, entry):
        cmd = entry[0]
        args = entry[1:]
        if cmd.upper() == "GET":
            key, = args
            self.logger.info("GET(read): [key: %s]", key)
            return self.kv.get(key)

    def create_snapshot(self):
        return pickle.dumps(self.kv, protocol=pickle.HIGHEST_PROTOCOL)

    def restore_snapshot(self, snapshot):
        self.kv = pickle.loads(snapshot)


class KVServer(object):
    def __init__(self, server):
        self.server = server

    async def put(self, request):
        key = request.match_info["key"]
        value = await request.content.read()
        await self.server.propose(("PUT", key, value))
        return web.Response(status=201)

    async def delete(self, request):
        key = request.match_info["key"]
        await self.server.propose(("DEL", key))
        return web.Response(status=204)

    async def get(self, request):
        key = request.match_info["key"]
        read = request.rel_url.query.get("read", "global")
        if read == "global":
            value = await self.server.read(("GET", key))
        elif read == "local":
            value = await self.server.read(("GET", key), local=True)
        elif read == "propose":
            value = await self.server.propose(("GET", key))
        else:
            value = None
        if value is None:
            return web.Response(status=404)
        return web.Response(body=value)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("node_id", help="Which node this is", type=int)
    parser.add_argument("port", help="What port to serve on", type=int)
    parser.add_argument("--initial", help="Specify initial raft ports, comma separated")
    parser.add_argument(
        "--debug", help="Whether to log debug output", action="store_true"
    )
    parser.add_argument("--uvloop", help="Whether to use uvloop", action="store_true")

    args = parser.parse_args()

    if args.uvloop:
        import uvloop

        uvloop.install()

    # Setup a logger
    logger = logging.getLogger("demo")
    logger.setLevel(logging.DEBUG if args.debug else logging.INFO)
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        fmt="%(asctime)s.%(msecs)03d %(levelname)-8s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    for name in ["aiohttp.access", "aiohttp.server"]:
        l = logging.getLogger(name)
        l.setLevel(logging.INFO)
        l.addHandler(handler)

    # Construct the application
    peers = ["127.0.0.1:%s" % p for p in args.initial.split(",")]
    server = Server(
        args.node_id, peers=peers, state_machine=KVStore(logger), logger=logger
    )
    kvserver = KVServer(server)
    app = web.Application(logger=logger)
    app.router.add_route("GET", "/{key}", kvserver.get)
    app.router.add_route("PUT", "/{key}", kvserver.put)
    app.router.add_route("DELETE", "/{key}", kvserver.delete)

    async def startup(app):
        await server.serve()

    async def shutdown(app):
        await server.stop()

    app.on_startup.append(startup)
    app.on_shutdown.append(shutdown)

    web.run_app(app, port=args.port)


if __name__ == "__main__":
    main()
