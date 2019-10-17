import asyncio
import argparse
import os
from concurrent import futures
from eio import start_server, new_channel


async def main(nclients, nbytes, duration, one_way):
    server = await start_server(('127.0.0.1', 5556), handler)
    loop = asyncio.get_running_loop()
    with futures.ProcessPoolExecutor(max_workers=nclients) as executor:
        async with server:
            clients = [
                    loop.run_in_executor(
                        executor,
                        bench_client,
                        nbytes,
                        duration,
                        one_way
                    )
                for _ in range(nclients)
            ]
            count = sum(await asyncio.gather(*clients))

    print(f"{count / duration} RPS")
    print(f"{duration / count * 1e6} us per request")
    print(f"{nbytes * count / (duration * 1e6)} MB/s")


async def handler(channel):
    async for req in channel:
        if req.is_info:
            pass
        else:
            await req.reply(1)


def bench_client(nbytes, duration, one_way):
    return asyncio.run(client(nbytes, duration, one_way))


async def client(nbytes, duration, one_way):
    count = 0
    running = True

    def stop():
        nonlocal running
        running = False

    loop = asyncio.get_running_loop()
    loop.call_later(duration, stop)

    payload = os.urandom(nbytes)

    async with await new_channel(("127.0.0.1", 5556)) as channel:
        if one_way:
            while running:
                await channel.info(payload)
                count += 1
        else:
            while running:
                await channel.request(payload)
                count += 1

    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Benchmark channels')
    parser.add_argument("--nclients", default=3, type=int, help="Number of clients")
    parser.add_argument("--nbytes", default=10, type=int, help="payload size in bytes")
    parser.add_argument("--duration", default=10, type=int, help="bench duration in secs")
    parser.add_argument("--one-way", action="store_true", help="Whether to bench one-way RPCs")
    parser.add_argument("--uvloop", action="store_true", help="Whether to use uvloop")
    args = parser.parse_args()

    if args.uvloop:
        import uvloop
        uvloop.install()

    print(
        f"Benchmarking: nclients={args.nclients}, nbytes={args.nbytes}, "
        f"duration={args.duration}, one_way={args.one_way}, uvloop={args.uvloop}"
    )

    asyncio.run(
        main(
            nclients=args.nclients,
            nbytes=args.nbytes,
            duration=args.duration,
            one_way=args.one_way
        )
    )
