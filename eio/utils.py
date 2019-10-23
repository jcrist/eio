import asyncio
import time


async def cancel_task(task):
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def id_generator(prefix, prefix_bits=10, counter_bits=10):
    """Returns an ordered, globally unique stream of 64 bit IDs.

    The IDs are a mix of a prefix, timestamp, and counter.

    The IDs are guaranteed to be globally unique across generator restarts
    provided the following conditions:

    - Generators with the same prefix don't restart more than once every
      millisecond
    - No more than ``2 ** counter_bits`` IDs are requested every millisecond
    - IDs will overflow in ``2 ** (64 - prefix_bits - counter_bits) / 1000 -
      time.time()`` seconds from now.

    Given the default configuration, this maths out to:
    - No more than 1024 unique generators at a time
    - No more than 1024 IDs per generator every millisecond
    - IDs will overflow in the year 2527

    Parameters
    ----------
    prefix : int
        A globally unique integer prefix to identify this generator with.
        Only one generator with this ID may exist at any time.
    prefix_bits : int, optional
        The number of bits to alot for storing the prefix.
    counter_bits : int, optional
        The number of bits to alot for storing the counter.
    """
    if prefix_bits + counter_bits >= 64:
        raise ValueError("prefix_bits + counter_bits must be < 64")
    if prefix.bit_length() > prefix_bits:
        raise ValueError("prefix requires more than %d bits" % prefix_bits)
    suffix_bits = 64 - prefix_bits
    time_bits = 64 - (prefix_bits + counter_bits)
    prefix_mask = (2 ** prefix_bits) - 1
    prefix = (prefix & prefix_mask) << suffix_bits
    time_ms = int(time.time() * 1000)
    time_mask = (2 ** time_bits) - 1
    timestamp = (time_ms & time_mask) << counter_bits
    val = prefix + timestamp
    while True:
        yield val
        val += 1
