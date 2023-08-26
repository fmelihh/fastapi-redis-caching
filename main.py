import httpx
import logging
import aioredis
import functools
from datetime import datetime
from datetime import timezone
from datetime import timedelta
from aioredis.exceptions import ResponseError
from fastapi import FastAPI, BackgroundTasks, Depends
from typing import List, Dict, Union, Iterable, Tuple

DEFAULT_KEY_PREFIX = "is-bitcoin-lit"
SENTIMENT_API_URL = "https://api.senticrypt.com/v1/bitcoin.json"
TWO_MINUTES = 60 + 60
HOURLY_BUCKET = "3600000"

BitcoinSentiments = List[Dict[str, Union[str, float]]]


def prefixed_key(f):
    """
    A method decorator that prefixes return values.

    Prefixes any string that the decorated method `f` returns with the value of
    the `prefix` attribute on the owner object `self`.
    """

    def prefixed_method(*args, **kwargs):
        self = args[0]
        key = f(*args, **kwargs)
        return f"{self.prefix}:{key}"

    return prefixed_method


class Keys:
    """Methods to generate key names for Redis data structures."""

    def __init__(self, prefix: str = DEFAULT_KEY_PREFIX):
        self.prefix = prefix

    @prefixed_key
    def timeseries_sentiment_key(self) -> str:
        """A time series containing 30-second snapshots of BTC sentiment."""
        return "sentiment:mean:30s"

    @prefixed_key
    def timeseries_price_key(self) -> str:
        """A time series containing 30-second snapshots of BTC price."""
        return "price:mean:30s"

    @prefixed_key
    def cache_key(self) -> str:
        return "cache"


log = logging.getLogger(__name__)
app = FastAPI(title="FastAPI Redis Caching")
redis = aioredis.from_url("redis://redis:6379")


def now():
    """Wrap call to utcnow, so that we can mock this function in tests."""
    return datetime.utcnow()


async def calculate_three_hours_of_data(keys: Keys) -> Dict[str, str]:
    sentiment_key = keys.timeseries_sentiment_key()
    price_key = keys.timeseries_price_key()
    three_hours_ago_ms = int((now() - timedelta(hours=3)).timestamp() * 1000)


async def add_many_to_timeseries(
    key_pairs: Iterable[Tuple[str, str]], data: BitcoinSentiments
):
    """
    Add many samples to a single timeseries key.

    `key_pairs` is an iterable of tuples containing in the 0th position the
    timestamp key into which to insert entries and the 1th position the name
    of the key within th `data` dict to find the sample.
    """
    partial = functools.partial(redis.execute_command, "TS.MADD")
    for datapoint in data:
        for time_series_key, sample_key in key_pairs:
            partial = functools.partial(
                partial,
                time_series_key,
                int(
                    float(datapoint["timestampt"]) * 1000,
                ),
                datapoint[sample_key],
            )
    return await partial()


async def persist(keys: Keys, data: BitcoinSentiments):
    ts_sentiment_key = keys.timeseries_sentiment_key()
    ts_price_key = keys.timeseries_price_key()
    await add_many_to_timeseries(
        ((ts_price_key, "btc_price"), (ts_sentiment_key, "mean")), data
    )


def make_keys():
    return Keys()


async def make_timeseries(key):
    """
    Create a timeseries with the Redis key `key`.

    We'll use the duplicate policy known as "first," which ignores
    duplicate pairs of timestamp and values if we add them.

    Because of this, we don't worry about handling this logic
    ourselves -- but note that there is a performance cost to writes
    using this policy.
    """
    try:
        await redis.execute_command(
            "TS.CREATE",
            key,
            "DUPLICATE_POLICY",
            "first",
        )
    except ResponseError as e:
        # Time series probably already exists
        log.info("Could not create timeseries %s, error: %s", key, e)


async def initialize_redis(keys: Keys):
    await make_timeseries(keys.timeseries_sentiment_key())
    await make_timeseries(keys.timeseries_price_key())


@app.on_event("startup")
async def startup_event():
    keys = Keys()
    await initialize_redis(keys)


@app.post("/refresh")
async def refresh(background_tasks: BackgroundTasks, keys: Keys = Depends(make_keys)):
    async with httpx.AsyncClient() as client:
        data = await client.get(SENTIMENT_API_URL)

    await persist(keys, data.json())
    data = await calculate_three_hours_of_data(keys)
