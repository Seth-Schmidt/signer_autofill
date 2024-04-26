import redis
from redis import asyncio as aioredis
from redis.asyncio.connection import ConnectionPool

from settings.config import settings as settings_conf
from utils.default_logger import logger


# setup logging
logger = logger.bind(module='Powerloom|RedisConn')

REDIS_CONN_CONF = {
    'host': settings_conf.redis.host,
    'port': settings_conf.redis.port,
    'password': settings_conf.redis.password,
    'db': settings_conf.redis.db,
    'retry_on_error': [redis.exceptions.ReadOnlyError],
}


def construct_redis_url():
    """
    Constructs a Redis URL based on the REDIS_CONN_CONF dictionary.

    Returns:
        str: Redis URL constructed from REDIS_CONN_CONF dictionary.
    """
    if REDIS_CONN_CONF['password']:
        return (
            f'redis://{REDIS_CONN_CONF["password"]}@{REDIS_CONN_CONF["host"]}:{REDIS_CONN_CONF["port"]}'
            f'/{REDIS_CONN_CONF["db"]}'
        )
    else:
        return f'redis://{REDIS_CONN_CONF["host"]}:{REDIS_CONN_CONF["port"]}/{REDIS_CONN_CONF["db"]}'

# ref https://github.com/redis/redis-py/issues/936


async def get_aioredis_pool(pool_size=200):
    """
    Returns an aioredis Redis connection pool.

    Args:
        pool_size (int): Maximum number of connections to the Redis server.

    Returns:
        aioredis.Redis: Redis connection pool.
    """
    pool = ConnectionPool.from_url(
        url=construct_redis_url(),
        retry_on_error=[redis.exceptions.ReadOnlyError],
        max_connections=pool_size,
    )

    return aioredis.Redis(connection_pool=pool)


class RedisPoolCache:
    def __init__(self, pool_size=2000):
        """
        Initializes a Redis connection object with the specified connection pool size.

        Args:
            pool_size (int): The maximum number of connections to keep in the pool.
        """
        self._aioredis_pool = None
        self._pool_size = pool_size

    async def populate(self):
        """
        Populates the Redis connection pool with the specified number of connections.
        """
        if not self._aioredis_pool:
            self._aioredis_pool: aioredis.Redis = await get_aioredis_pool(
                self._pool_size,
            )