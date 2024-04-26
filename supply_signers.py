import asyncio
import aiorwlock
import resource
import time
import uvloop

from multiprocessing import Process
from httpx import AsyncClient
from httpx import AsyncHTTPTransport
from httpx import Limits
from httpx import Timeout
from redis import asyncio as aioredis
from web3 import Web3
from web3.eth import AsyncEth
from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential

from settings.config import settings
from utils.callback_helpers import send_failure_notifications_async
from utils.models.data_models import SignerResupplyIssue
from utils.models.data_models import SignerReportState
from utils.default_logger import logger
from utils.helper_functions import aiorwlock_aqcuire_release
from utils.redis.redis_conn import RedisPoolCache
from utils.rpc import RpcHelper
from utils.transaction_utils import write_transaction


class SignerManager:
    _aioredis_pool: RedisPoolCache
    _redis_conn: aioredis.Redis
    _rpc_helper: RpcHelper
    _async_transport: AsyncHTTPTransport
    _httpx_client: AsyncClient
    _source_nonce: int
    _min_supply_balance: int

    def __init__(self):
        self._logger = logger.bind(module="SignerManager")
        self._source_pkey = settings.source_private_key
        self._source_address = settings.source_address
        self._chain_id = settings.source_chain_id
        self._rpc_url = settings.rpc.full_nodes[0].url
        self._web3_async = Web3(
            Web3.AsyncHTTPProvider(self._rpc_url),
            modules={'eth': (AsyncEth,)},
            middlewares=[],
        )
        self._rwlock = aiorwlock.RWLock(fast=True)
        self._min_signer_value = self._web3_async.to_wei(settings.min_signer_value, 'ether')
        self._source_balance_threshold = self._web3_async.to_wei(settings.source_balance_threshold, 'ether')
        self._signers = settings.snapshot_submissions.signers
        self._initialized = False


    async def _init_redis_pool(self):
        """
        Initializes the Redis connection pool and populates it with connections.
        """
        self._aioredis_pool = RedisPoolCache()
        await self._aioredis_pool.populate()
        self._redis_conn = self._aioredis_pool._aioredis_pool


    async def _init_rpc_helper(self):
        """
        Initializes the RpcHelper objects for the worker and anchor chain, and sets up the protocol state contract.
        """
        self._rpc_helper = RpcHelper(rpc_settings=settings.rpc)
        await self._rpc_helper.init()
        self._logger.info('RPC helper nodes: {}', self._rpc_helper._nodes)


    async def _init_httpx_client(self):
        """
        Initializes the HTTPX client and transport objects for making HTTP requests.
        """
        self._async_transport = AsyncHTTPTransport(
            limits=Limits(
                max_connections=200,
                max_keepalive_connections=50,
                keepalive_expiry=None,
            ),
        )
        self._client = AsyncClient(
            timeout=Timeout(timeout=5.0),
            follow_redirects=False,
            transport=self._async_transport,
        )


    async def _init_source_nonce(self):
        self._source_nonce = await self._rpc_helper.get_wallet_balance(self._source_address)
        self._min_supply_balance = self._min_signer_value * len(self._signers)
        
    
    @aiorwlock_aqcuire_release
    async def _increment_nonce(self):
        self._source_nonce += 1
        self._logger.info(
            'Using signer {} for submission task. Incremented nonce {}',
            self._signer_address, self._source_nonce,
        )

    @aiorwlock_aqcuire_release
    async def _reset_nonce(self, value: int = 0):
        if value > 0:
            self._source_nonce = value
            self._logger.info(
                'Using signer {} for submission task. Reset nonce to {}',
                self._signer_address, self._source_nonce,
            )
        else:
            correct_nonce = await self._w3.eth.get_transaction_count(
                self._signer_address,
            )
            if correct_nonce and type(correct_nonce) is int:
                self._source_nonce = correct_nonce
                self._logger.info(
                    'Using signer {} for submission task. Reset nonce to {}',
                    self._signer_address, self._source_nonce,
                )
            else:
                self._logger.error(
                    'Using signer {} for submission task. Could not reset nonce',
                    self._signer_address,
                )

    @retry(
        reraise=True,
        retry=retry_if_exception_type(Exception),
        wait=wait_random_exponential(multiplier=1, max=2),
        stop=stop_after_attempt(3),
    )
    async def send_eth(self, to_address: str, value: int):
        """
        Send ETH to signer address
        """
        _nonce = self._source_nonce
        await self._increment_nonce()
        self._logger.trace(f'nonce: {_nonce}')

        try:
            tx_hash = await write_transaction(
                w3=self._web3_async,
                chain_id=self._chain_id,
                from_address=self._source_address,
                private_key=self._source_pkey,
                to_address=to_address,
                value=value,
                nonce=_nonce,
            )

            self._logger.info(
                f'submitted transaction with tx_hash: {tx_hash}',
            )
            await self._web3_async.eth.wait_for_transaction_receipt(tx_hash, timeout=20)

        except Exception as e:
            if 'nonce too low' in str(e):
                error = eval(str(e))
                message = error['message']
                next_nonce = int(message.split('next nonce ')[1].split(',')[0])
                self._logger.info(
                    'Nonce too low error. Next nonce: {}', next_nonce,
                )
                await self._reset_nonce(next_nonce)
                # reset queue
                raise Exception('nonce error, reset nonce')
            else:
                self._logger.info(
                    'Error sending ETH. Retrying...',
                )
                # sleep for two seconds before updating nonce
                time.sleep(2)
                await self._reset_nonce()

                raise e
        else:
            return tx_hash

    async def check_and_send(self, signer_address: str):
        """
        Check if the nonce is correct and send the transaction
        """
        try:
            signer_balance = await self._web3_async.eth.get_balance(signer_address)
            if signer_balance < self._min_signer_value:
                balance_diff = self._min_signer_value - signer_balance
                self._logger.info(
                    'Signer {} has insufficient balance. Sending {} ETH...', signer_address, balance_diff
                )
                tx_hash = await self.send_eth(signer_address, balance_diff)
                self._logger.info(
                    'Sent {} ETH to signer: {}', balance_diff, signer_address,
                )
                return tx_hash
            else:
                self._logger.info(
                    'Signer {} has sufficient balance. No action needed', signer_address,
                )
        except Exception as e:
            self._logger.error(
                'Error managing signer {}, Error: {}', signer_address, e,
            )
            raise e


    async def check_source_balance(self):
        """
        Check if the source address has sufficient balance
        """
        source_balance = await self._rpc_helper.get_wallet_balance(self._source_address)
        if source_balance < self._source_balance_threshold:
            balance_diff = self._source_balance_threshold - source_balance
            balance_diff = balance_diff / 10**18
            self._logger.debug(
                'Source address is low on funds. Sending notification...',
            )
            notification_message = SignerResupplyIssue(
                instanceID=self._source_address,
                issueType=SignerReportState.SOURCE_BALANCE.value,
                projectID='source_balance_low',
                epochId="signer_refill",
                timeOfReporting=str(int(time.time())),
                extra=f'Balance needed: {balance_diff}',
            )
            await send_failure_notifications_async(self._client, notification_message)
        if source_balance < self._min_supply_balance:
            self._logger.debug(
                'Source address does not have enough funds to send to all signers.', 
                'Required: {} ETH', self._min_supply_balance,
                'Available: {} ETH', source_balance,
            )
            return False
        else:
            self._logger.debug(
                'Source address has sufficient balance to supply signers.',
            )
            return True


    async def init(self):
        if not self._initialized:
            await self._init_redis_pool()
            await self._init_rpc_helper()
            await self._init_httpx_client()
            await self._init_source_nonce()
        self._initialized = True


    async def run(self):
        await self.init()
        
        self._logger.debug('Starting {}', Process.name)
        
        while True:
            source_balance_sufficient = await self.check_source_balance()
            if not source_balance_sufficient:
                await asyncio.sleep(5)
                break

            await asyncio.sleep(5)
            break


def main():
    # set resource limits and start event loop for signer manager
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(
        resource.RLIMIT_NOFILE,
        (settings.rlimit.file_descriptors, hard),
    )
    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    manager_process = SignerManager()
    loop.run_until_complete(manager_process.run())


if __name__ == '__main__':
    main()