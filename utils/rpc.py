import asyncio
import tenacity

from tenacity import retry
from tenacity import retry_if_exception_type
from tenacity import stop_after_attempt
from tenacity import wait_random_exponential
from web3 import Web3
from web3.eth import AsyncEth

from settings.config import settings
from utils.exceptions import RPCException
from utils.models.settings_model import RPCConfigBase
from functools import wraps

from utils.default_logger import logger


def acquire_rpc_semaphore(fn):
    """
    A decorator function that acquires a bounded semaphore before executing the decorated function and releases it
    after the function is executed. This decorator is intended to be used with async functions.

    Args:
        fn: The async function to be decorated.

    Returns:
        The decorated async function.
    """
    @wraps(fn)
    async def wrapped(self, *args, **kwargs):
        sem: asyncio.BoundedSemaphore = self._semaphore
        await sem.acquire()
        result = None
        try:
            result = await fn(self, *args, **kwargs)
        except Exception as e:
            logger.opt(exception=True).error('Error in asyncio semaphore acquisition decorator: {}', e)
        finally:
            sem.release()
            return result
    return wrapped


class RpcHelper(object):

    def __init__(self, rpc_settings: RPCConfigBase = settings.rpc, archive_mode=False):
        """
        Initializes an instance of the RpcHelper class.

        Args:
            rpc_settings (RPCConfigBase, optional): The RPC configuration settings to use. Defaults to settings.rpc.
            archive_mode (bool, optional): Whether to operate in archive mode. Defaults to False.
        """
        self._archive_mode = archive_mode
        self._rpc_settings = rpc_settings
        self._nodes = list()
        self._current_node_index = 0
        self._node_count = 0
        self._initialized = False
        self._sync_nodes_initialized = False
        self._logger = logger.bind(module='Powerloom|RpcHelper')
        self._semaphore = None

    async def _load_async_web3_providers(self):
        """
        Loads async web3 providers for each node in the list of nodes.
        If a node already has a web3 client, it is skipped.
        """
        for node in self._nodes:
            node['web3_client_async'] = Web3(
                Web3.AsyncHTTPProvider(node['rpc_url']),
                modules={'eth': (AsyncEth,)},
                middlewares=[],
            )
            self._logger.info('Loaded async web3 provider for node {}: {}', node['rpc_url'], node['web3_client_async'])
        self._logger.info('Post async web3 provider loading: {}', self._nodes)

    async def init(self):
        """
        Initializes the RPC client by loading web3 providers and rate limits,
        loading rate limit SHAs, initializing HTTP clients, and loading async
        web3 providers.

        Args:
            redis_conn: Redis connection object.

        Returns:
            None
        """
        if not self._initialized:
            self._semaphore = asyncio.BoundedSemaphore(value=settings.rpc.semaphore_value)
            if not self._sync_nodes_initialized:
                self._logger.debug('Sync nodes not initialized, initializing...')
                self.sync_init()
            if self._nodes:
                # load async web3 providers
                for node in self._nodes:
                    node['web3_client_async'] = Web3(
                        Web3.AsyncHTTPProvider(node['rpc_url']),
                        modules={'eth': (AsyncEth,)},
                        middlewares=[],
                    )
                    self._logger.info('Loaded async web3 provider for node {}: {}', node['rpc_url'], node['web3_client_async'])
                self._logger.info('Post async web3 provider loading: {}', self._nodes)
                self._initialized = True
                self._logger.info('RPC client initialized')
            else:
                self._logger.error('No full nor archive nodes found in config')

    def sync_init(self):
        if self._sync_nodes_initialized:
            return
        if self._archive_mode:
            nodes = self._rpc_settings.archive_nodes
        else:
            nodes = self._rpc_settings.full_nodes
        if not nodes:
            self._logger.error('No full nor archive nodes found in config')
            raise Exception('No full nor archive nodes found in config')
        for node in nodes:
            try:
                self._nodes.append(
                    {
                        'web3_client': Web3(Web3.HTTPProvider(node.url)),
                        'web3_client_async': None,
                        'rpc_url': node.url,
                    },
                )
            except Exception as exc:
                self._logger.opt(exception=settings.logs.trace_enabled).error(
                    (
                        'Error while initialising one of the web3 providers,'
                        f' err_msg: {exc}'
                    ),
                )
            else:
                self._logger.info('Loaded blank node settings for node {}', node.url)          
        self._node_count = len(self._nodes)
        self._sync_nodes_initialized = True

    def get_current_node(self):
        """
        Returns the current node to use for RPC calls.

        If the sync nodes have not been initialized, it initializes them by loading web3 providers and rate limits.
        If there are no full nodes available, it raises an exception.

        Returns:
            The current node to use for RPC calls.
        """
        # NOTE: the following should not do an implicit initialization of the nodes. too much of hidden logic
        # if not self._sync_nodes_initialized:
        #     self._load_web3_providers_and_rate_limits()
        #     self._sync_nodes_initialized = True

        if self._node_count == 0:
            raise Exception('No full nodes available')
        return self._nodes[self._current_node_index]

    def _on_node_exception(self, retry_state: tenacity.RetryCallState):
        """
        Callback function to handle exceptions raised during RPC calls to nodes.
        It updates the node index to retry the RPC call on the next node.

        Args:
            retry_state (tenacity.RetryCallState): The retry state object containing information about the retry.

        Returns:
            None
        """
        exc_idx = retry_state.kwargs['node_idx']
        next_node_idx = (retry_state.kwargs.get('node_idx', 0) + 1) % self._node_count
        retry_state.kwargs['node_idx'] = next_node_idx
        self._logger.warning(
            'Found exception while performing RPC {} on node {} at idx {}. '
            'Injecting next node {} at idx {} | exception: {} ',
            retry_state.fn, self._nodes[exc_idx], exc_idx, self._nodes[next_node_idx],
            next_node_idx, retry_state.outcome.exception(),
        )

    @acquire_rpc_semaphore
    async def get_wallet_balance(self, wallet_address: str):
        """
        Returns the current block number of the Ethereum blockchain.

        Args:
            redis_conn: Redis connection object.

        Returns:
            The current block number of the Ethereum blockchain.

        Raises:
            RPCException: If an error occurs while making the RPC call.
        """
        @retry(
            reraise=True,
            retry=retry_if_exception_type(RPCException),
            wait=wait_random_exponential(multiplier=1, max=10),
            stop=stop_after_attempt(settings.rpc.retry),
            before_sleep=self._on_node_exception,
        )
        async def f(node_idx):
            node = self._nodes[node_idx]
            web3_provider = node['web3_client_async']

            try:
                wallet_balance = await web3_provider.eth.get_balance(
                    wallet_address,
                )
            except Exception as e:
                exc = RPCException(
                    request='get_balance',
                    response=None,
                    underlying_exception=e,
                    extra_info=f'RPC_GET_BALANCE ERROR: {str(e)}',
                )
                self._logger.trace('Error in get_wallet_balance, wallet {}, error {}', wallet_address, str(exc))
                raise exc
            else:
                return wallet_balance
        return await f(node_idx=0)

    @acquire_rpc_semaphore
    async def get_wallet_nonce(self, wallet_address: str):
        """
        Returns the current block number of the Ethereum blockchain.

        Args:
            redis_conn: Redis connection object.

        Returns:
            The current block number of the Ethereum blockchain.

        Raises:
            RPCException: If an error occurs while making the RPC call.
        """
        @retry(
            reraise=True,
            retry=retry_if_exception_type(RPCException),
            wait=wait_random_exponential(multiplier=1, max=10),
            stop=stop_after_attempt(settings.rpc.retry),
            before_sleep=self._on_node_exception,
        )
        async def f(node_idx):
            node = self._nodes[node_idx]
            web3_provider = node['web3_client_async']

            try:
                wallet_nonce = await web3_provider.eth.get_transaction_count(
                wallet_address,
            )
            except Exception as e:
                exc = RPCException(
                    request='get_transaction_count',
                    response=None,
                    underlying_exception=e,
                    extra_info=f'RPC_GET_TRANSACTION_COUNT ERROR: {str(e)}',
                )
                self._logger.trace('Error in get_wallet_nonce, wallet {}, error {}', wallet_address, str(exc))
                raise exc
            else:
                return wallet_nonce
        return await f(node_idx=0)

    @acquire_rpc_semaphore
    async def get_transaction_receipt(self, tx_hash: str):
        """
        Retrieves the transaction receipt for a given transaction hash.

        Args:
            tx_hash (str): The transaction hash for which to retrieve the receipt.
            redis_conn: Redis connection object.

        Returns:
            The transaction receipt details as a dictionary.

        Raises:
            RPCException: If an error occurs while retrieving the transaction receipt.
        """

        @retry(
            reraise=True,
            retry=retry_if_exception_type(RPCException),
            wait=wait_random_exponential(multiplier=1, max=10),
            stop=stop_after_attempt(settings.rpc.retry),
            before_sleep=self._on_node_exception,
        )
        async def f(node_idx):
            node = self._nodes[node_idx]

            try:
                tx_receipt_details = await node['web3_client_async'].eth.get_transaction_receipt(
                    tx_hash,
                )
            except Exception as e:
                exc = RPCException(
                    request={
                        'txHash': tx_hash,
                    },
                    response=None,
                    underlying_exception=e,
                    extra_info=f'RPC_GET_TRANSACTION_RECEIPT_ERROR: {str(e)}',
                )
                self._logger.trace('Error in get transaction receipt for tx hash {}, error {}', tx_hash, str(exc))
                raise exc
            else:
                return tx_receipt_details
        return await f(node_idx=0)
    