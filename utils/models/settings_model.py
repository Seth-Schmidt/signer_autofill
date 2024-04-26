from typing import List
from typing import Optional
from typing import Union

from pydantic import BaseModel


class RPCNodeConfig(BaseModel):
    url: str


class ConnectionLimits(BaseModel):
    max_connections: int = 100
    max_keepalive_connections: int = 50
    keepalive_expiry: int = 300


class RPCConfigBase(BaseModel):
    full_nodes: List[RPCNodeConfig]
    archive_nodes: Optional[List[RPCNodeConfig]]
    force_archive_blocks: Optional[int]
    retry: int
    request_time_out: int
    connection_limits: ConnectionLimits


class RPCConfigFull(RPCConfigBase):
    skip_epoch_threshold_blocks: int
    polling_interval: int
    semaphore_value: int = 20


class Logs(BaseModel):
    trace_enabled: bool
    write_to_files: bool


class SignerConfig(BaseModel):
    address: str
    private_key: str


class TxSubmissionConfig(BaseModel):
    enabled: bool = True
    # relayer: RelayerService
    signers: List[SignerConfig] = []


class Redis(BaseModel):
    host: str
    port: int
    db: int
    password: Union[str, None] = None
    ssl: bool = False
    cluster_mode: bool = False


class Settings(BaseModel):
    source_private_key: str
    source_address: str
    source_chain_id: int
    rpc: RPCConfigFull
    snapshot_submissions: TxSubmissionConfig
    logs: Logs
    min_signer_value: float
    source_balance_threshold: float
    redis: Redis
