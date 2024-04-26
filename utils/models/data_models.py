from typing import Optional
from enum import Enum

from pydantic import BaseModel


class SignerReportState(Enum):
    SOURCE_BALANCE = 'INSUFFICIENT_SOURCE_BALANCE'
    SIGNER_NONCE = 'SIGNER_NONCE_STALE'


class SignerResupplyIssue(BaseModel):
    instanceID: str
    issueType: str
    projectID: str
    epochId: str
    timeOfReporting: str
    extra: Optional[str] = ''