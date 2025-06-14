# libs/llm_core.py
from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field
from libs.models import TxnType   # ← единственный enum!

class ParsedSmsCore(BaseModel):
    """Мини-схема, которую возвращает Gemini."""
    txn_type: TxnType
    date: datetime
    amount: Optional[Decimal] = Field(None, ge=0)
    currency: Optional[str]
    card: Optional[str]
    merchant: Optional[str]
    city: Optional[str]
    address: Optional[str]
    balance: Optional[Decimal]
