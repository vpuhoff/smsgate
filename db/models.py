# db/models.py
from datetime import datetime as dt
from decimal import Decimal
from sqlalchemy import Integer, String, Numeric, DateTime, Index
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import insert

class Base(DeclarativeBase):
    pass

class SmsData(Base):
    __tablename__ = "sms_data"

    # Обязательные поля (nullable=False)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    msg_id: Mapped[str] = mapped_column(String, unique=True, nullable=True)
    original_body: Mapped[str] = mapped_column(String, nullable=True)
    sender: Mapped[str] = mapped_column(String, nullable=False)
    datetime: Mapped[dt] = mapped_column(DateTime(timezone=True), nullable=False)
    card: Mapped[str] = mapped_column(String(4), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(14, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    txn_type: Mapped[str] = mapped_column(String, nullable=False)

    # Необязательные поля (nullable=True по умолчанию)
    # Используем `| None` (или Optional[...]) чтобы указать это
    balance: Mapped[Decimal | None] = mapped_column(Numeric(14, 2))
    merchant: Mapped[str | None] = mapped_column(String)
    address: Mapped[str | None] = mapped_column(String)
    city: Mapped[str | None] = mapped_column(String)

    device_id: Mapped[str | None] = mapped_column(String)
    parser_version: Mapped[str | None] = mapped_column(String)

    __table_args__ = (
        Index("idx_sms_sender", "sender"),
        Index("idx_sms_datetime", "datetime"),
        Index("idx_sms_txn_type", "txn_type"),
    )

