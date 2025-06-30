import asyncio
from functools import lru_cache
import logging
from typing import Any, List, Literal, Mapping, Optional
from datetime import datetime as dt
from decimal import Decimal
import sys
import os

# Removed PocketBase SDK import
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

# SQLAlchemy imports
from sqlalchemy.future import select
from sqlalchemy import update, delete
from sqlalchemy.dialects.postgresql import insert

root_directory = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if root_directory not in sys.path:
    sys.path.insert(0, root_directory)

from libs.config import get_settings
from libs.models import ParsedSMS # Assuming ParsedSMS is a Pydantic model defined here
from libs.sentry import sentry_capture

# Database imports
from db.session import SessionLocal
from db.models import SmsData

__all__ = ["upsert_parsed_sms"] # Removed PocketBase client exports

logger = logging.getLogger(__name__)

# Removed AsyncPocketBaseClient and related functions (get_pb_client, get_async_pb_client)

COLLECTION_DEBIT: Literal["sms_data"] = "sms_data" # Still refers to the table name

async def upsert_parsed_sms(parsed_sms: ParsedSMS) -> None:
    """
    Upsert *parsed_sms* into the 'sms_data' table using SQLAlchemy.
    Guarantees idempotency by 'msg_id'.
    """
    try:
        # Convert ParsedSMS to a dictionary suitable for SmsData model
        record_data = {
            "msg_id": parsed_sms.msg_id,
            "original_body": parsed_sms.raw_body,
            "sender": parsed_sms.sender,
            "datetime": parsed_sms.date,
            "card": parsed_sms.card,
            "amount": parsed_sms.amount,
            "currency": parsed_sms.currency,
            "balance": parsed_sms.balance,
            "merchant": parsed_sms.merchant,
            "address": parsed_sms.address,
            "city": parsed_sms.city,
            "txn_type": parsed_sms.txn_type,
            "device_id": parsed_sms.device_id, # Assuming these exist in ParsedSMS
            "parser_version": parsed_sms.parser_version, # Assuming these exist in ParsedSMS
        }

        # Handle Decimal types for amount and balance
        record_data["amount"] = Decimal(str(record_data["amount"]))
        if record_data["balance"] is not None:
            record_data["balance"] = Decimal(str(record_data["balance"]))

        async with SessionLocal() as session:
            # Construct the INSERT ON CONFLICT DO UPDATE statement
            stmt = insert(SmsData).values(**record_data)
            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=[SmsData.msg_id], # Conflict on msg_id (msg_id)
                set_={
                    "original_body": stmt.excluded.original_body,
                    "sender": stmt.excluded.sender,
                    "datetime": stmt.excluded.datetime,
                    "card": stmt.excluded.card,
                    "amount": stmt.excluded.amount,
                    "currency": stmt.excluded.currency,
                    "balance": stmt.excluded.balance,
                    "merchant": stmt.excluded.merchant,
                    "address": stmt.excluded.address,
                    "city": stmt.excluded.city,
                    "txn_type": stmt.excluded.txn_type,
                    "device_id": stmt.excluded.device_id,
                    "parser_version": stmt.excluded.parser_version,
                }
            )
            await session.execute(on_conflict_stmt)
            await session.commit()
            logger.info("Parsed SMS record with msg_id '%s' successfully created/updated in DB.", parsed_sms.msg_id)
    except RetryError as exc:
        logger.error("DB upsert gave up: %s", exc)
        try:
            sentry_capture(exc, extras={"record": record_data})
        except NameError:
            logger.warning("sentry_capture not available, skipping Sentry capture.")
        raise
    except Exception as e:
        logger.error("Failed to create/update parsed SMS record in DB: %s", e)
        raise

# MCP Server Integration
import uvicorn
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
import datetime
from libs.sentry import sentry_capture
from libs.config import get_settings


# Создаём объект-сервер
mcp = FastMCP(
    "SQLAlchemy DB Connector", # Changed name
    dependencies=[],
    instructions="""
        This server provides tools to interact with 'sms_data' records directly in the database.
        You can:
        - create_parsed_sms(parsed_sms_data: dict): Create or update an SMS record using 'msg_id' as unique key.
        - get_record_by_id(record_id: int) -> dict: Retrieve a single SMS record by its primary key ID.
        - find_sms_records(sender: str = None, card: str = None, txn_type: str = None, min_amount: float = None, max_amount: float = None, start_date: str = None, end_date: str = None) -> list[dict]: Find SMS records based on various criteria.
        - update_record_by_id(record_id: int, updates: dict) -> str: Update an existing SMS record by its primary key ID.
        - delete_record_by_id(record_id: int) -> str: Delete an SMS record by its primary key ID.
    """
)
mcp.settings.port = 9122
mcp.settings.host = "0.0.0.0"

@mcp.tool()
async def get_record_by_id(record_id: int) -> dict[str, Any]:
    """
    Retrieves a single SMS record from the 'sms_data' table by its primary key ID.

    Args:
        record_id (int): The primary key ID of the SMS record to retrieve.

    Returns:
        dict: The retrieved record data, or an error message if not found.
    """
    try:
        async with SessionLocal() as session:
            stmt = select(SmsData).where(SmsData.id == record_id)
            result = await session.execute(stmt)
            record = result.scalar_one_or_none()

            if record:
                # Convert SmsData object to dictionary for response
                return {c.name: getattr(record, c.name) for c in record.__table__.columns}
            else:
                return {"error": f"Record with ID '{record_id}' not found in 'sms_data' collection."}
    except Exception as e:
        logger.error("Failed to retrieve record by ID: %s", e)
        return {"error": f"Failed to retrieve record: {e}"}

@mcp.tool()
async def find_sms_records(
    sender: Optional[str] = None,
    card: Optional[str] = None,
    txn_type: Optional[str] = None,
    min_amount: Optional[float] = None,
    max_amount: Optional[float] = None,
    start_date: Optional[str] = None, # ISO format string 'YYYY-MM-DDTHH:MM:SS'
    end_date: Optional[str] = None,   # ISO format string 'YYYY-MM-DDTHH:MM:SS'
) -> List[dict[str, Any]]:
    """
    Finds SMS records in the 'sms_data' table based on various criteria.

    Args:
        sender (str, optional): Filter by sender.
        card (str, optional): Filter by last 4 digits of card.
        txn_type (str, optional): Filter by transaction type.
        min_amount (float, optional): Filter by minimum amount (inclusive).
        max_amount (float, optional): Filter by maximum amount (inclusive).
        start_date (str, optional): Filter records from this date/time onwards (ISO format, e.g., '2024-01-01T00:00:00').
        end_date (str, optional): Filter records up to this date/time (ISO format, e.g., '2024-12-31T23:59:59').

    Returns:
        list[dict]: A list of matching records.
    """
    try:
        async with SessionLocal() as session:
            query = select(SmsData)
            conditions = []

            if sender:
                conditions.append(SmsData.sender == sender)
            if card:
                conditions.append(SmsData.card == card)
            if txn_type:
                conditions.append(SmsData.txn_type == txn_type)
            if min_amount is not None:
                conditions.append(SmsData.amount >= Decimal(str(min_amount)))
            if max_amount is not None:
                conditions.append(SmsData.amount <= Decimal(str(max_amount)))
            if start_date:
                try:
                    start_dt = dt.fromisoformat(start_date)
                    conditions.append(SmsData.datetime >= start_dt)
                except ValueError:
                    return {"error": "Invalid start_date format. Use ISO 8601 (e.g., '2024-01-01T00:00:00')."}
            if end_date:
                try:
                    end_dt = dt.fromisoformat(end_date)
                    conditions.append(SmsData.datetime <= end_dt)
                except ValueError:
                    return {"error": "Invalid end_date format. Use ISO 8601 (e.g., '2024-12-31T23:59:59')."}

            if conditions:
                query = query.where(*conditions)

            result = await session.execute(query)
            records = result.scalars().all()

            return [
                {c.name: getattr(record, c.name) for c in record.__table__.columns}
                for record in records
            ]
    except Exception as e:
        logger.error("Failed to find SMS records: %s", e)
        return {"error": f"Failed to find records: {e}"}

@mcp.tool()
async def update_record_by_id(record_id: int, updates: dict[str, Any]) -> str:
    """
    Updates an existing SMS record in the 'sms_data' table by its primary key ID.

    Args:
        record_id (int): The primary key ID of the record to update.
        updates (dict): A dictionary of fields and their new values.
                        For 'amount' and 'balance', ensure values are convertible to float.
                        For 'datetime', ensure value is an ISO format string.

    Returns:
        str: A message indicating success or failure.
    """
    try:
        async with SessionLocal() as session:
            # Prepare updates dictionary, converting types if necessary
            if 'amount' in updates and updates['amount'] is not None:
                updates['amount'] = Decimal(str(updates['amount']))
            if 'balance' in updates and updates['balance'] is not None:
                updates['balance'] = Decimal(str(updates['balance']))
            if 'datetime' in updates and isinstance(updates['datetime'], str):
                updates['datetime'] = dt.fromisoformat(updates['datetime'])

            stmt = update(SmsData).where(SmsData.id == record_id).values(**updates)
            result = await session.execute(stmt)
            await session.commit()

            if result.rowcount == 0:
                return f"Record with ID '{record_id}' not found in 'sms_data' collection. No update performed."

            return f"Record '{record_id}' in 'sms_data' collection updated successfully."
    except Exception as e:
        logger.error("Failed to update record by ID: %s", e)
        return f"Failed to update record: {e}"

@mcp.tool()
async def delete_record_by_id(record_id: int) -> str:
    """
    Deletes an SMS record from the 'sms_data' table by its primary key ID.

    Args:
        record_id (int): The primary key ID of the record to delete.

    Returns:
        str: A message indicating success or failure.
    """
    try:
        async with SessionLocal() as session:
            stmt = delete(SmsData).where(SmsData.id == record_id)
            result = await session.execute(stmt)
            await session.commit()

            if result.rowcount == 0:
                return f"Record with ID '{record_id}' not found in 'sms_data' collection. No deletion performed."

            return f"Record '{record_id}' deleted successfully from 'sms_data' collection."
    except Exception as e:
        logger.error("Failed to delete record by ID: %s", e)
        return f"Failed to delete record: {e}"

@mcp.tool()
async def create_parsed_sms(parsed_sms_data: dict) -> str:
    """
    Creates or updates a parsed SMS record in the 'sms_data' collection.
    Expects a dictionary with keys matching the ParsedSMS model fields.
    'msg_id' is used as the unique key for idempotency.
    'date' should be in ISO format string (e.g., '2024-01-01T12:30:00') if passed as string, otherwise datetime object.
    'amount' and 'balance' should be convertible to float.
    """
    try:
        if 'date' in parsed_sms_data and isinstance(parsed_sms_data['date'], str):
            parsed_sms_data['date'] = datetime.datetime.fromisoformat(parsed_sms_data['date'])

        if 'amount' in parsed_sms_data and parsed_sms_data['amount'] is not None:
            parsed_sms_data['amount'] = float(parsed_sms_data['amount'])
        if 'balance' in parsed_sms_data and parsed_sms_data['balance'] is not None:
            parsed_sms_data['balance'] = float(parsed_sms_data['balance'])

        # Ensure all required fields for ParsedSMS are present, or provide defaults/validation in ParsedSMS model
        parsed_sms = ParsedSMS(**parsed_sms_data)

        await upsert_parsed_sms(parsed_sms)
        return f"Parsed SMS record with msg_id '{parsed_sms.msg_id}' successfully created/updated."
    except Exception as e:
        logger.error("Failed to create/update parsed SMS record: %s", e)
        return f"Failed to create/update parsed SMS record: {e}"


@mcp.tool()
def get_current_datetime() -> str:
    """
    Returns the current local time in ISO-8601 format.
    """
    return dt.now().astimezone().isoformat()

if __name__ == "__main__":
    mcp.run(transport="sse")