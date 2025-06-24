# services/xml_watcher/watcher.py
"""
XML Watcher
===========

Если мобильное приложение не успело прислать SMS-ы в реальном времени,
дочитываем backup-XML и кладём неизвестные сообщения в поток `sms_raw`.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import logging
import shutil
from pathlib import Path
import sys
from typing import Iterable, List

import xml.etree.ElementTree as ET
from libs.config import get_settings
from libs.models import RawSMS
from libs.nats_utils import publish_raw_sms, get_nats_connection
from libs.sentry import init_sentry, sentry_capture

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("xml_watcher")
log.setLevel(logging.INFO)

SCAN_INTERVAL = 10  # секунд
# ────────────────────────── XML helpers ──────────────────────────────


def _iter_sms(xml_path: Path) -> Iterable[RawSMS]:
    """Генерирует RawSMS из XML-дампа."""
    tree = ET.parse(xml_path)
    root = tree.getroot()

    for elem in root.findall("sms"):
        body = elem.get("body", "")
        date_ms = int(elem.get("date", "0"))
        date_dt = datetime.fromtimestamp(date_ms / 1_000, tz=timezone.utc)
        address = elem.get("address", "")
        msg_id = hashlib.sha1(f"{body}".encode()).hexdigest()

        yield RawSMS(
            source="xml",
            device_id="xml_backup",
            msg_id=msg_id,
            sender=address,
            date=date_dt.isoformat(),
            body=body,
        )


def _move_to_processed(src: Path, processed_dir: Path) -> None:
    """Перемещает импортированный файл в подпапку processed/."""
    processed_dir.mkdir(exist_ok=True)
    dst = processed_dir / src.name
    shutil.move(src, dst)
    log.info("Moved %s → %s", src, dst)


# ───────────────────────── обработчик XML ────────────────────────────


class XMLHandler:
    """Обрабатывает появление новых XML-файлов."""

    def __init__(self, processed_dir: Path, loop: asyncio.AbstractEventLoop) -> None:
        self._processed_dir = processed_dir
        self._loop = loop

    async def _process(self, xml_path: Path) -> None:
        log.info("Processing %s", xml_path)
        try:
            msgs: List[RawSMS] = await asyncio.to_thread(lambda: list(_iter_sms(xml_path)))
            nats = await get_nats_connection()

            for sms in msgs:
                await publish_raw_sms(nats, sms)

            _move_to_processed(xml_path, self._processed_dir)
            log.info("Imported %d message(s) from %s", len(msgs), xml_path)
        except Exception as exc:  # noqa: BLE001
            sentry_capture(exc, extras={"file": str(xml_path)})
            log.exception("Failed to import %s", xml_path)


# ───────────────────────────── main ──────────────────────────────────


async def _scan_loop(loop: asyncio.AbstractEventLoop,
                     backup_dir: Path,
                     processed_dir: Path) -> None:
    """Раз в SCAN_INTERVAL секунд ищем новые XML-файлы и обрабатываем их."""
    handler = XMLHandler(processed_dir, loop)

    while True:
        for xml_file in backup_dir.glob("*.xml"):
            # если файл ещё лежит в backup_dir, значит не обработан
            await handler._process(xml_file)
        await asyncio.sleep(SCAN_INTERVAL)


def main() -> None:
    settings = get_settings()
    init_sentry(release="xml_watcher@1.0.0")

    backup_dir = Path(settings.backup_dir).resolve()
    processed_dir = backup_dir / "processed"
    log.info("Watching XML dir: %s (polling every %ss)", backup_dir, SCAN_INTERVAL)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # запускаем фоновое сканирование
    loop.create_task(_scan_loop(loop, backup_dir, processed_dir))

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


if __name__ == "__main__":
    main()
