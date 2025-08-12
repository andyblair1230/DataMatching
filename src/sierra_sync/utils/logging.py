from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        # Attach any extra fields passed via logger.bind-like style (record.__dict__ extras)
        for k, v in record.__dict__.items():
            if k not in (
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            ):
                payload[k] = v
        return json.dumps(payload, ensure_ascii=False)


def get_logger(
    name: str, logs_root: Path, run_id: str | None = None, console_level: int = logging.INFO
) -> logging.Logger:
    """
    Create/get a logger that writes JSON lines to logs_root/YYYYMMDD/run_id.log
    and human-readable INFO to console.
    """
    logger = logging.getLogger(name)
    if getattr(logger, "_sierra_configured", False):
        return logger

    logger.setLevel(logging.DEBUG)

    # Console handler (brief)
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    ch.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    logger.addHandler(ch)

    # File handler (JSON)
    date_part = datetime.now().strftime("%Y%m%d")
    run_part = run_id or datetime.now().strftime("%H%M%S")
    log_dir = Path(logs_root) / date_part
    _ensure_dir(log_dir)
    fh_path = log_dir / f"{run_part}.log"
    fh = logging.FileHandler(fh_path, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(JsonFormatter())
    logger.addHandler(fh)

    logger._sierra_configured = True  # type: ignore[attr-defined]
    logger.debug("logger_initialized", extra={"log_file": str(fh_path)})
    return logger
