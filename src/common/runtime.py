"""Small notebook runtime helpers."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable


def configure_logging(task_name: str) -> logging.Logger:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return logging.getLogger(task_name)


def widget_values(dbutils, names: Iterable[str]) -> dict[str, str]:
    return {name: dbutils.widgets.get(name) for name in names}


def log_metrics(logger: logging.Logger, status: str, **metrics: object) -> None:
    logger.info(json.dumps({"status": status, **metrics}, default=str, sort_keys=True))

