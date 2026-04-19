from __future__ import annotations

import argparse
import asyncio
from datetime import UTC, datetime, timedelta

from press_intelligence.core.config import get_settings
from press_intelligence.core.logging import configure_logging, get_logger
from press_intelligence.services.guardian_pipeline import GuardianPipelineService

logger = get_logger(__name__)


async def run_bootstrap(start_date: str, end_date: str, force: bool) -> dict[str, object]:
    service = GuardianPipelineService(get_settings())
    return await service.bootstrap(start_date=start_date, end_date=end_date, force=force)


def main() -> None:
    parser = argparse.ArgumentParser(description="Bootstrap BigQuery resources and load Guardian data.")
    parser.add_argument("--start-date", dest="start_date", default=None)
    parser.add_argument("--end-date", dest="end_date", default=None)
    parser.add_argument("--days", dest="days", type=int, default=3)
    parser.add_argument("--force", dest="force", action="store_true")
    args = parser.parse_args()

    configure_logging(get_settings())

    end = datetime.now(UTC).date()
    start = end - timedelta(days=max(args.days - 1, 0))
    start_date = args.start_date or start.isoformat()
    end_date = args.end_date or end.isoformat()

    logger.info(
        "bootstrap.start",
        start_date=start_date,
        end_date=end_date,
        force=args.force,
    )
    result = asyncio.run(run_bootstrap(start_date=start_date, end_date=end_date, force=args.force))
    logger.info("bootstrap.complete", **result)


if __name__ == "__main__":
    main()
