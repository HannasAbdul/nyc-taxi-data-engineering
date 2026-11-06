import logging

from ingestion.src.nyc_taxi.config import LOG_DIR


def setup_logging() -> None:
    """Configure local logging without overriding an existing host logger.

    Airflow and other orchestrators configure their own root handlers.
    This function only installs local handlers when none are configured.
    """
    root_logger = logging.getLogger()
    if root_logger.handlers:
        return

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_DIR / "ingestion.log"),
            logging.StreamHandler(),
        ],
    )
