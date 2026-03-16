"""
Main application module.
"""

import logging
from pathlib import Path


def setup_logging() -> None:
    """Configure logging for the application."""
    log_level = logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("app.log"),
        ],
    )


def main() -> None:
    """Main entry point for the application."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    logger.info("Starting application...")
    
    # Your application logic goes here
    print("Hello, World!")
    
    logger.info("Application finished successfully.")


if __name__ == "__main__":
    main()
