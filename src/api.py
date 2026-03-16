from contextlib import asynccontextmanager
import logging

from fastapi import FastAPI

import config


@asynccontextmanager
async def lifespan(_: FastAPI):
    print("Initializing API...")
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(config.LOGS_FOLDER / "app.log"),
            ],
        )
    yield


app = FastAPI(title="SIGCore API", lifespan=lifespan)

@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
