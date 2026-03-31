"""
Babel — FastAPI application entrypoint.
"""

import logging
import logging.handlers
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from src.config import get_settings
from src.db.database import init_db
from src.scheduler import start_scheduler, stop_scheduler
from src.web.routes import router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        level=settings.LOG_LEVEL,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    # Add rotating file handler
    log_file = Path(__file__).parent.parent / "data" / "babel.log"
    log_file.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.handlers.RotatingFileHandler(
        str(log_file), maxBytes=5*1024*1024, backupCount=3
    )
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
    logging.getLogger().addHandler(file_handler)

    # Suppress noisy loggers but allow scanner debug
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logger = logging.getLogger("babel")

    # Initialize database
    await init_db(settings.DB_PATH)
    logger.info("Database initialized at %s", settings.DB_PATH)

    # Set up templates
    templates_dir = Path(__file__).parent / "web" / "templates"
    app.state.templates = Jinja2Templates(directory=str(templates_dir))

    # Start scheduler
    start_scheduler()
    logger.info("Scheduler started (interval: %dh)", settings.SCAN_INTERVAL_HOURS)

    yield

    # Shutdown
    stop_scheduler()
    logger.info("Scheduler stopped")


app = FastAPI(title="Babel", lifespan=lifespan)
app.include_router(router)

# Mount static files if directory exists
static_dir = Path(__file__).parent / "web" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
