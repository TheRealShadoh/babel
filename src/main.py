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

from src import __version__
from src.config import get_settings
from src.db.database import init_db
from src.scheduler import start_scheduler, stop_scheduler
from src.watchdog import start_watchdog, stop_watchdog
from src.web.auth import BasicAuthMiddleware
from src.web.routes import router

_LOG_FILE_HANDLER_NAME = "babel_rotating_file_handler"


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logging.basicConfig(
        level=settings.LOG_LEVEL,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    # Add rotating file handler — guarded so repeated lifespan runs (e.g.
    # `--reload`, or multiple app instances in a test session) don't stack
    # duplicate handlers and duplicate every log line.
    root_logger = logging.getLogger()
    if not any(getattr(h, "name", None) == _LOG_FILE_HANDLER_NAME for h in root_logger.handlers):
        log_file = Path(__file__).parent.parent / "data" / "babel.log"
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            str(log_file), maxBytes=5*1024*1024, backupCount=3
        )
        file_handler.name = _LOG_FILE_HANDLER_NAME
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
        root_logger.addHandler(file_handler)

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
    templates = Jinja2Templates(directory=str(templates_dir))
    templates.env.globals["babel_version"] = __version__
    app.state.templates = templates

    # Watch for event-loop stalls. A blocked loop stops serving HTTP *and*
    # stops reaping child processes; Docker will report the container unhealthy
    # but never restart it, so the process has to detect this itself.
    await start_watchdog(
        settings.WATCHDOG_INTERVAL,
        settings.WATCHDOG_UNHEALTHY_LAG,
        settings.WATCHDOG_ABORT_LAG,
    )

    # Start scheduler
    await start_scheduler()

    yield

    # Shutdown
    stop_scheduler()
    logger.info("Scheduler stopped")
    await stop_watchdog()


app = FastAPI(title="Babel", lifespan=lifespan)
app.add_middleware(BasicAuthMiddleware)
app.include_router(router)

# Mount static files if directory exists
static_dir = Path(__file__).parent / "web" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
