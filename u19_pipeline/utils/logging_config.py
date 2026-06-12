"""Logging helpers for u19_pipeline.

Library usage (u19_pipeline/*):
    Call get_logger(__name__) at module level.  No handlers are attached here;
    the caller (application or script) is responsible for configuring them.

Script usage (scripts/*):
    Call setup_logging() once at the top of the __main__ block.  This installs
    a Rich handler on the root logger so all u19_pipeline loggers emit
    formatted output without disturbing any downstream caller that configures
    their own handlers.
"""

import logging

# Silence the library's own loggers by default (PEP 396 / logging HOWTO).
# Downstream code or scripts call setup_logging() to attach real handlers.
logging.getLogger("u19_pipeline").addHandler(logging.NullHandler())


def get_logger(name: str) -> logging.Logger:
    """Return a named logger.  Never configures handlers — safe for library use."""
    return logging.getLogger(name)


def setup_logging(level: int = logging.INFO) -> None:
    """Configure the root logger with a Rich handler.

    Call this exactly once at the entry point of a script or application.
    Safe to call multiple times (idempotent via basicConfig semantics).
    """
    from rich.console import Console
    from rich.logging import RichHandler

    logging.basicConfig(
        level=level,
        format="%(message)s",
        datefmt="[%X]",
        handlers=[
            RichHandler(
                console=Console(stderr=True),
                rich_tracebacks=True,
                show_path=True,
            )
        ],
    )
