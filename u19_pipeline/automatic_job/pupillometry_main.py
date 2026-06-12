import sys

from u19_pipeline.utils.logging_config import get_logger

logger = get_logger(__name__)

if __name__ == "__main__":
    import time

    from scripts.conf_file_finding import try_find_conf_file

    try_find_conf_file()
    time.sleep(1)

    import u19_pipeline.automatic_job.pupillometry_handler as ph

    args = sys.argv[1:]
    logger.debug("args %s", args)
    logger.debug("args[0] %s", args[0])
    logger.debug("args[1] %s", args[1])
    logger.debug("args[2] %s", args[2])

    ph.PupillometryProcessingHandler.analyze_videos_pupillometry(args[0], args[1], args[2])
