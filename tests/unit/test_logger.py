def test_logging_setup():
    from src.utils.logger import get_logger, setup_logging

    setup_logging()
    logger = get_logger("test")
    logger.info("hello world")
