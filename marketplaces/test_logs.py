from utils.logging import Logging
import logging

# Example usage
if __name__ == "__main__":
    # Initialize a logger with a specific name and log level
    logging_instance = Logging(name="MyLogger", log_level=logging.INFO)
    logger = logging_instance.get_logger()

    # Use the logger with the adjusted structure
    logger.info(event="application_started")
    try:
        # Intentional error for demonstration
        1 / 0
    except Exception as e:
        logger.exception(event="application_error")

    logger.info(event="application_finished")
