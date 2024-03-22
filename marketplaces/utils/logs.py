import logging
import structlog

class Logs:
    def __init__(self, name: str, log_level=logging.INFO):
        self.name = name
        self.log_level = log_level
        self.logger = structlog.get_logger(logger_name=self.name)

    def configure_logger(self, log_level):
        """Configure structlog logger settings."""
        structlog.configure(
            processors=[
                structlog.stdlib.add_logger_name,
                structlog.contextvars.merge_contextvars,  # Merges context variables into the log entries
                structlog.stdlib.add_log_level,  # Add log level info
                structlog.stdlib.PositionalArgumentsFormatter(),
                structlog.processors.StackInfoRenderer(),
                structlog.processors.set_exc_info,  # Adds exception information to log entries
                structlog.processors.TimeStamper(fmt="%Y-%m-%d %H:%M:%S", utc=False),  # Add time to log entries
                structlog.dev.ConsoleRenderer(),  # Development-friendly console output
            ],
            wrapper_class=structlog.make_filtering_bound_logger(self.log_level), # Filter level INFO and above logs
            context_class=dict,
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=False,  # Caching of config. Dev - False, Prod - True
        )

        # Set the log level for the root logger to control which messages are shown
        logging.basicConfig(level=log_level)

    def get_logger(self):
        """Return the configured structlog logger."""
        return self.logger