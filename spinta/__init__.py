import importlib.metadata

from .logging_config import setup_logging

__version__ = importlib.metadata.version(__name__)


# Call the setup function to configure logging globally when the package is imported
setup_logging()
