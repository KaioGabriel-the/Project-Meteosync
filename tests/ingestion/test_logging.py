import logging
import tempfile
import unittest
from pathlib import Path

from scripts.utils.logging_config import configure_ingestion_logger


class IngestionLoggingTests(unittest.TestCase):
    def test_logger_writes_to_stdout_and_file(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "ingestor.log"
            logger = configure_ingestion_logger("test_ingestor_logger", log_path)

            self.assertTrue(any(type(handler) is logging.StreamHandler for handler in logger.handlers))
            self.assertTrue(any(type(handler) is logging.FileHandler for handler in logger.handlers))
            self.assertEqual(log_path.parent, Path(temp_dir))

            for handler in logger.handlers:
                handler.close()
            logger.handlers.clear()
