"""Tests for the main application module."""

import unittest
from unittest.mock import patch

from src.app import main, setup_logging


class TestApp(unittest.TestCase):
    """Test cases for the app module."""

    def test_setup_logging(self):
        """Test that logging is configured without errors."""
        # This should not raise any exceptions
        setup_logging()
        
        # Verify that logging is configured
        import logging
        self.assertIsNotNone(logging.getLogger(__name__))

    @patch('builtins.print')
    def test_main(self, mock_print):
        """Test the main function."""
        # Run the main function
        main()
        
        # Verify that print was called with expected message
        mock_print.assert_called_once_with("Hello, World!")


if __name__ == "__main__":
    unittest.main()
