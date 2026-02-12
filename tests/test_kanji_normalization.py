"""
Unit tests for Kanji normalization module.
"""

import unittest
import os
import sys

# Add the src directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.kanji_normalization import normalize_kanji_name


class TestKanjiNormalization(unittest.TestCase):
    """Test cases for Kanji normalization module."""
    
    def test_normalize_kanji_name_normal(self):
        """Test normalization with valid Kanji characters."""
        # Common Japanese Kanji characters
        input_str = "山田太郎"
        result = normalize_kanji_name(input_str)
        self.assertEqual(result, input_str)
    
    def test_normalize_kanji_name_with_ascii(self):
        """Test normalization with ASCII characters."""
        input_str = "Yamada Taro 山田太郎"
        result = normalize_kanji_name(input_str)
        self.assertEqual(result, input_str)
    
    def test_normalize_kanji_name_with_invalid_chars(self):
        """Test normalization with invalid characters."""
        # Using a character outside the allowed ranges
        input_str = "山田\U0010FFFF太郎"  # Very high Unicode code point
        result = normalize_kanji_name(input_str)
        self.assertEqual(result, "山田\u25A0太郎")  # Invalid char replaced with black square
    
    def test_normalize_kanji_name_with_newlines(self):
        """Test normalization with newline characters."""
        input_str = "山田\r\n太郎"
        result = normalize_kanji_name(input_str)
        self.assertEqual(result, "山田\n太郎")  # CR removed, LF kept
    
    def test_normalize_kanji_name_null_input(self):
        """Test normalization with null input."""
        result = normalize_kanji_name(None)
        self.assertEqual(result, "")
    
    def test_normalize_kanji_name_empty_input(self):
        """Test normalization with empty input."""
        result = normalize_kanji_name("")
        self.assertEqual(result, "")


if __name__ == "__main__":
    unittest.main()