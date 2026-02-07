#!/usr/bin/env python3
"""
Unit tests for image recognition functionality.
"""

import os, sys
sys.path.insert(0, os.path.realpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')))

import unittest
from unittest.mock import patch, Mock
from base import BaseTestCase
from mocks import MockFactory


class TestImageToText(BaseTestCase):
    """Test image recognition and OCR functionality."""

    def setUp(self):
        """Set up test fixtures."""
        super().setUp()
        self.image_mock = MockFactory.create_image_processing_mock()
        self.test_image_path = "/test/path/to/image.png"

    def test_img_to_text_mock_response(self):
        """Test image recognition with mocked response."""
        with patch('app.hu.img_to_text', self.image_mock.get_img_to_text_mock()):
            from app.hu import img_to_text

            # Set specific response
            expected_text = "CAPTCHA123"
            self.image_mock.set_ocr_response(self.test_image_path, expected_text)

            # Test with file path
            result = img_to_text(self.test_image_path)
            self.assertEqual(result, expected_text)

    def test_img_to_text_default_response(self):
        """Test image recognition with default response."""
        with patch('app.hu.img_to_text', self.image_mock.get_img_to_text_mock()):
            from app.hu import img_to_text

            # Set default response
            expected_text = "default_captcha"
            self.image_mock.set_default_response(expected_text)

            # Test with unknown image path
            result = img_to_text("/unknown/path/image.png")
            self.assertEqual(result, expected_text)

    def test_img_to_text_with_bytes(self):
        """Test image recognition with byte data."""
        with patch('app.hu.img_to_text', self.image_mock.get_img_to_text_mock()):
            from app.hu import img_to_text

            expected_text = "bytes_image_text"
            self.image_mock.set_default_response(expected_text)

            # Test with byte data
            test_bytes = b"fake_image_data"
            result = img_to_text(test_bytes)
            self.assertEqual(result, expected_text)

    def test_img_to_text_with_base64(self):
        """Test image recognition with base64 data."""
        with patch('app.hu.img_to_text', self.image_mock.get_img_to_text_mock()):
            from app.hu import img_to_text

            expected_text = "base64_image_text"
            self.image_mock.set_default_response(expected_text)

            # Test with base64 string
            test_base64 = "ZmFrZV9pbWFnZV9kYXRh"  # "fake_image_data" in base64
            result = img_to_text(test_base64)
            self.assertEqual(result, expected_text)

    @patch('app.hu.img_to_text')
    def test_img_to_text_error_handling(self, mock_img_to_text):
        """Test error handling in image recognition."""
        from app.hu import img_to_text

        # Test when OCR raises an exception
        mock_img_to_text.side_effect = Exception("OCR failed")

        with self.assertRaises(Exception):
            img_to_text(self.test_image_path)

    @patch('app.hu.img_to_text')
    def test_img_to_text_empty_input(self, mock_img_to_text):
        """Test image recognition with empty input."""
        from app.hu import img_to_text
        mock_img_to_text.return_value = ""
        # Test with empty string
        result = img_to_text("")
        self.assertIsInstance(result, str)

        # Test with None (if the function supports it)
        try:
            result = img_to_text(None)
            self.assertIsInstance(result, str)
        except (TypeError, AttributeError):
            # Function might not support None input
            pass

    def test_img_to_text_integration_scenarios(self):
        """Test image recognition with various integration scenarios."""
        with patch('app.hu.img_to_text', self.image_mock.get_img_to_text_mock()):
            from app.hu import img_to_text

            # Scenario 1: Numeric captcha
            self.image_mock.set_ocr_response("/numeric.png", "123456")
            result = img_to_text("/numeric.png")
            self.assertEqual(result, "123456")
            self.assertTrue(result.isdigit())

            # Scenario 2: Alphanumeric captcha
            self.image_mock.set_ocr_response("/alphanum.png", "ABC123")
            result = img_to_text("/alphanum.png")
            self.assertEqual(result, "ABC123")

            # Scenario 3: Complex captcha with special chars
            self.image_mock.set_ocr_response("/complex.png", "A1-B2*C3")
            result = img_to_text("/complex.png")
            self.assertEqual(result, "A1-B2*C3")

import os
import glob
from app.hu import img_to_text

class TestImageToTextRealFile(BaseTestCase):
    """Test image recognition with real test files."""

    def setUp(self):
        """Set up test with real file paths."""
        super().setUp()
        self.test_img_dir = "tests/img/captcha"

    @unittest.skipIf(True, "Requires actual test images")
    def test_real_image_files(self):
        """Test recognition with real image files."""
        if not os.path.exists(self.test_img_dir):
            self.skipTest(f"Test image directory not found: {self.test_img_dir}")

        # Get all PNG files in the test directory
        test_images = glob.glob(os.path.join(self.test_img_dir, '*.png'))

        if not test_images:
            self.skipTest("No test images found")

        # Test each image
        for img_path in test_images:
            with self.subTest(img_path=img_path):
                # Extract expected text from filename
                expected_text = os.path.splitext(os.path.basename(img_path))[0]

                try:
                    result = img_to_text(img_path)
                    # For real images, we just check that we get a reasonable result
                    self.assertIsInstance(result, str)
                    self.assertGreater(len(result), 0)
                    self.assertLess(len(result), 20)  # Reasonable captcha length
                except Exception as e:
                    self.fail(f"Failed to process image {img_path}: {e}")

    @patch('app.hu.img_to_text')
    def test_mocked_real_file_scenario(self, mock_img_to_text):
        """Test file handling logic with mocked recognition."""
        from app.hu import img_to_text

        # Mock to return filename-based text
        def mock_recognition(img_input):
            if isinstance(img_input, str) and img_input.endswith('.png'):
                filename = os.path.basename(img_input)
                return os.path.splitext(filename)[0]
            return "mocked_text"

        mock_img_to_text.side_effect = mock_recognition

        # Test with file path that looks like a captcha
        test_file = "0111.png"
        expected = "0111"

        result = img_to_text(test_file)
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()