#!/usr/bin/env python3
"""
Mock patterns and utilities for external services.
Provides standardized mocks for databases, APIs, and external services.
"""

import sys
import os
import json
from typing import Dict, List, Any, Optional, AsyncGenerator
from unittest.mock import AsyncMock, Mock, MagicMock
from datetime import datetime, date

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class DatabaseMock:
    """Mock database operations with realistic behavior."""

    def __init__(self):
        self.data = {}
        self.session = self._create_mock_session()
        self.transaction = Mock()

    def _create_mock_session(self):
        """Create a mock database session."""
        session = Mock()

        # Mock execute method
        session.execute = AsyncMock()
        session.execute.return_value = Mock()
        session.execute.return_value.scalars = Mock()
        session.execute.return_value.scalars.return_value.all = Mock(return_value=[])

        # Mock scalar methods
        session.scalar = AsyncMock()
        session.scalars = AsyncMock()

        # Mock transaction methods
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        session.flush = AsyncMock()

        # Mock CRUD operations
        session.add = Mock()
        session.delete = Mock()

        return session

    def mock_query_result(self, result_data: List[Any]):
        """Mock query to return specific data."""
        self.session.execute.return_value.scalars.return_value.all.return_value = result_data
        return self.session.execute.return_value

    def mock_scalar_result(self, result_data: Any):
        """Mock scalar query to return specific data."""
        self.session.scalar.return_value = result_data
        return self.session.scalar

    def mock_aggregate_result(self, aggregate_type: str, result: Any):
        """Mock aggregate query results."""
        return result


class StockDataMock:
    """Mock stock data provider for testing."""

    def __init__(self):
        self.mock_stocks = {}
        self.mock_quotes = {}
        self.mock_klines = {}
        self.mock_transactions = {}

    def add_stock(self, code: str, stock_data: Dict[str, Any]):
        """Add a stock to the mock data."""
        self.mock_stocks[code] = stock_data

    def add_quote(self, code: str, quote_data: Dict[str, Any]):
        """Add quote data for a stock."""
        self.mock_quotes[code] = quote_data

    def add_kline(self, code: str, period: str, kline_data: List[Dict[str, Any]]):
        """Add kline data for a stock."""
        key = f"{code}_{period}"
        self.mock_klines[key] = kline_data

    def get_stock_manager_mock(self):
        """Get a mock stock manager."""
        manager = AsyncMock()

        # Mock is_exists
        manager.is_exists.side_effect = lambda code: code in self.mock_stocks

        # Mock read_all
        manager.read_all.return_value = list(self.mock_stocks.values())

        # Mock get_quotes
        def mock_get_quotes(codes_str):
            codes = codes_str.split(',')
            result = {}
            for code in codes:
                if code in self.mock_quotes:
                    result[code] = self.mock_quotes[code]
            return result
        manager.get_quotes.side_effect = mock_get_quotes

        # Mock get_klines
        def mock_get_klines(codes_str, period):
            codes = codes_str.split(',')
            result = {}
            for code in codes:
                key = f"{code}_{period}"
                if key in self.mock_klines:
                    result[code] = self.mock_klines[key]
            return result
        manager.get_klines.side_effect = mock_get_klines

        return manager


class AuthenticationMock:
    """Mock authentication system for testing."""

    def __init__(self):
        self.users = {}
        self.active_tokens = {}

    def add_user(self, email: str, password: str, user_data: Dict[str, Any] = None):
        """Add a user to the mock authentication system."""
        if user_data is None:
            user_data = {}

        self.users[email] = {
            'password': password,
            'email': email,
            'username': user_data.get('username', email.split('@')[0]),
            'is_active': user_data.get('is_active', True),
            'is_verified': user_data.get('is_verified', True),
            'is_superuser': user_data.get('is_superuser', False),
            'id': user_data.get('id', len(self.users) + 1),
        }

    def generate_token(self, email: str) -> str:
        """Generate a mock JWT token."""
        token = f"mock_token_{email}_{datetime.now().timestamp()}"
        self.active_tokens[token] = email
        return token

    def validate_token(self, token: str) -> Optional[Dict[str, Any]]:
        """Validate a mock JWT token."""
        if token in self.active_tokens:
            email = self.active_tokens[token]
            return self.users.get(email)
        return None

    def get_user(self, email: str) -> Optional[Dict[str, Any]]:
        """Get user data by email."""
        return self.users.get(email)

    def get_auth_mock(self):
        """Get a mock authentication service."""
        auth = AsyncMock()

        # Mock login
        auth.login.side_effect = lambda email, password: {
            'access_token': self.generate_token(email),
            'token_type': 'bearer'
        } if email in self.users and self.users[email]['password'] == password else None

        # Mock get_current_user
        auth.get_current_user.side_effect = lambda token: self.validate_token(token)

        return auth


class ExternalAPIMock:
    """Mock external API services."""

    def __init__(self):
        self.responses = {}
        self.request_history = []

    def set_response(self, url: str, method: str, response_data: Any, status_code: int = 200):
        """Set a mock response for an API call."""
        key = f"{method.upper()}:{url}"
        self.responses[key] = {
            'data': response_data,
            'status_code': status_code
        }

    def get_requests_mock(self):
        """Get a mock requests library."""
        mock_requests = Mock()

        # Mock GET requests
        def mock_get(url, **kwargs):
            self.request_history.append({'method': 'GET', 'url': url, 'kwargs': kwargs})
            key = f"GET:{url}"

            if key in self.responses:
                response_data = self.responses[key]
                mock_response = Mock()
                mock_response.status_code = response_data['status_code']
                mock_response.json.return_value = response_data['data']
                mock_response.text = json.dumps(response_data['data'])
                mock_response.raise_for_status.return_value = None
                return mock_response

            # Default response
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.json.return_value = {}
            mock_response.text = '{}'
            return mock_response

        # Mock POST requests
        def mock_post(url, **kwargs):
            self.request_history.append({'method': 'POST', 'url': url, 'kwargs': kwargs})
            key = f"POST:{url}"

            if key in self.responses:
                response_data = self.responses[key]
                mock_response = Mock()
                mock_response.status_code = response_data['status_code']
                mock_response.json.return_value = response_data['data']
                mock_response.text = json.dumps(response_data['data'])
                mock_response.raise_for_status.return_value = None
                return mock_response

            # Default response
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.json.return_value = {}
            mock_response.text = '{}'
            return mock_response

        mock_requests.get = mock_get
        mock_requests.post = mock_post

        return mock_requests


class ImageProcessingMock:
    """Mock image processing and OCR services."""

    def __init__(self):
        self.text_responses = {}
        self.default_response = "captcha_text"

    def set_ocr_response(self, image_path: str, text: str):
        """Set OCR response for a specific image."""
        self.text_responses[image_path] = text

    def set_default_response(self, text: str):
        """Set default OCR response."""
        self.default_response = text

    def get_img_to_text_mock(self):
        """Get a mock image-to-text function."""
        def mock_img_to_text(image_input):
            # Handle different input types
            if isinstance(image_input, str):
                # File path or base64 string
                if image_input in self.text_responses:
                    return self.text_responses[image_input]
                elif os.path.exists(image_input):
                    # Extract filename from path
                    filename = os.path.basename(image_input)
                    filename_without_ext = os.path.splitext(filename)[0]
                    return filename_without_ext
            return self.default_response

        return mock_img_to_text


# Mock factories
class MockFactory:
    """Factory for creating standardized mocks."""

    @staticmethod
    def create_database_mock() -> DatabaseMock:
        """Create a database mock."""
        return DatabaseMock()

    @staticmethod
    def create_stock_data_mock() -> StockDataMock:
        """Create a stock data mock."""
        return StockDataMock()

    @staticmethod
    def create_authentication_mock() -> AuthenticationMock:
        """Create an authentication mock."""
        return AuthenticationMock()

    @staticmethod
    def create_external_api_mock() -> ExternalAPIMock:
        """Create an external API mock."""
        return ExternalAPIMock()

    @staticmethod
    def create_image_processing_mock() -> ImageProcessingMock:
        """Create an image processing mock."""
        return ImageProcessingMock()


# Context managers for mock setup
class MockContext:
    """Context manager for setting up mocks during tests."""

    def __init__(self, mock_obj, target_path: str):
        self.mock_obj = mock_obj
        self.target_path = target_path
        self.patcher = None

    def __enter__(self):
        import sys
        if '.' in self.target_path:
            module_path, attr_name = self.target_path.rsplit('.', 1)
            module = sys.modules[module_path]
            original = getattr(module, attr_name)
            setattr(module, attr_name, self.mock_obj)
        else:
            original = None

        return self.mock_obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        import sys
        if '.' in self.target_path:
            module_path, attr_name = self.target_path.rsplit('.', 1)
            # Restore original if needed
            # For simplicity, we'll leave the mock in place

    @staticmethod
    def patch_database(target_path: str = 'app.db'):
        """Patch database operations."""
        db_mock = MockFactory.create_database_mock()
        return MockContext(db_mock.session, f'{target_path}.async_session_maker')

    @staticmethod
    def patch_stock_manager(target_path: str = 'app.stock.manager'):
        """Patch stock manager."""
        stock_mock = MockFactory.create_stock_data_mock()
        manager_mock = stock_mock.get_stock_manager_mock()
        return MockContext(manager_mock, f'{target_path}.AllStocks')

    @staticmethod
    def patch_authentication(target_path: str = 'app.users.auth'):
        """Patch authentication."""
        auth_mock = MockFactory.create_authentication_mock()
        return MockContext(auth_mock.get_auth_mock(), f'{target_path}')


# Test data setup helpers
def setup_test_environment():
    """Set up complete test environment with all mocks."""
    return {
        'database': MockFactory.create_database_mock(),
        'stock_data': MockFactory.create_stock_data_mock(),
        'authentication': MockFactory.create_authentication_mock(),
        'external_api': MockFactory.create_external_api_mock(),
        'image_processing': MockFactory.create_image_processing_mock(),
        'trading_date': MockFactory.create_trading_date_mock(),
    }