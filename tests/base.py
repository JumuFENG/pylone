#!/usr/bin/env python3
"""
Base test classes and utilities for Pylone test suite.
Provides common setup, teardown, and utility functions for all tests.
"""

import unittest
import sys
import os
from typing import Optional, List, Dict, Any
from unittest.mock import AsyncMock, Mock

# Add project root to Python path (done once here)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class BaseTestCase(unittest.TestCase):
    """Base test case for synchronous tests with common utilities."""
    
    def setUp(self):
        """Common synchronous setup."""
        self._setup_test_data()
        self._setup_mocks()
    
    def tearDown(self):
        """Common synchronous cleanup."""
        self._cleanup_test_data()
    
    def _setup_test_data(self):
        """Override in subclasses for test data setup."""
        pass
    
    def _setup_mocks(self):
        """Override in subclasses for mock setup."""
        pass
    
    def _cleanup_test_data(self):
        """Override in subclasses for test data cleanup."""
        pass


class BaseAsyncTestCase(unittest.IsolatedAsyncioTestCase):
    """Base test case for async tests with common setup and utilities."""
    
    async def asyncSetUp(self):
        """Common async setup for all tests."""
        await self._setup_test_data()
        await self._setup_mocks()
    
    async def asyncTearDown(self):
        """Common async cleanup."""
        await self._cleanup_test_data()
    
    async def _setup_test_data(self):
        """Override in subclasses for async test data setup."""
        pass
    
    async def _setup_mocks(self):
        """Override in subclasses for async mock setup."""
        pass
    
    async def _cleanup_test_data(self):
        """Override in subclasses for async test data cleanup."""
        pass
    
    def assertDictContainsSubset(self, actual: Dict[str, Any], expected_subset: Dict[str, Any], msg: Optional[str] = None):
        """Assert that actual dict contains all key-value pairs from expected_subset."""
        for key, value in expected_subset.items():
            with self.subTest(key=key):
                self.assertIn(key, actual, msg or f"Key '{key}' not found in actual dict")
                self.assertEqual(actual[key], value, msg or f"Value mismatch for key '{key}': expected {value}, got {actual[key]}")


class DatabaseTestCase(BaseAsyncTestCase):
    """Base test case for database operations with transaction rollback."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._transaction = None
        self._session = None
    
    async def asyncSetUp(self):
        """Set up database transaction for test isolation."""
        await super().asyncSetUp()
        await self._setup_database_transaction()
    
    async def asyncTearDown(self):
        """Rollback transaction to clean up test data."""
        await self._rollback_database_transaction()
        await super().asyncTearDown()
    
    async def _setup_database_transaction(self):
        """Set up database transaction. Override in subclasses."""
        pass
    
    async def _rollback_database_transaction(self):
        """Rollback database transaction. Override in subclasses."""
        pass


class APITestCase(BaseAsyncTestCase):
    """Base test case for API endpoint testing."""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = None
        self.base_url = "http://localhost:8000"
    
    async def asyncSetUp(self):
        """Set up API test client."""
        await super().asyncSetUp()
        await self._setup_api_client()
    
    async def _setup_api_client(self):
        """Set up API client. Override in subclasses."""
        pass


class MockTestCaseMixin:
    """Mixin providing common mock utilities."""
    
    def create_mock_user(self, **kwargs):
        """Create a mock user with default values."""
        defaults = {
            'id': 1,
            'email': 'test@example.com',
            'username': 'testuser',
            'is_active': True,
            'is_verified': True
        }
        defaults.update(kwargs)
        return Mock(**defaults)
    
    def create_mock_stock(self, **kwargs):
        """Create a mock stock with default values."""
        defaults = {
            'code': 'TEST001',
            'name': 'Test Stock',
            'typekind': 'stock',
            'setup_date': '2020-01-01',
            'quit_date': None
        }
        defaults.update(kwargs)
        return Mock(**defaults)
    
    def create_async_mock(self, return_value=None, **kwargs):
        """Create an AsyncMock with common configuration."""
        mock = AsyncMock(**kwargs)
        if return_value is not None:
            mock.return_value = return_value
        return mock
    
    def create_mock_session(self):
        """Create a mock database session."""
        session = Mock()
        session.execute = AsyncMock()
        session.commit = AsyncMock()
        session.rollback = AsyncMock()
        session.add = Mock()
        session.delete = Mock()
        session.query = Mock()
        return session


# Utility functions
def assert_lists_equal_ignore_order(list1: List[Any], list2: List[Any], msg: Optional[str] = None):
    """Assert that two lists contain the same elements regardless of order."""
    from collections import Counter
    counter1 = Counter(list1)
    counter2 = Counter(list2)
    assert counter1 == counter2, msg or f"Lists differ: {list1} vs {list2}"


def create_test_file_path(filename: str) -> str:
    """Create absolute path to a test file."""
    return os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'fixtures',
        filename
    )


def skip_if_no_database():
    """Decorator to skip tests if database is not available."""
    def decorator(test_func):
        def wrapper(self):
            try:
                from app.db import async_session_maker
                return test_func(self)
            except ImportError:
                self.skipTest("Database not available for testing")
        return wrapper
    return decorator


def skip_if_no_internet():
    """Decorator to skip tests if internet connection is not available."""
    def decorator(test_func):
        def wrapper(self):
            try:
                import requests
                requests.get('https://www.google.com', timeout=5)
                return test_func(self)
            except Exception:
                self.skipTest("Internet connection not available for testing")
        return wrapper
    return decorator