import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.append('src')

from gee_auth import GEEAuthenticator, get_authenticator, authenticate_ee, initialize_earth_engine


class TestGEEAuthenticator:
    """Test class for GEEAuthenticator."""

    def test_init(self):
        """Test initialization of GEEAuthenticator."""
        auth = GEEAuthenticator()
        assert auth.service_account is None
        assert auth.key_file is None
        assert auth.project is None
        assert auth.initialized is False

        # Test with parameters
        auth = GEEAuthenticator('service@example.com', 'key.json', 'project-id')
        assert auth.service_account == 'service@example.com'
        assert auth.key_file == 'key.json'
        assert auth.project == 'project-id'
        assert auth.initialized is False

    @patch('gee_auth.ee')
    def test_is_initialized_true(self, mock_ee):
        """Test is_initialized when EE is initialized."""
        # Mock the ee.Number().getInfo() call to return successfully
        mock_number = MagicMock()
        mock_ee.Number.return_value = mock_number
        mock_number.getInfo.return_value = 1

        auth = GEEAuthenticator()
        assert auth.is_initialized() is True

    @patch('gee_auth.ee')
    def test_is_initialized_false_exception(self, mock_ee):
        """Test is_initialized when EE raises an exception."""
        # Mock the ee.Number().getInfo() call to raise an exception
        mock_number = MagicMock()
        mock_ee.Number.return_value = mock_number
        mock_number.getInfo.side_effect = Exception("Not initialized")

        auth = GEEAuthenticator()
        assert auth.is_initialized() is False

    @patch('gee_auth.ee')
    def test_is_initialized_false_attribute_error(self, mock_ee):
        """Test is_initialized when EE has an AttributeError."""
        # Mock the ee.Number() call to raise an AttributeError
        mock_ee.Number.side_effect = AttributeError("No attribute Number")

        auth = GEEAuthenticator()
        assert auth.is_initialized() is False

    @patch('gee_auth.GEEAuthenticator.is_initialized')
    def test_authenticate_already_initialized(self, mock_is_initialized):
        """Test authenticate when already initialized."""
        mock_is_initialized.return_value = True
        
        auth = GEEAuthenticator()
        result = auth.authenticate()
        
        assert result is True
        mock_is_initialized.assert_called_once()

    @patch('gee_auth.GEEAuthenticator.is_initialized')
    @patch('gee_auth.GEEAuthenticator._authenticate_with_service_account')
    def test_authenticate_with_service_account(self, mock_auth_service, mock_is_initialized):
        """Test authenticate with service account."""
        mock_is_initialized.return_value = False
        
        auth = GEEAuthenticator('service@example.com', 'key.json')
        result = auth.authenticate()
        
        assert result is True
        mock_auth_service.assert_called_once()
        assert auth.initialized is True

    @patch('gee_auth.GEEAuthenticator.is_initialized')
    @patch('gee_auth.GEEAuthenticator._authenticate_with_user_account')
    def test_authenticate_with_user_account(self, mock_auth_user, mock_is_initialized):
        """Test authenticate with user account."""
        mock_is_initialized.return_value = False
        
        auth = GEEAuthenticator()
        result = auth.authenticate()
        
        assert result is True
        mock_auth_user.assert_called_once()
        assert auth.initialized is True

    @patch('gee_auth.GEEAuthenticator.is_initialized')
    def test_authenticate_exception(self, mock_is_initialized):
        """Test authenticate with exception."""
        mock_is_initialized.side_effect = Exception("Auth error")
        
        auth = GEEAuthenticator()
        result = auth.authenticate()
        
        assert result is False

    @patch('gee_auth.ee')
    def test_authenticate_with_service_account_implementation(self, mock_ee):
        """Test _authenticate_with_service_account implementation."""
        auth = GEEAuthenticator('service@example.com', 'key.json', 'project-id')
        
        # Mock the ServiceAccountCredentials
        mock_credentials = MagicMock()
        mock_ee.ServiceAccountCredentials.return_value = mock_credentials
        
        auth._authenticate_with_service_account()
        
        mock_ee.ServiceAccountCredentials.assert_called_once_with('service@example.com', 'key.json')
        mock_ee.Initialize.assert_called_once_with(mock_credentials, project='project-id')

    @patch('gee_auth.ee')
    def test_authenticate_with_user_account_persisted(self, mock_ee):
        """Test _authenticate_with_user_account with persisted credentials."""
        auth = GEEAuthenticator(project='project-id')
        
        # No exception from ee.Initialize
        auth._authenticate_with_user_account()
        
        mock_ee.Initialize.assert_called_once_with(project='project-id')
        mock_ee.Authenticate.assert_not_called()

    @patch('gee_auth.ee')
    def test_authenticate_with_user_account_new(self, mock_ee):
        """Test _authenticate_with_user_account with new credentials."""
        auth = GEEAuthenticator(project='project-id')
        
        # Create a custom exception for testing
        class MockEEException(Exception):
            pass
        
        # Mock ee.Initialize to raise an exception the first time and then return normally
        mock_ee.Initialize.side_effect = [MockEEException("No credentials"), None]
        
        # Call the method
        auth._authenticate_with_user_account()
        
        # Verify ee.Initialize was called twice (once for the failure, once after auth)
        assert mock_ee.Initialize.call_count == 2
        # Verify Authenticate was called once
        mock_ee.Authenticate.assert_called_once()

    def test_get_authenticator(self):
        """Test get_authenticator function."""
        auth = get_authenticator('service@example.com', 'key.json', 'project-id')
        
        assert isinstance(auth, GEEAuthenticator)
        assert auth.service_account == 'service@example.com'
        assert auth.key_file == 'key.json'
        assert auth.project == 'project-id'

    @patch('gee_auth.get_authenticator')
    def test_authenticate_ee(self, mock_get_authenticator):
        """Test authenticate_ee function."""
        mock_auth = MagicMock()
        mock_auth.authenticate.return_value = True
        mock_get_authenticator.return_value = mock_auth
        
        result = authenticate_ee('service@example.com', 'key.json', 'project-id')
        
        assert result is True
        mock_get_authenticator.assert_called_once_with('service@example.com', 'key.json', 'project-id')
        mock_auth.authenticate.assert_called_once()

    @patch('gee_auth.ee')
    def test_initialize_earth_engine_class_method_with_project(self, mock_ee):
        """Test initialize_earth_engine class method with project ID."""
        auth = GEEAuthenticator(project='project-id')
        result = auth.initialize_earth_engine()
        
        assert result is True
        mock_ee.Initialize.assert_called_once_with(project='project-id')
        assert auth.initialized is True

    @patch('gee_auth.ee')
    def test_initialize_earth_engine_class_method_without_project(self, mock_ee):
        """Test initialize_earth_engine class method without project ID."""
        auth = GEEAuthenticator()
        result = auth.initialize_earth_engine()
        
        assert result is True
        mock_ee.Initialize.assert_called_once_with()
        assert auth.initialized is True

    @patch('gee_auth.ee')
    def test_initialize_earth_engine_class_method_exception(self, mock_ee):
        """Test initialize_earth_engine class method with exception."""
        mock_ee.Initialize.side_effect = Exception("Init error")
        
        auth = GEEAuthenticator()
        result = auth.initialize_earth_engine()
        
        assert result is False
        mock_ee.Initialize.assert_called_once()
        assert auth.initialized is False

    @patch('gee_auth.ee')
    def test_initialize_earth_engine_function_with_project(self, mock_ee):
        """Test initialize_earth_engine function with project ID."""
        result = initialize_earth_engine('project-id')
        
        assert result is True
        mock_ee.Initialize.assert_called_once_with(project='project-id')

    @patch('gee_auth.ee')
    def test_initialize_earth_engine_function_without_project(self, mock_ee):
        """Test initialize_earth_engine function without project ID."""
        result = initialize_earth_engine()
        
        assert result is True
        mock_ee.Initialize.assert_called_once_with()

    @patch('gee_auth.ee')
    def test_initialize_earth_engine_function_exception(self, mock_ee):
        """Test initialize_earth_engine function with exception."""
        mock_ee.Initialize.side_effect = Exception("Init error")
        
        result = initialize_earth_engine()
        
        assert result is False
        mock_ee.Initialize.assert_called_once() 