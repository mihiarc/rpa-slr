import ee
import os
import json
from pathlib import Path

class GEEAuthenticator:
    """
    Class to handle Google Earth Engine authentication.
    """
    
    def __init__(self, service_account=None, key_file=None, project=None):
        """
        Initialize authenticator with optional service account credentials.
        
        Args:
            service_account (str, optional): Service account email
            key_file (str, optional): Path to service account key file
            project (str, optional): GEE project ID
        """
        self.service_account = service_account
        self.key_file = key_file
        self.project = project
        self.initialized = False
    
    def authenticate(self):
        """
        Authenticate with Google Earth Engine.
        First tries to authenticate with credentials if provided,
        otherwise falls back to user authentication.
        
        Returns:
            bool: True if authentication was successful
        """
        try:
            if self.is_initialized():
                print("Already authenticated with Earth Engine")
                return True
                
            if self.service_account and self.key_file:
                self._authenticate_with_service_account()
            else:
                self._authenticate_with_user_account()
                
            self.initialized = True
            return True
        except Exception as e:
            print(f"Authentication failed: {str(e)}")
            return False
    
    def _authenticate_with_service_account(self):
        """
        Authenticate using service account credentials.
        """
        credentials = ee.ServiceAccountCredentials(self.service_account, self.key_file)
        
        if self.project:
            ee.Initialize(credentials, project=self.project)
        else:
            ee.Initialize(credentials)
            
        print(f"Successfully authenticated with service account: {self.service_account}")
    
    def _authenticate_with_user_account(self):
        """
        Authenticate using user account (interactive).
        """
        # First try to use previously persisted credentials
        try:
            if self.project:
                ee.Initialize(project=self.project)
            else:
                ee.Initialize()
            print("Successfully authenticated with Earth Engine using persisted credentials")
        except Exception:
            # If persisted credentials don't work, request new ones
            ee.Authenticate()
            if self.project:
                ee.Initialize(project=self.project)
            else:
                ee.Initialize()
            print("Successfully authenticated with Earth Engine using new credentials")
    
    def is_initialized(self):
        """
        Check if Earth Engine has already been initialized.
        
        Returns:
            bool: True if initialized
        """
        try:
            # Try to use a simple Earth Engine function to check if initialized
            ee.Number(1).getInfo()
            return True
        except Exception:
            # Catch any exception that might occur if EE is not initialized
            return False

    def initialize_earth_engine(self):
        """
        Initialize Earth Engine with the specified project.
        
        Returns:
            bool: True if initialization was successful
        """
        try:
            if self.project:
                ee.Initialize(project=self.project)
                print(f"Successfully initialized Earth Engine with project: {self.project}")
            else:
                ee.Initialize()
                print("Successfully initialized Earth Engine")
            self.initialized = True
            return True
        except Exception as e:
            print(f"Failed to initialize Earth Engine: {str(e)}")
            return False


# Convenient function to get a default authenticator
def get_authenticator(service_account=None, key_file=None, project=None):
    """
    Create and return a GEEAuthenticator instance.
    
    Args:
        service_account (str, optional): Service account email
        key_file (str, optional): Path to service account key file
        project (str, optional): GEE project ID
        
    Returns:
        GEEAuthenticator: An authenticator instance
    """
    return GEEAuthenticator(service_account, key_file, project)


# Simple function for backward compatibility
def authenticate_ee(service_account=None, key_file=None, project=None):
    """
    Authenticate with Google Earth Engine.
    
    Args:
        service_account (str, optional): Service account email
        key_file (str, optional): Path to service account key file
        project (str, optional): GEE project ID
        
    Returns:
        bool: True if authentication was successful
    """
    authenticator = get_authenticator(service_account, key_file, project)
    return authenticator.authenticate()


# Function to initialize Earth Engine with a project ID
def initialize_earth_engine(project_id=None):
    """
    Initialize Earth Engine with the specified project.
    
    Args:
        project_id (str, optional): Earth Engine project ID
        
    Returns:
        bool: True if initialization was successful
    """
    try:
        if project_id:
            ee.Initialize(project=project_id)
            print(f"Successfully initialized Earth Engine with project: {project_id}")
        else:
            ee.Initialize()
            print("Successfully initialized Earth Engine")
        return True
    except Exception as e:
        print(f"Failed to initialize Earth Engine: {str(e)}")
        return False

# Execute authentication when script is run directly
if __name__ == "__main__":
    print("Running Google Earth Engine authentication test...")
    
    # Check if already authenticated
    authenticator = get_authenticator()
    if authenticator.is_initialized():
        print("Earth Engine is already initialized.")
    else:
        print("Attempting to authenticate with Earth Engine...")
        success = authenticate_ee()
        if success:
            print("Authentication successful!")
        else:
            print("Authentication failed!") 