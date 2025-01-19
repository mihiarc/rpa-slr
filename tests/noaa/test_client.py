"""
Tests for the NOAA API client.
"""

import pytest
import responses
from unittest.mock import patch
from src.noaa.client import NOAAClient, NOAAApiError

# Test data fixtures
SAMPLE_ANNUAL_RESPONSE = {
    "count": 2,
    "AnnualFloodCount": [
        {
            "stnId": "8638610",
            "stnName": "Sewells Point, VA",
            "year": 2010,
            "majCount": 0,
            "modCount": 1,
            "minCount": 6,
            "nanCount": 0
        },
        {
            "stnId": "8638610",
            "stnName": "Sewells Point, VA",
            "year": 2011,
            "majCount": 2,
            "modCount": 2,
            "minCount": 6,
            "nanCount": 0
        }
    ]
}

SAMPLE_PROJECTION_RESPONSE = {
    "count": 2,
    "DecadalProjection": [
        {
            "stnId": "8638610",
            "stnName": "Sewells Point, VA",
            "decade": 2050,
            "source": "test_source",
            "low": 85,
            "intLow": 100,
            "intermediate": 125,
            "intHigh": 150,
            "high": 185
        }
    ]
}

@pytest.fixture
def client():
    """Create a NOAAClient instance for testing."""
    return NOAAClient(requests_per_second=10.0)  # Higher rate limit for testing

@pytest.fixture
def mock_responses():
    """Setup mock responses using the responses library."""
    with responses.RequestsMock() as rsps:
        yield rsps

class TestNOAAClient:
    """Test suite for NOAAClient class."""
    
    def test_init_default_values(self):
        """Test client initialization with default values."""
        client = NOAAClient()
        assert client.api_base_url == "https://api.tidesandcurrents.noaa.gov/dpapi/prod/webapi"
        assert client.rate_limiter.requests_per_second == 2.0

    def test_init_custom_values(self):
        """Test client initialization with custom values."""
        client = NOAAClient(api_base_url="https://test.api", requests_per_second=5.0)
        assert client.api_base_url == "https://test.api"
        assert client.rate_limiter.requests_per_second == 5.0

    @responses.activate
    def test_fetch_annual_flood_counts_success(self, client):
        """Test successful fetch of annual flood counts."""
        responses.add(
            responses.GET,
            f"{client.api_base_url}/htf/htf_annual.json",
            json=SAMPLE_ANNUAL_RESPONSE,
            status=200
        )

        result = client.fetch_annual_flood_counts(station="8638610")
        assert len(result) == 2
        assert result[0]["stnId"] == "8638610"
        assert result[0]["year"] == 2010
        assert "majCount" in result[0]

    @responses.activate
    def test_fetch_annual_flood_counts_all_stations(self, client):
        """Test fetching flood counts for all stations."""
        responses.add(
            responses.GET,
            f"{client.api_base_url}/htf/htf_annual.json",
            json=SAMPLE_ANNUAL_RESPONSE,
            status=200
        )

        result = client.fetch_annual_flood_counts()
        assert len(result) == 2

    @responses.activate
    def test_fetch_annual_flood_counts_error(self, client):
        """Test handling of API errors in annual flood counts."""
        responses.add(
            responses.GET,
            f"{client.api_base_url}/htf/htf_annual.json",
            status=404
        )

        with pytest.raises(NOAAApiError):
            client.fetch_annual_flood_counts(station="8638610")

    @responses.activate
    def test_fetch_decadal_projections_success(self, client):
        """Test successful fetch of decadal projections."""
        responses.add(
            responses.GET,
            f"{client.api_base_url}/htf/htf_projection_decadal.json",
            json=SAMPLE_PROJECTION_RESPONSE,
            status=200
        )

        result = client.fetch_decadal_projections(station="8638610")
        assert result["count"] == 2
        assert "DecadalProjection" in result
        assert result["DecadalProjection"][0]["decade"] == 2050

    @responses.activate
    def test_fetch_decadal_projections_error(self, client):
        """Test handling of API errors in decadal projections."""
        responses.add(
            responses.GET,
            f"{client.api_base_url}/htf/htf_projection_decadal.json",
            status=500
        )

        with pytest.raises(NOAAApiError):
            client.fetch_decadal_projections(station="8638610")

    @responses.activate
    def test_invalid_json_response(self, client):
        """Test handling of invalid JSON responses."""
        responses.add(
            responses.GET,
            f"{client.api_base_url}/htf/htf_annual.json",
            body="Invalid JSON",
            status=200
        )

        with pytest.raises(NOAAApiError):
            client.fetch_annual_flood_counts()

    def test_rate_limiting(self, client):
        """Test that rate limiting is enforced."""
        with patch('time.sleep') as mock_sleep:
            with responses.RequestsMock() as rsps:
                rsps.add(
                    responses.GET,
                    f"{client.api_base_url}/htf/htf_annual.json",
                    json=SAMPLE_ANNUAL_RESPONSE,
                    status=200
                )
                
                # Make multiple requests
                for _ in range(3):
                    client.fetch_annual_flood_counts()
                
                # Verify rate limiting was applied
                assert mock_sleep.called 