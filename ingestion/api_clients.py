import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def get_session_with_retries(retries=3, backoff_factor=0.3, status_forcelist=(429, 500, 502, 504)):
    """Creates a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session

def fetch_apifootball_data(api_key: str, endpoint: str, params: dict) -> dict:
    """
    Fetches data from the apifootball.com API.

    Args:
        api_key: The API key for authentication.
        endpoint: The API endpoint to fetch (e.g., 'get_teams' or 'get_standings').
        params: A dictionary of parameters for the API call.

    Returns:
        A dictionary with the API response.
    """
    
    BASE_URL = "https://apiv3.apifootball.com/"
    params['APIkey'] = api_key
    params['action'] = endpoint

    session = get_session_with_retries()
    try:
        response = session.get(BASE_URL, params=params)
        response.raise_for_status()  # Raises an HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        # logging.error(f"Error fetching data from apifootball.com: {e}", exc_info=True)
        # return None
        raise e

def fetch_apisports_data(api_key: str, endpoint: str, params: dict) -> dict:
    """
    Fetches data from the api-sports.io API.

    Args:
        api_key: The API key for authentication.
        endpoint: The API endpoint to fetch (e.g., 'teams' or 'standings').
        params: A dictionary of parameters for the API call.

    Returns:
        A dictionary with the API response.
    """
    BASE_URL = "https://v3.football.api-sports.io/"
    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': api_key
    }
    
    session = get_session_with_retries()
    try:
        response = session.get(f"{BASE_URL}{endpoint}", headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        # logging.error(f"Error fetching data from api-sports.io: {e}", exc_info=True)
        # return None
        raise e