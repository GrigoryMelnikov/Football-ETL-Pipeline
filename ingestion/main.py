import os
import json
import uuid
import base64
from datetime import datetime, timezone
from . import api_clients
from . import gcp_utils
from . import config as cfg

# --- Configuration ---
PROJECT_ID = cfg.PROJECT_ID or os.environ.get("GCP_PROJECT")
BUCKET_NAME = cfg.BUCKET_NAME or os.environ.get("BUCKET_NAME")

# --- Helpers ---
def get_current_season() -> int:
    """
    Determines the current football season based on the month.
    Assumes the new season starts in August.
    Uses UTC time to avoid local timezone differences.
    """
    now = datetime.now(timezone.utc)
    return now.year if now.month >= 8 else now.year - 1

def get_league_ids(env_name) -> list[int] | None:
    raw = os.environ.get(env_name) or getattr(cfg, env_name, None)
    if not raw:
        return None
    try:
        ids = json.loads(raw)
        return [int(x) for x in ids]
    except Exception:
        return None
    
def rollback_uploaded_files(api_name, run_id, uploaded_files: list[str]):
    try:
        gcp_utils.remove_from_gcs(BUCKET_NAME, uploaded_files)
        gcp_utils.log_struct({
            "etl-stage": "injection",
            "event": "remove_uploaded_files",
            "api-source": api_name,
            "reason": "cleanup_successful",
            "run_id": run_id,
        }, severity="INFO")
    except Exception as e:
        gcp_utils.log_struct({
            "etl-stage": "injection",
            "event": "remove_uploaded_files",
            "api-source": api_name,
            "reason": str(e),
            "run_id": run_id,
            "files": uploaded_files,
        }, severity="CRITICAL")

def parse_trigger_message(event: dict) -> dict:
    """
    Parses the incoming Pub/Sub message to extract arguments.
    The message data is expected to be a base64-encoded JSON string.
    """
    if 'data' not in event or not event['data']:
        return {}
    try:
        message_data = base64.b64decode(event['data']).decode('utf-8')
        data = json.loads(message_data)
        return data if isinstance(data, dict) else {}
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
        gcp_utils.log_struct({
            "etl-stage": "injection",
            "event": "parse_trigger_message_error",
            "reason": "Malformed Pub/Sub message data",
            "error": str(e)
        }, severity="WARNING")
        return {}

# Generic fetch and store function
def fetch_and_store(run_id, api_name, api_key, endpoint, league_ids, season=None) -> dict:
    uploaded_files = []

    for league_id in league_ids:
        # api call
        try:
            if not season:
                season = get_current_season()
            if api_name == "apifootball":
                params = {"league_id": league_id}
                data = api_clients.fetch_apifootball_data(api_key, endpoint, params)
                dir_path = f"apifootball/season_{season}/league_{league_id}/{endpoint}"
            else:
                params = {"league": league_id}
                params["season"] = '2023' 
                data = api_clients.fetch_apisports_data(api_key, endpoint, params)
                dir_path = f"apisports/season_{season}/league_{league_id}/{endpoint}"
            if not data:
                raise Exception("No data returned")
        except Exception as e:
            gcp_utils.log_struct({
                "etl-stage": "injection",
                "event": "fecth_error",
                "api-source": api_name,
                "league_id": league_id,
                "endpoint": endpoint,
                "reason": str(e),
                "run_id": run_id,
            }, severity="ERROR")
            return {"success": False, "uploaded_files": uploaded_files} 
        
        # store data
        try: 
            timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            file_name = f"{dir_path}/{run_id}_{timestamp}.json"
            gcp_utils.upload_to_gcs(BUCKET_NAME, file_name, json.dumps(data))
            uploaded_files.append(file_name)
        except Exception as e:
            gcp_utils.log_struct({
                "etl-stage": "injection",
                "event": "store_error",
                "api-source": api_name,
                "league_id": league_id,
                "endpoint": endpoint,
                "reason": str(e),
                "run_id": run_id,
            }, severity="ERROR")
            return {"success": False, "uploaded_files": uploaded_files}  
    return {"success": True, "uploaded_files": uploaded_files}

# API-Football ingestion function
def ingest_apifootball(event, context):
    run_id = str(uuid.uuid4())
    api_name = "apifootball"

    api_key = gcp_utils.get_secret(PROJECT_ID, "apifootball-api-key")
    if not api_key:
        gcp_utils.log_struct({
            "etl-stage": "injection",
            "event": "no_api_key_found", 
            "api-source": "apifootball", 
            "reason": "missing_api_key"
        }, severity="ERROR")
        return
    
    args = parse_trigger_message(event)
    season = args.get('season')
    if not season:
        # If season is not in the message, fall back to the calculated value.
        season = get_current_season() # FREE TIER LIMITATION


    league_ids = args.get('leagues') 
    if league_ids is None:
        gcp_utils.log_struct({
            "etl-stage": "injection",
            "event": "no_league_provided", 
            "api-source": api_name,
            "api-source": "apifootball", 
            "reason": "leagues_not_in_trigger_message",
            "run_id": run_id,
        }, severity="ERROR")
        return
    
    teams_response = fetch_and_store(run_id, api_name, api_key, "get_teams", league_ids)
    if not teams_response['success']:
        gcp_utils.rollback_uploaded_files(api_name, run_id, teams_response["uploaded_files"] + standings_response['uploaded_files'])
        return
    
    standings_response = fetch_and_store(run_id, api_name, api_key, "get_standings", league_ids)

    if teams_response['success'] and standings_response['success']:
        gcp_utils.startDataflowPipeline(
            api_name=api_name, 
            uploaded_files=teams_response["uploaded_files"] + standings_response['uploaded_files']
        )
    else:
        gcp_utils.rollback_uploaded_files(api_name, run_id, teams_response["uploaded_files"] + standings_response['uploaded_files'])
        return

# API-Sports ingestion function
def ingest_apisports(event, context):
    run_id = str(uuid.uuid4())
    api_name = "apisports"

    api_key = gcp_utils.get_secret(PROJECT_ID, "apisports-api-key")
    if not api_key:
        gcp_utils.log_struct({
            "etl-stage": "injection",
            "event": "no_api_key_found", 
            "api-source": "apisports", 
            "reason": "missing_api_key"
        }, severity="ERROR")
        return

    args = parse_trigger_message(event)
    season = args.get('season')
    if not season:
        # If season is not in the message, fall back to the calculated value.
        # season = get_current_season()
        season = '2023' # FREE TIER LIMITATION


    league_ids = args.get('leagues')
    if not league_ids:
        gcp_utils.log_struct({
            "etl-stage": "injection",
            "event": "no_league_provided",
            "api-source": api_name,
            "reason": "leagues_not_in_trigger_message",
            "run_id": run_id,
        }, severity="ERROR")
        league_ids = get_league_ids("APISPORTS_LEAGUE_IDS")

    
    teams_response = fetch_and_store(run_id, api_name, api_key, "teams", league_ids, season)

    if not teams_response['success']:
        gcp_utils.rollback_uploaded_files(api_name, run_id, teams_response["uploaded_files"] + standings_response['uploaded_files'])
        return
    
    standings_response = fetch_and_store(run_id, api_name, api_key, "standings", league_ids, season)

    if teams_response['success'] and standings_response['success']:
        gcp_utils.startDataflowPipeline(
            api_name=api_name, 
            uploaded_files=teams_response["uploaded_files"] + standings_response['uploaded_files']
        )
    else:
        gcp_utils.rollback_uploaded_files(api_name, run_id, teams_response["uploaded_files"] + standings_response['uploaded_files'])
        return
        
if __name__ == "__main__":
    # For local testing purposes only
    ingest_apifootball({}, None)
