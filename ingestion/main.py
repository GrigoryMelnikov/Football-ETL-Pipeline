import os
import json
import uuid
from datetime import datetime, timezone
from . import api_clients
from . import gcp_utils
from . import config as cfg

# --- Configuration ---
PROJECT_ID = cfg.PROJECT_ID or os.environ.get("GCP_PROJECT")
BUCKET_NAME = cfg.BUCKET_NAME or os.environ.get("BUCKET_NAME")
PUBSUB_TOPIC_APIFOOTBALL = cfg.PUBSUB_TOPIC_APIFOOTBALL or os.environ.get("PUBSUB_TOPIC_APIFOOTBALL", None)
PUBSUB_TOPIC_APISPORTS = cfg.PUBSUB_TOPIC_APISPORTS or os.environ.get("PUBSUB_TOPIC_APISPORTS", None)

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
            "event": "remove_uploaded_files",
            "api": api_name,
            "reason": "cleanup_successful",
            "run_id": run_id,
        }, severity="INFO")
    except Exception as e:
        gcp_utils.log_struct({
            "event": "remove_uploaded_files",
            "api": api_name,
            "reason": str(e),
            "run_id": run_id,
            "files": uploaded_files,
        }, severity="ERROR")

# Generic fetch and store function
def fetch_and_store(run_id, api_name, api_key, endpoint, league_ids, season=None) -> list[str] | None:
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
                # params["season"] = season
                params["season"] = '2023' # FREE TIER LIMITATION
                data = api_clients.fetch_apisports_data(api_key, endpoint, params)
                dir_path = f"apisports/season_{season}/league_{league_id}/{endpoint}"
            if not data:
                raise Exception("No data returned")
        except Exception as e:
            gcp_utils.log_struct({
                "event": "fecth_error",
                "api": api_name,
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
                "event": "store_error",
                "api": api_name,
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
        gcp_utils.log_struct({"event": "no_api_key_found", "source": "apifootball", "success": False, "reason": "missing_api_key"}, severity="ERROR")
        return

    league_ids = get_league_ids("APIFOOTBALL_LEAGUE_IDS")
    if league_ids is None:
        gcp_utils.log_struct({"event": "no_league_provided", "source": "apifootball", "success": False, "reason": "invalid_league_ids_format"}, severity="ERROR")
        return
    
    teams_response = fetch_and_store(run_id, api_name, api_key, "get_teams", league_ids)
    if not teams_response['success']:
        # If failed not even try
        standings_response = {"success": False, "uploaded_files": []}
    else:
        standings_response = fetch_and_store(run_id, api_name, api_key, "get_standings", league_ids)

    # both calls was successful
    if teams_response['success'] and standings_response['success']:
        gcp_utils.publish_event(PROJECT_ID, PUBSUB_TOPIC_APIFOOTBALL, {
            "event": "ingest_success",
            "source": api_name,
            "uploaded_files": teams_response["uploaded_files"] + standings_response['uploaded_files'],
            "bucket": BUCKET_NAME,
            "run_id": run_id,
        })
    else:
        try: 
            gcp_utils.rollback_uploaded_files(api_name, run_id, teams_response["uploaded_files"] + standings_response['uploaded_files'])
        except Exception:
            # Rollback failed, cloud function should not trying to retry 
            return
        # Rollback succeeded, raise to trigger retry
        raise Exception("Rollback succeded. Retrying ingestion...")

# API-Sports ingestion function
def ingest_apisports(event, context):
    run_id = str(uuid.uuid4())
    api_name = "apisports"
    #TODO: fetch from args
    season = get_current_season()

    api_key = gcp_utils.get_secret(PROJECT_ID, "apisports-api-key")
    if not api_key:
        gcp_utils.log_struct({"event": "no_api_key_found", "source": "apisports", "success": False, "reason": "missing_api_key"}, severity="ERROR")
        return

    league_ids = get_league_ids("APISPORTS_LEAGUE_IDS")
    if league_ids is None:
        gcp_utils.log_struct({"event": "no_league_provided", "source": "apisports", "success": False, "reason": "invalid_league_ids_format"}, severity="ERROR")
        return
    
    teams_response = fetch_and_store(run_id, api_name, api_key, "teams", league_ids, season)
    if not teams_response['success']:
        # If failed not even try
        standings_response = {"success": False, "uploaded_files": []}
    else:
        standings_response = fetch_and_store(run_id, api_name, api_key, "standings", league_ids, season)

    # both calls was successful
    if teams_response['success'] and standings_response and standings_response['success']:
        gcp_utils.publish_event(PROJECT_ID, PUBSUB_TOPIC_APISPORTS, {
            "event": "ingest_success",
            "source": api_name,
            "uploaded_files": teams_response["uploaded_files"] + standings_response['uploaded_files'],
            "bucket": BUCKET_NAME,
            "run_id": run_id,
        })
    else:
        try: 
            gcp_utils.rollback_uploaded_files(api_name, run_id, teams_response["uploaded_files"] + standings_response['uploaded_files'])
        except Exception:
            # Rollback failed, cloud function should not trying to retry 
            return
        # Rollback succeeded, raise to trigger retry
        raise Exception("Rollback succeded. Retrying ingestion...")
        
if __name__ == "__main__":
    # For local testing purposes only
    ingest_apifootball({}, None)
