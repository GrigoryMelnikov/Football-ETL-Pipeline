# Centralized Python configuration (reads from environment, safe defaults provided).
import os

PROJECT_ID = os.environ.get("GCP_PROJECT")
REGION = os.environ.get("GCP_REGION", "us-central1")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

PUBSUB_TOPIC_APIFOOTBALL = os.environ.get("PUBSUB_TOPIC_APIFOOTBALL", "ingest-apifootball-success")
PUBSUB_TOPIC_APISPORTS = os.environ.get("PUBSUB_TOPIC_APISPORTS", "ingest-apisports-success")

TRIGGER_TOPIC_APIFOOTBALL = os.environ.get("TRIGGER_TOPIC_APIFOOTBALL", "ingest_apifootball_trigger")
TRIGGER_TOPIC_APISPORTS = os.environ.get("TRIGGER_TOPIC_APISPORTS", "ingest_apisports_trigger")

FUNCTION_RUNTIME = os.environ.get("FUNCTION_RUNTIME", "python39")
SCHEDULE_APIFOOTBALL = os.environ.get("SCHEDULE_APIFOOTBALL", "0 1 * * *")
SCHEDULE_APISPORTS = os.environ.get("SCHEDULE_APISPORTS", "0 1 * * *")

# League IDs (JSON string)
APIFOOTBALL_LEAGUE_IDS = os.environ.get("APIFOOTBALL_LEAGUE_IDS", [153])   
APISPORTS_LEAGUE_IDS = os.environ.get("APISPORTS_LEAGUE_IDS", [40]) 