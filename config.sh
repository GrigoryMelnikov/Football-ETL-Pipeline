#!/bin/bash
# Centralized configuration for infra & deploy scripts.
# Keep secrets in Secret Manager (.env should only be used locally to populate secrets).

# Project / region
export PROJECT_ID=$(gcloud config get-value project)
export REGION="us-central1" # Cheapest region as of jul-25

# Storage
export BUCKET_NAME="${PROJECT_ID}-football-stage"

# Dedicated service account for Cloud Scheduler job
export SA_SCHEDULER_NAME="sa-pipeline-scheduler"
export SA_SCHEDULER_EMAIL="${SA_SCHEDULER_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
# Dedicated service account for Ingestion
export SA_INGEST_NAME="sa-ingest-function"
export SA_INGEST_EMAIL="${SA_INGEST_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
# Dedicated service account for Dataflow
export SA_DATAFLOW_NAME="sa-dataflow-job"
export SA_DATAFLOW_EMAIL="${SA_DATAFLOW_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# Pub/Sub topics
export TRIGGER_TOPIC_APIFOOTBALL="ingest_apifootball_trigger"
export TRIGGER_TOPIC_APISPORTS="ingest_apisports_trigger"
export SUCCESS_TOPIC_APIFOOTBALL="ingest_apifootball_success"
export SUCCESS_TOPIC_APISPORTS="ingest_apisports_success"

# Cloud Function runtime and schedules
export FUNCTION_RUNTIME="python312"
export SCHEDULE_APIFOOTBALL="0 1 * * *"
export SCHEDULE_APISPORTS="0 1 * * *"

# Cloud Function arguments 
export APISPORTS_LEAGUE_IDS='[40]'
export APIFOOTBALL_LEAGUE_IDS='[153]'