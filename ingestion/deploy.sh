#!/bin/bash

# source central config (repo root)
source "$(dirname "$0")/../config.sh"

echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${GS_BUCKET}"
echo "Scheduler Service Account: ${SA_SCHEDULER_EMAIL}"
echo "Inject Service Account: ${SA_INGEST_EMAIL}"

# --- Create/Update Dataflow Launch Parameters in Secret Manager ---
echo "Upserting Dataflow launch parameters into Secret Manager..."

# Construct the JSON payload using variables from config.sh
SECRET_JSON=$(cat <<EOF
{
  "project_id": "${PROJECT_ID}",
  "region": "${REGION}",
  "template_name": "football-unified",
  "template_path": "gs://${BUCKET_NAME}/dataflow/templates/football-unified-pipeline.json",
  "bq_dataset": "${BQ_DATASET}",
  "bq_table_prefix": "${BQ_TABLE_PREFIX}",
  "schema_path": "gs://${BUCKET_NAME}/dataflow/schemas/v1.json",
  "temp_location": "gs://${BUCKET_NAME}/dataflow/temp",
  "staging_location": "gs://${BUCKET_NAME}/dataflow/staging",
  "service_account_email": "${SA_DATAFLOW_EMAIL}"
}
EOF
)

echo "${SECRET_JSON}"

# Add the new configuration as the latest version of the secret
echo "${SECRET_JSON}" | gcloud secrets versions add "${DATAFLOW_SECRET_ID}" \
  --project="${PROJECT_ID}" \
  --data-file=- \
  --quiet

# Ensure required topics exist (idempotent)
gcloud pubsub topics create "${TRIGGER_TOPIC_APIFOOTBALL}" --project="${PROJECT_ID}" 2>/dev/null || true
gcloud pubsub topics create "${TRIGGER_TOPIC_APISPORTS}" --project="${PROJECT_ID}" 2>/dev/null || true

# Deploy ingest-apifootball Function
echo "Deploying ingest_apifootball function..."
gcloud functions deploy ingest_apifootball \
  --runtime "${FUNCTION_RUNTIME}" \
  --trigger-topic "${TRIGGER_TOPIC_APIFOOTBALL}" \
  --entry-point ingest_apifootball \
  --region "${REGION}" \
  --service-account "${SA_INGEST_EMAIL}" \
  --set-env-vars "GCP_PROJECT=${PROJECT_ID},BUCKET_NAME=${BUCKET_NAME},APIFOOTBALL_LEAGUE_IDS=${APIFOOTBALL_LEAGUE_IDS},DATAFLOW_SECRET_ID=${DATAFLOW_SECRET_ID}" \
  --source "$(dirname "$0")" \
  --allow-unauthenticated \
  --quiet

gcloud run services add-iam-policy-binding ingest-apifootball \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/run.invoker" \
  --region="${REGION}"

# Deploy ingest-apisports Function
echo "Deploying ingest_apisports function..."
gcloud functions deploy ingest_apisports \
  --runtime "${FUNCTION_RUNTIME}" \
  --trigger-topic "${TRIGGER_TOPIC_APISPORTS}" \
  --entry-point ingest_apisports \
  --region "${REGION}" \
  --service-account "${SA_INGEST_EMAIL}" \
  --set-env-vars "GCP_PROJECT=${PROJECT_ID},BUCKET_NAME=${BUCKET_NAME},APISPORTS_LEAGUE_IDS=${APISPORTS_LEAGUE_IDS},DATAFLOW_SECRET_ID=${DATAFLOW_SECRET_ID}" \
  --source "$(dirname "$0")" \
  --allow-unauthenticated \
  --quiet
  

gcloud run services add-iam-policy-binding ingest-apisports \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/run.invoker" \
  --region="${REGION}"

# --- Create Cloud Scheduler Cron Jobs ---
echo "Creating/Replacing Cloud Scheduler jobs..."

gcloud scheduler jobs delete ingest-apifootball-job --location="${REGION}" --quiet || true
gcloud scheduler jobs create pubsub ingest-apifootball-job \
  --schedule "${SCHEDULE_APIFOOTBALL}" \
  --topic "${TRIGGER_TOPIC_APIFOOTBALL}" \
  --message-body "Run" \
  --location "${REGION}"

gcloud scheduler jobs delete ingest-apisports-job --location="${REGION}" --quiet || true
gcloud scheduler jobs create pubsub ingest-apisports-job \
  --schedule "${SCHEDULE_APISPORTS}" \
  --topic "${TRIGGER_TOPIC_APISPORTS}" \
  --message-body "Run" \
  --location "${REGION}"

echo "Deployment complete."
