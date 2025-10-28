#!/bin/bash

# source central config (repo root)
source "$(dirname "$0")/../config.sh"

GS_BUCKET="gs://${BUCKET_NAME}"

echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${GS_BUCKET}"
echo "Scheduler Service Account: ${SA_SCHEDULER_EMAIL}"
echo "Inject Service Account: ${SA_INGEST_EMAIL}"

# Ensure required topics exist (idempotent)
gcloud pubsub topics create "${TRIGGER_TOPIC_APIFOOTBALL}" --project="${PROJECT_ID}" 2>/dev/null || true
gcloud pubsub topics create "${TRIGGER_TOPIC_APISPORTS}" --project="${PROJECT_ID}" 2>/dev/null || true
gcloud pubsub topics create "${SUCCESS_TOPIC_APIFOOTBALL}" --project="${PROJECT_ID}" 2>/dev/null || true
gcloud pubsub topics create "${SUCCESS_TOPIC_APISPORTS}" --project="${PROJECT_ID}" 2>/dev/null || true

# Deploy ingest-apifootball Function
echo "Deploying ingest_apifootball function..."
gcloud functions deploy ingest_apifootball \
  --runtime "${FUNCTION_RUNTIME}" \
  --trigger-topic "${TRIGGER_TOPIC_APIFOOTBALL}" \
  --entry-point ingest_apifootball \
  --region "${REGION}" \
  --service-account "${SA_INGEST_EMAIL}" \
  --set-env-vars "GCP_PROJECT=${PROJECT_ID},BUCKET_NAME=${BUCKET_NAME},PUBSUB_TOPIC_APIFOOTBALL=${SUCCESS_TOPIC_APIFOOTBALL},APIFOOTBALL_LEAGUE_IDS=${APIFOOTBALL_LEAGUE_IDS}" \
  --source "$(dirname "$0")" \
  --quiet

gcloud run services add-iam-policy-binding ingest-apifootball \
  --member="serviceAccount:${SA_SCHEDULER_EMAIL}" \
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
  --set-env-vars "GCP_PROJECT=${PROJECT_ID},BUCKET_NAME=${BUCKET_NAME},PUBSUB_TOPIC_APISPORTS=${SUCCESS_TOPIC_APISPORTS},APISPORTS_LEAGUE_IDS=${APISPORTS_LEAGUE_IDS}" \
  --source "$(dirname "$0")" \
  --quiet

gcloud run services add-iam-policy-binding ingest-apisports \
  --member="serviceAccount:${SA_SCHEDULER_EMAIL}" \
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
