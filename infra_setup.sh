#!/bin/bash
source ./config.sh


echo "Project: ${PROJECT_ID}"
echo "Region: ${REGION}"
echo "Bucket: ${BUCKET_NAME}"
echo "Scheduler Service Account: ${SA_SCHEDULER_EMAIL}"
echo "Ingestion Service Account: ${SA_INGEST_EMAIL}"
echo "Dataflow Service Account: ${SA_DATAFLOW_EMAIL}"

GS_BUCKET_NAME="gs://${BUCKET_NAME}"

echo "Enabling necessary GCP APIs..."
gcloud services enable \
  cloudfunctions.googleapis.com \
  eventarc.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  cloudscheduler.googleapis.com \
  storage-api.googleapis.com \
  pubsub.googleapis.com
  --project "${PROJECT_ID}"

# --- Create GCS Bucket ---
echo "--- Creating GCS bucket: ${BUCKET_NAME} ---"
gcloud storage buckets create ${GS_BUCKET_NAME} --location=${REGION}

# --- Create Pub/Sub Instance ---
echo "--- Creating Pub/Sub topics... ---"
gcloud pubsub topics create "${TRIGGER_TOPIC_APIFOOTBALL}" --project="${PROJECT_ID}" 2>/dev/null || true
gcloud pubsub topics create "${TRIGGER_TOPIC_APISPORTS}" --project="${PROJECT_ID}" 2>/dev/null || true
gcloud pubsub topics create "${SUCCESS_TOPIC_APIFOOTBALL}" --project="${PROJECT_ID}" 2>/dev/null || true
gcloud pubsub topics create "${SUCCESS_TOPIC_APISPORTS}" --project="${PROJECT_ID}" 2>/dev/null || true

# --- Create Secret in Secret Manager ---
gcloud secrets create apifootball-api-key --replication-policy="automatic" --quiet || echo "Secret apifootball-api-key already exists."
gcloud secrets create apisports-api-key --replication-policy="automatic" --quiet || echo "Secret apisports-api-key already exists."

source .env
if [ -f .env ]; then
  source .env
  echo "Storing API keys and league IDs in Secret Manager (new versions)..."
  echo -n "${APIFOOTBALL_KEY:-}" | gcloud secrets versions add apifootball-api-key --data-file=- 2>/dev/null || true
  echo -n "${APISPORTS_KEY:-}"   | gcloud secrets versions add apisports-api-key --data-file=- 2>/dev/null || true
  echo -n "${APIFOOTBALL_LEAGUE_IDS:-}" | gcloud secrets versions add apifootball-league-ids --data-file=- 2>/dev/null || true
  echo -n "${APISPORTS_LEAGUE_IDS:-}"   | gcloud secrets versions add apisports-league-ids --data-file=- 2>/dev/null || true
else
  echo ".env not found; skipping secret population step."
fi

# --- Create (or reuse) a dedicated service accounts ---
echo "--- Creating Service Accounts ---"
gcloud iam service-accounts create "${SA_SCHEDULER_NAME}" --display-name="SA for Pipeline Scheduler" --project="${PROJECT_ID}" 2>/dev/null || echo "Service Account ${SA_SCHEDULER_NAME} already exists."
gcloud iam service-accounts create "${SA_INGEST_NAME}" --display-name="SA for Ingestion Cloud Function" --project="${PROJECT_ID}" 2>/dev/null || echo "Service Account ${SA_INGEST_NAME} already exists."
gcloud iam service-accounts create "${SA_DATAFLOW_NAME}" --display-name="SA for Dataflow Processing Job" --project="${PROJECT_ID}" 2>/dev/null || echo "Service Account ${SA_DATAFLOW_EMAIL} already exists."

# --- Grant Fine-Grained IAM Permissions ---
echo "--- Granting IAM Permissions (Principle of Least Privilege) ---"
# SA_SCHEDULER
echo "Binding Scheduler SA (${SA_SCHEDULER_NAME})..."
gcloud pubsub topics add-iam-policy-binding "${TRIGGER_TOPIC_APIFOOTBALL}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_SCHEDULER_EMAIL}" \
  --role="roles/pubsub.publisher"

gcloud pubsub topics add-iam-policy-binding "${TRIGGER_TOPIC_APISPORTS}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_SCHEDULER_EMAIL}" \
  --role="roles/pubsub.publisher"

# SA_INGEST
gcloud secrets add-iam-policy-binding apifootball-api-key \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" || true

gcloud secrets add-iam-policy-binding apisports-api-key \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" || true

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/logging.logWriter"

gcloud storage buckets add-iam-policy-binding ${GS_BUCKET_NAME} \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/storage.objectUser"

gcloud pubsub topics add-iam-policy-binding ${SUCCESS_TOPIC_APIFOOTBALL} \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/pubsub.publisher"

gcloud pubsub topics add-iam-policy-binding ${SUCCESS_TOPIC_APISPORTS} \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/pubsub.publisher"

# SA_DATAFLOW_NAME
gcloud storage buckets add-iam-policy-binding ${GS_BUCKET_NAME} \
  --member="serviceAccount:${SA_DATAFLOW_EMAIL}" \
  --role="roles/storage.objectUser"

echo "Infrastructure setup complete."