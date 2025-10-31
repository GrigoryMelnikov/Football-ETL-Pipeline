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
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  datastore.googleapis.com \
  datapipelines.googleapis.com \
  cloudresourcemanager.googleapis.com \
  --project "${PROJECT_ID}"

# --- Create GCS Bucket ---
echo "--- Creating GCS bucket: ${BUCKET_NAME} ---"
gcloud storage buckets create ${GS_BUCKET_NAME} --location=${REGION}

# --- Create Pub/Sub Instance ---
echo "--- Creating Pub/Sub topics... ---"
gcloud pubsub topics create "${TRIGGER_TOPIC_APIFOOTBALL}" --project="${PROJECT_ID}" 2>/dev/null || true
gcloud pubsub topics create "${TRIGGER_TOPIC_APISPORTS}" --project="${PROJECT_ID}" 2>/dev/null || true

# --- Create Secret in Secret Manager ---
gcloud secrets create apifootball-api-key --replication-policy="automatic" --quiet || echo "Secret apifootball-api-key already exists."
gcloud secrets create apisports-api-key --replication-policy="automatic" --quiet || echo "Secret apisports-api-key already exists."
gcloud secrets create "${DATAFLOW_SECRET_ID}" --replication-policy="automatic" --quiet|| echo "Secret ${DATAFLOW_SECRET_ID} already exists."

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

# --- Create BigQuery Dataset and Table ---
echo "Creating BigQuery Dataset and Table..."
bq --location=${REGION} mk --dataset ${PROJECT_ID}:${BQ_DATASET}

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
  --role="roles/secretmanager.secretAccessor" 

gcloud secrets add-iam-policy-binding apisports-api-key \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" 

gcloud secrets add-iam-policy-binding ${DATAFLOW_SECRET_ID} \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/secretmanager.secretAccessor" 

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/logging.logWriter"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/dataflow.developer"

gcloud storage buckets add-iam-policy-binding ${GS_BUCKET_NAME} \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/storage.objectUser"

gcloud iam service-accounts add-iam-policy-binding "${SA_DATAFLOW_EMAIL}" \
  --project="${PROJECT_ID}" \
  --member="serviceAccount:${SA_INGEST_EMAIL}" \
  --role="roles/iam.serviceAccountUser" 

# SA_DATAFLOW_NAME
echo "Granting permissions to Dataflow SA..."
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
SA_CLOUD_BUILD="${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_DATAFLOW_EMAIL}" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:${SA_DATAFLOW_EMAIL}" \
    --role="roles/artifactregistry.reader"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_DATAFLOW_EMAIL}" \
  --role="roles/pubsub.subscriber"

gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${SA_DATAFLOW_EMAIL}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding ${PROJECT_ID} \
  --member="serviceAccount:${SA_DATAFLOW_EMAIL}" \
  --role="roles/bigquery.jobUser"

echo "Granting Cloud Build permissions to launch the job..."
# Allows Cloud Build to act as the Dataflow worker SA during job launch
gcloud iam service-accounts add-iam-policy-binding "${SA_DATAFLOW_EMAIL}" \
  --member="serviceAccount:${SA_CLOUD_BUILD}" \
  --role="roles/iam.serviceAccountUser" \
  --project="${PROJECT_ID}"

echo "Building Dataflow Flex Template..."
gcloud storage cp "unified_schemas/${SCHEMA_JSON_FILE}.json" "${SCHEMA_PATH}" --project="${PROJECT_ID}"

cd ./dataflow-flex
gcloud dataflow flex-template build "$TEMPLATE_PATH" \
  --image-gcr-path "${TEMPLATE_IMAGE}" \
  --sdk-language "PYTHON" \
  --flex-template-base-image "PYTHON3" \
  --metadata-file "metadata.json" \
  --py-path "./" \
  --temp-location "${DATAFLOW_TEMP_LOCATION}" \
  --staging-location "${DATAFLOW_STAGING_LOCATION}" \
  --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt" \
  --env "FLEX_TEMPLATE_PYTHON_SETUP_FILE=setup.py" \
  --env "FLEX_TEMPLATE_PYTHON_PY_FILE=main.py" \
  --project "$PROJECT_ID"
cd ..

echo "Infrastructure setup complete."