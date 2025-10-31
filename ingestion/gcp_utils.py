import logging
import json
from datetime import datetime, timezone

from google.cloud import storage
from google.cloud import secretmanager
from googleapiclient.discovery import build

from . import config as cfg

def log_struct(payload: dict, severity: str = "INFO"):
    """
    Emit a structured log. Uses Cloud Logging logger when available,
    otherwise emits a JSON string on the stdlib logger.
    """
    text = json.dumps(payload, default=str)
    lvl = getattr(logging, severity.upper(), logging.INFO)
    logging.log(lvl, text)

def get_secret(project_id: str, secret_id: str, version_id: str = "latest") -> str:
    """Retrieves a secret from Google Secret Manager."""
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        return response.payload.data.decode("UTF-8")
    except Exception as e:
        logging.error(f"Failed to retrieve secret {secret_id}: {e}", exc_info=True)
        return None


def upload_to_gcs(bucket_name: str, destination_blob_name: str, data: str):
    """
    Uploads data to a GCS bucket.
    Returns:
        True if upload is successful, False otherwise.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type='application/json')
        # do not emit logs here — final structured log will be emitted by caller
        return
    except Exception as e:
        # do not emit logs here to avoid noisy logs; caller will detect Exception and include error context if needed
        raise e

def remove_from_gcs(bucket_name, destination_blob_names: list[str]) -> bool:
    """"
    Removes files from GCS bucket
    Returns:
        True if deletion is successfull, False otherwise. 
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = [bucket.blob(blob_name) for blob_name in destination_blob_names]
        bucket.delete_blobs(blobs)
        # do not emit logs here — final structured log will be emitted by caller
        return 
    except Exception as e:
        # do not emit logs here to avoid noisy logs; caller will detect Exception and include error context if needed
        raise e

def startDataflowPipeline(api_name, uploaded_files):
    try:
        params_json = get_secret(cfg.PROJECT_ID, cfg.DATAFLOW_SECRET_ID)
        if not params_json:
            raise ValueError(f"Could not retrieve Dataflow parameters from secret: {cfg.DATAFLOW_SECRET_ID}")
        params = json.loads(params_json)
        
        uploaded_gs_files = [ f'{cfg.GS_BUCKET}/{file}' for file in uploaded_files]

        service = build('dataflow', 'v1b3', cache_discovery=False)

        request = service.projects().locations().flexTemplates().launch(
            projectId=cfg.PROJECT_ID,
            location=cfg.REGION,
            # gcsPath=cfg.TEMPLATE_PATH,
            body={
                'launchParameter': {
                    'jobName': f"{params['template_name']}-{api_name}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
                    'parameters': {
                        'input_files': json.dumps(uploaded_gs_files),
                        'api_name': api_name,
                        'output_table': f"{params['project_id']}:{params['bq_dataset']}.{params['bq_table_prefix']}_{api_name}",
                        'schema_path': params['schema_path']
                    },
                    'environment': {
                        'tempLocation': params['temp_location'],
                        'stagingLocation': params['staging_location'],
                        'serviceAccountEmail': params['service_account_email'],
                    },
                    "containerSpecGcsPath":  params['template_path'],
                }
            },
        )
        response = request.execute()
        log_struct({
                "etl-stage": "injection",
                "event": "Dataflow pipeline execution requested", 
                "api-source": api_name, 
            }, severity="INFO")
    except Exception as e:
        logging.error(f"Failed to launch Dataflow job. Error: {e}")
        # Re-raise the exception so the caller can handle the failure.
        raise
