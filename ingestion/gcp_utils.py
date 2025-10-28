import logging
import json
from google.cloud import storage
from google.cloud import pubsub_v1
from google.cloud import secretmanager


# try to initialize Cloud Logging client logger; fall back to stdlib
try:
    from google.cloud import logging as cloud_logging  # type: ignore
    _cloud_logging_client = cloud_logging.Client()
    CLOUD_LOGGER = _cloud_logging_client.logger("ingest")
except Exception:
    CLOUD_LOGGER = None

def log_struct(payload: dict, severity: str = "INFO"):
    """
    Emit a structured log. Uses Cloud Logging logger when available,
    otherwise emits a JSON string on the stdlib logger.
    """
    #TODO
    # if CLOUD_LOGGER:
    #     try:
    #         CLOUD_LOGGER.log_struct(payload, severity=severity)
    #         return
    #     except Exception:
    #         logging.warning("Cloud Logging emit failed; falling back to stdlib", exc_info=True)

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

def publish_event(project_id: str, topic_name: str, payload: dict):
    """Publish a JSON payload to the given Pub/Sub topic (projects/{PROJECT_ID}/topics/{topic})."""
    if not project_id or not topic_name:
        log_struct({"event": "publish_failed", "reason": "missing_project_or_topic", "topic": topic_name, "payload": payload}, severity="ERROR")
        return

    topic_path = f"projects/{project_id}/topics/{topic_name}"
    try:
        publisher = pubsub_v1.PublisherClient()
        data = json.dumps(payload).encode("utf-8")
        future = publisher.publish(topic_path, data)
        message_id = future.result(timeout=10)
        # log publish result as structured log (still a final-result style log)
        log_struct({"event": "published", "topic": topic_path, "message_id": message_id, "payload": payload}, severity="INFO")
    except Exception as e:
        log_struct({"event": "publish_failed", "topic": topic_path, "error": str(e), "exception_type": type(e).__name__, "payload": payload}, severity="ERROR")


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