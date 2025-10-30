import logging
import json
from google.cloud import storage
from apache_beam.io.gcp.internal.clients import bigquery
from typing import Dict, Union, Any


def read_gcs_file(file_path) -> Union[Dict, None]:
    """Reads the content of a file from GCS, returning None on failure."""
    try:

        parts = file_path.replace('gs://', '').split('/', 1)
        bucket_name = parts[0]
        file_name = parts[1] 

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_string()
        logging.debug(f"Successfully read GCS file: gs://{file_path}")
        return json.loads(content)
    except Exception as e:
        # Log error but don't crash. The caller will handle the None return.
        logging.error(f"Failed to read or parse GCS file gs://{file_path}: {e}")
        return None

def create_bq_table_schema(raw_schema: Dict[str, Any]) -> bigquery.TableSchema:
    """
    Converts a raw schema dictionary (from a JSON file) into a BigQuery TableSchema object.

    Args:
        raw_schema: The dictionary loaded from the schema JSON file.

    Returns:
        An apache_beam.io.gcp.internal.clients.bigquery.TableSchema object.

    Raises:
        KeyError: If the raw_schema is missing the 'fields' key.
        TypeError: If 'fields' is not a list.
    """
    if 'fields' not in raw_schema:
        raise KeyError("Schema dictionary must contain a 'fields' key.")
    if not isinstance(raw_schema['fields'], list):
        raise TypeError("The 'fields' key in the schema must contain a list of field definitions.")

    table_schema = bigquery.TableSchema()
    for field in raw_schema.get("fields", []):
        field_schema = bigquery.TableFieldSchema()
        field_schema.name = field.get("name")
        field_schema.type = field.get("type", "STRING")
        field_schema.mode = field.get("mode", "NULLABLE")  # Default to NULLABLE for BQ safety
        table_schema.fields.append(field_schema)

    logging.info("Successfully created BigQuery TableSchema object.")
    return table_schema

def log_struct(payload: dict, severity: str = "INFO") -> None:
    """Logs a structured log message."""
    logger = logging.getLogger()
    log_method = getattr(logger, severity.lower(), logger.info)
    log_method(json.dumps(payload))



