import logging 
import json
import re
from apache_beam.io.gcp.internal.clients import bigquery

from typing import Tuple, Iterable, Dict, Any

def extractPk(file_path: str) -> Tuple[str, str]:
    """
    Extracts the league ID from a GCS file path using a regular expression.

    Args:
        file_path: The full GCS path to an input file.

    Returns:
        A tuple containing the extracted league ID and the original file path.
        Returns ('unknown', file_path) if the pattern is not found.
    """
    # Regex to capture the 'league_{league_id}' part of the path
    match = re.search(r"/season_(\d+)/league_(\d+)/", file_path)
    if not match:
        logging.warning(f'Could not extract season-league PK from path: {file_path}')
        return "unknown", file_path
    season = match.group(1)
    league_id = match.group(2)
    pk = f'{season}-{league_id}'
    return pk, file_path


def parseSchema(schema_data: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
    """
    Parses a raw schema dictionary into a map for validation.

    Raises:
        ValueError: If a schema structure is invalid.
    """
    try:
        return (
            schema_data["version"],
            {
                field["name"]: {
                    "type": field["type"],
                    "mode": field.get("mode", "REQUIRED")
                }
            for field in schema_data.get("fields", [])
            }
        )
    except KeyError as e:
        raise KeyError("Schema structure is invalid: Missing key. {e}")

def enforceSchemaGenerator(records: Iterable[Dict[str, Any]], schema_map: Dict[str, Dict[str, str]]) -> Iterable[Dict[str, Any]]:
    """
    Ensures each record conforms to the provided schema map.

    - Validates that all required fields are present.
    - Validates that the data type of each field can be cast to the target type.
    - Removes any fields not present in the schema.

    Yields:
        A dictionary for each record that strictly conforms to the schema.

    Raises:
        ValueError: If a required field is missing.
        TypeError: If a field's value cannot be cast to its target type.
    """
    TYPE_CAST_MAP = {
        'STRING': str,
        'INTEGER': int,
        'TIMESTAMP': str  # Pass as string; BigQuery can auto-parse ISO 8601 format
    }

    for record in records:
        unified_record = {}
        for field_name, properties in schema_map.items():
            target_type = properties['type']
            mode = properties['mode']

            # 1. Check for missing required fields
            if field_name not in record or record.get(field_name) is None:
                if mode == 'NULLABLE':
                    unified_record[field_name] = None
                    continue
                else:
                    raise ValueError(f"Missing required field: '{field_name}' in record.")

            value = record[field_name]

            # 2. Validate and cast data type
            if target_type not in TYPE_CAST_MAP:
                raise TypeError(f"Unsupported schema type '{target_type}' for field '{field_name}'")

            cast_func = TYPE_CAST_MAP[target_type]
            try:
                unified_record[field_name] = cast_func(value)
            except (ValueError, TypeError):
                raise TypeError(
                    f"Type validation failed for field '{field_name}'. "
                    f"Could not cast value '{value}' (type: {type(value).__name__}) "
                    f"to target type '{target_type}'."
                )
        yield unified_record


def bqSchemaFromJson(schema_json: Dict[str, Any]) -> bigquery.TableSchema:
    """Loads a BigQuery schema from a JSON file, raising exceptions on failure."""
    try:
        table_schema = bigquery.TableSchema()
        for field in schema_json.get("fields", []):
            field_schema = bigquery.TableFieldSchema()
            field_schema.name = field.get("name")
            field_schema.type = field.get("type", "STRING")
            field_schema.mode = field.get("mode", "NULLABLE")
            table_schema.fields.append(field_schema)
        logging.debug(f"Successfully loaded BigQuery schema")
        return table_schema
    except Exception as e:
        logging.error(f"An unexpected error occurred while loading schema: {e}")
        raise