# Schema Definition

This document details the standardized schema used for the final BigQuery tables. The schema is designed to capture crucial information about each team's identity and its performance within a given season, normalizing data from both API-Sports and API-Football into a single, unified structure. 

The schema definitions is managed in the `unified_schemas/` directory.
## Schema Format and Management 

The official definition for the pipeline's schema is maintained in a JSON file that directly mirrors the format required by Google BigQuery. This approach makes the schema machine-readable and ensures that the "source of truth" is stored alongside the project's source code.
* **Format**: The schema is a JSON object containing a `version` key and a `fields` key. The `fields` key holds an array of objects, where each object defines a column with `name`, `type` (`STRING`, `INTEGER`, etc.), and `mode` (`REQUIRED` or `NULLABLE`) properties. 
* **Source of Truth**: The schema detailed below is a human-readable representation of the file located at **`ingestion/unified_schemas/v1.json`**. This JSON file is programmatically read by the Dataflow pipeline during execution to perform data validation and is used to define the BigQuery table structure.

## How to Evolve the Schema (e.g., Create `v2`) 
To introduce a new version of the schema, follow these steps: 
1. **Create a New JSON File**: In the `ingestion/unified_schemas/` directory, create a new file (e.g., `v2.json`). 
2. **Update the JSON Content**: Copy the content from `v1.json`, increment the `"version"` number to `2`, and add, remove, or modify the field definitions in the `fields` array as needed. 
3. **Update Configuration**: In the root `config.sh` file, change the `SCHEMA_JSON_FILE` variable to point to your new file. 
4. **Update Transformation Logic**: If you added new fields, you must update the transformation functions in `dataflow-flex/football_pipeline/transforms.py` (`transformApifootball` and `transformApisports`) to extract and populate these new fields from the source API data. 
5. **Re-run Setup and Deploy**: 
	* Upload the new schema file to GCS inside configured location (`SCHEMA_PATH`). 
	* Run `cd ingestion/ && bash deploy.sh` to update the secret that points the Dataflow job to the new schema path. 
This versioned approach ensures that schema changes are deliberate, tracked, and can be implemented without disrupting the existing pipeline.

## Standardized Schema: `v1`

| Field Name        | Data Type | Mode     | Description                                                                                         |
| ----------------- | --------- | -------- | --------------------------------------------------------------------------------------------------- |
| `pk`              | STRING    | REQUIRED | A unique primary key for the record, generated as `{season}-{league_id}-{team_id}`.                 |
| `team_id`         | STRING    | NULLABLE | The unique identifier for the team from the source API.                                             |
| `team_name`       | STRING    | NULLABLE | The official name of the football team.                                                             |
| `team_country`    | STRING    | NULLABLE | The country where the team is based.                                                                |
| `league_id`       | STRING    | NULLABLE | The unique identifier for the league from the source API.                                           |
| `league_name`     | STRING    | NULLABLE | The name of the league (e.g., "Premier League").                                                    |
| `season`          | INTEGER   | NULLABLE | The year the season took place (e.g., 2023).                                                        |
| `rank`            | INTEGER   | NULLABLE | The team's final position in the league table.                                                      |
| `points`          | INTEGER   | NULLABLE | The total points accumulated by the team in the season.                                             |
| `games_played`    | INTEGER   | NULLABLE | The total number of games played by the team.                                                       |
| `wins`            | INTEGER   | NULLABLE | The total number of games won.                                                                      |
| `draws`           | INTEGER   | NULLABLE | The total number of games drawn.                                                                    |
| `losses`          | INTEGER   | NULLABLE | The total number of games lost.                                                                     |
| `goals_for`       | INTEGER   | NULLABLE | The total number of goals scored by the team.                                                       |
| `goals_against`   | INTEGER   | NULLABLE | The total number of goals conceded by the team.                                                     |
| `goal_difference` | INTEGER   | NULLABLE | The difference between goals scored and goals conceded.                                             |
| `form`            | STRING    | NULLABLE | A string representing the team's recent form (e.g., "WWLDW").                                       |
| `venue_name`      | STRING    | NULLABLE | The name of the team's home stadium.                                                                |
| `venue_city`      | STRING    | NULLABLE | The city where the team's home stadium is located.                                                  |
| `update_datetime` | TIMESTAMP | NULLABLE | The timestamp when the record was processed and loaded. Defaults to the current time in BigQuery.   |
| `schema_version`  | STRING    | NULLABLE | The version of this schema (`v1`) used to process the record, aiding in data lineage and evolution. |

## Schema Design Decisions and Field Mapping

*   **Primary Key (`pk`)**: A composite key was created to uniquely identify a team's performance in a specific league and season. This is crucial for idempotency, ensuring that re-running the pipeline for the same data doesn't create duplicate records (due to the `WRITE_TRUNCATE` disposition).
*   **Data Provenance**: The data from API-Sports and API-Football are stored in separate tables (`teams_apisports` and `teams_apifootball`). This approach was chosen over merging the data to maintain a clear line of provenance and allow for easy comparison and validation between the two sources. Both tables, however, adhere to this same standardized schema.
*   **Conflict Resolution**: Since the data sources are not merged, there is no direct conflict resolution at the record level. Instead, the transformation logic within the Dataflow pipeline (`football_pipeline/transforms.py`) is tailored to each API, mapping its specific field names and data structures to the standard schema. For example, `transformApisports` and `transformApifootball` contain the distinct logic for parsing their respective source JSONs.
*   **Schema Enforcement**: The pipeline includes a strict schema enforcement step (`helpers.enforceSchemaGenerator`). Before a record is sent to BigQuery, this function:
    1.  **Validates Presence**: Checks that all `REQUIRED` fields are present.
    2.  **Casts Types**: Attempts to cast each value to its target data type (e.g., `str` to `int`).
    3.  **Strips Extra Fields**: Removes any fields from the source data that are not defined in the schema.
    This process guarantees that the data loaded into BigQuery is clean, consistent, and adheres to the defined structure. Any record that fails this validation is sent to the dead-letter queue for manual review.
*   **Extensibility (`schema_version`)**: Including a `schema_version` field makes the pipeline more robust for future changes. If a new version of the schema is introduced (e.g., `v2.json`), both new and old data can coexist in BigQuery, and consumers of the data can easily identify which schema version a particular record conforms to.