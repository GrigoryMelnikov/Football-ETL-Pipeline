import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.pvalue import TaggedOutput

import json
import logging
from typing import Tuple, Iterable
from football_pipeline.utils import helpers


# --- Dedicated API-specific transformation functions ---
"""
def transformApi(teams_data, standings_data, schema, pcol_pk) -> Iterable[dict]:
    schema_version, schema_fields = helpers.parseSchema(schema)

    yield from helpers.enforceSchemaGenerator(transformed_records, schema_fields)
"""

def transformApifootball(teams_data: dict, standings_data: dict, schema: Tuple, pcol_pk: str) -> Iterable[dict]:
    """
    Joins teams with standings and transforms to unified schema for API-Football.
    TODO: Implement the join and transformation logic here.
    """
    logging.debug(f"Running transformApifootball for {len(teams_data)} teams and {len(standings_data)} standings records.")

    if not standings_data or not teams_data:
        raise ValueError("APIFootball input data is missing for standings or teams.")
    
    schema_version, schema_fields = schema

    # Create a lookup dictionary for teams for efficient joining
    teams_dict = {team['team_key']: team for team in teams_data}

    transformed_records = []
    for standing in standings_data:
        team_id = standing.get('team_id')
        team_info = teams_dict.get(team_id, {})
        venue_info = team_info.get('venue', {})

        # Safely get goal stats, defaulting to 0 if missing
        goals_for = int(standing.get('overall_league_GF', 0))
        goals_against = int(standing.get('overall_league_GA', 0))

        if not team_info:
            logging.warning(f"No team information found for team_id: {team_id}. Skipping record.")
            continue

        record = {
            "pk": f'{pcol_pk}-{team_id}',
            "team_id": team_id,
            "team_name": standing.get('team_name'),
            "team_country": team_info.get('team_country'),
            "league_id": standing.get('league_id'),
            "league_name": standing.get('league_name'),
            "season": pcol_pk.split('-')[0],
            "rank": standing.get('overall_league_position'),
            "points": standing.get('overall_league_PTS'),
            "games_played": standing.get('overall_league_payed'),
            "wins": standing.get('overall_league_W'),
            "draws": standing.get('overall_league_D'),
            "losses": standing.get('overall_league_L'),
            "goals_for": goals_for,
            "goals_against": goals_against,
            "goal_difference": goals_for - goals_against,
            "form": standing.get('overall_league_form'),
            "venue_name": venue_info.get('venue_name'),
            "venue_city": venue_info.get('venue_city'),
            "schema_version": schema_version,
        }
        transformed_records.append(record)
    
    yield from helpers.enforceSchemaGenerator(transformed_records, schema_fields)

def transformApisports(teams_data: dict, standings_data: dict, schema: Tuple, pcol_pk: str) -> Iterable[dict]:
    teams_response = teams_data.get('response', [])
    standings_response = standings_data.get('response', [])

    if not standings_response or not teams_response:
        raise ValueError("API-Sports 'response' data is empty or missing for standings or teams.")

    schema_version, schema_fields = schema
    
    try:
        league_info = standings_response[0]['league']
        standings_list = league_info['standings'][0]
    except (IndexError, KeyError) as e:
        raise ValueError(f"Could not find standings list in API-Sports data structure for PK {pcol_pk}: {e}")

    teams_dict = {team['team']['id']: team for team in teams_response}
    transformed_records = []

    for standing in standings_list:
        team_id = standing['team']['id']
        team_info_wrapper = teams_dict.get(team_id, {})
        team_info = team_info_wrapper.get('team', {})
        venue_info = team_info_wrapper.get('venue', {})

        if not team_info:
            logging.warning(f"No team information found for team_id: {team_id} in PK {pcol_pk}. Skipping record.")
            continue
        
        record = {
            "pk": f'{pcol_pk}-{team_id}',
            "team_id": team_id,
            "team_name": team_info.get('name'),
            "team_country": team_info.get('country'),
            "league_id": league_info.get('id'),
            "league_name": league_info.get('name'),
            "season": league_info.get('season'),
            "rank": standing.get('rank'),
            "points": standing.get('points'),
            "games_played": standing.get('all', {}).get('played'),
            "wins": standing.get('all', {}).get('win'),
            "draws": standing.get('all', {}).get('draw'),
            "losses": standing.get('all', {}).get('lose'),
            "goals_for": standing.get('all', {}).get('goals', {}).get('for'),
            "goals_against": standing.get('all', {}).get('goals', {}).get('against'),
            "goal_difference": standing.get('goalsDiff'),
            "form": standing.get('form'),
            "venue_name": venue_info.get('name'),
            "venue_city": venue_info.get('city'),
            "schema_version": schema_version,
        }
        transformed_records.append(record)

    yield from helpers.enforceSchemaGenerator(transformed_records, schema_fields)


TRANSFORM_MAP = {
    'apifootball': transformApifootball,
    'apisports': transformApisports,
}

# --- Generic PTransform for processing events ---
class ProcessFilesDoFn(beam.DoFn):
    """
    A Beam DoFn to read, process, and combine data for a single season-league.
    """

    DEAD_LETTER_TAG = 'dead_letter'

    def __init__(self, api_name: str, schema_version: str, schema_fields: dict):
        self.api_name = api_name
        self.schema_version = schema_version
        self.schema_fields = schema_fields
        if api_name not in TRANSFORM_MAP:
            raise ValueError(f"Unsupported API name: {api_name}")

    def process(self, element: Tuple[str, Iterable[str]]) -> Iterable[dict]:
        try:
            pk, file_paths = element
            logging.info(f'Processing files for PK: {pk}')

            teams_data = {}
            standings_data = {}

            #1. Read and parse all files for this season-league
            for path in file_paths:
                try:
                    with FileSystems.open(path, "r") as f:
                        content = json.load(f)
                        # Simplified logic to determine file type
                        if "teams/" in path:
                            teams_data = content
                        elif "standings/" in path:
                            standings_data = content
                except Exception as e:
                    logging.error("Failed to read or parse file %s: %s", path, e)
                    raise Exception(f"Dead letter file: {path}, error: {e}") from e
                
            # 2. Apply logic based on the API name
            transform_fn = TRANSFORM_MAP[self.api_name]
            processed = transform_fn(
                teams_data=teams_data,
                standings_data=standings_data,
                schema=(self.schema_version, self.schema_fields),
                pcol_pk=pk
            )

            # 3. Yield processed records (line-by-line)
            yield from processed
        except Exception as e:
            logging.error(f"Error processing PK {pk}. Sending to dead-letter. Error: {e}", exc_info=True)
            yield TaggedOutput(self.DEAD_LETTER_TAG, {"PK": pk, "files": list(file_paths), "error": str(e)})
