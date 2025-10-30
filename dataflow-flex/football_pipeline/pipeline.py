import apache_beam as beam
from apache_beam.options.pipeline_options import GoogleCloudOptions
# from apache_beam.io import WriteToBigQuery
import json
import logging

from football_pipeline.transforms import ProcessFilesDoFn
from football_pipeline.utils import gcp
from football_pipeline.utils import helpers

def run_pipeline(options, input_files, api_name, output_table, schema_path):
    """Defines and runs the Beam pipeline."""
    # Deserialize input files
    input_files = json.loads(input_files)
    # Download schema
    raw_schema = gcp.read_gcs_file(schema_path)
    schema_version, schema_fields = helpers.parseSchema(raw_schema)

    # Automatically determine the dead-letter path using the job's temp_location.
    # This is a required option when running on Dataflow.
    try:
        gcp_options = options.view_as(GoogleCloudOptions)
        temp_location = gcp_options.temp_location
        if not temp_location:
            raise ValueError("Pipeline requires --temp_location to be set for automatic dead-letter path.")
        dead_letter_path = f'{temp_location}/dead_letter/'
        logging.info(f"Dead-letter files will be written to: {dead_letter_path}")
    except (ValueError, AttributeError) as e:
        logging.error(f"Could not determine dead-letter path from pipeline options: {e}")
        # Fallback for local runs
        dead_letter_path = "/tmp/dead_letter/"
        logging.warning(f"Using local fallback dead-letter path: {dead_letter_path}")

    with beam.Pipeline(options=options) as p:
        processed_results = (
            p
            | "Create PCollection from input files" >> beam.Create(input_files)
            | "Extract (season-league, path) tuples" >> beam.Map(helpers.extractPk)
            | "Group file paths by season-league" >> beam.GroupByKey()
            | "Process and combine season-league files" 
                >> beam.ParDo(
                    ProcessFilesDoFn(api_name=api_name, schema_version=schema_version, schema_fields=schema_fields)
                ).with_outputs(ProcessFilesDoFn.DEAD_LETTER_TAG, main='main')
            )
        # Main output stream for successfully processed data, writing to BigQuery
        main_output = processed_results.main
        (
            main_output
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=output_table,
                schema=helpers.bqSchemaFromJson(raw_schema),
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )
        # Dead-letter output stream for failed data
        dead_letter_output = getattr(processed_results, ProcessFilesDoFn.DEAD_LETTER_TAG)
        (
            dead_letter_output
            | "Serialize Dead-Letter elements" >> beam.Map(json.dumps)
            | "Write Dead-Letter output to GCS"
                >> beam.io.WriteToText(dead_letter_path, file_name_suffix=".json", num_shards=1)
        )
