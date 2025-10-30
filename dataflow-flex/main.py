import argparse
import logging
from football_pipeline import pipeline
from apache_beam.options.pipeline_options import PipelineOptions

def run():
    """Parses command-line arguments and runs the pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_files",
        type=str,
        help="Serialized List of GCS paths for the input JSON files.",
        required=True,
    )
    parser.add_argument(
        "--api_name",
        type=str,
        help="Name of the API (e.g., 'apifootball', 'apisports').",
        required=True,
    )
    parser.add_argument(
        "--output_table",
        type=str,
        help="GCS path prefix for the output file.",
        required=True,
    )
    parser.add_argument(
        "--schema_path",
        type=str,
        help="The path to the JSON schema file",
        required=True,
    )
    
    custom_args, pipeline_args = parser.parse_known_args()

    logging.info("Starting pipeline with the following options:")
    logging.info(f"API Name: {custom_args.api_name}")
    logging.info(f"Input Files: {custom_args.input_files}")
    logging.info(f"Output File: {custom_args.output_table}")
    logging.info(f"Schema Path: {custom_args.schema_path}")
    
    pipeline_options = PipelineOptions(pipeline_args, streaming=False)
    pipeline.run_pipeline(
        pipeline_options,
        custom_args.input_files,
        custom_args.api_name,
        custom_args.output_table,
        custom_args.schema_path
    )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()