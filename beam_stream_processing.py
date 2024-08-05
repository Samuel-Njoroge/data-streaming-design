import argparse
import json, ast
import logging
import time
from apache_beam.io.gcp.bigquery_tools import RetryStrategy
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import bigquery

# Define the BigQuery schema for Output table
schema =','.join([
        'Customer_ID:INTEGER',
        'Customer_Name:STRING',
        'Transaction_Date:DATE',
        'Email_Address:STRING',
        'Phone_Number:STRING',
        'Amount:FLOAT64',
        'Product:STRING'
])
ERROR_SCHEMA = ','.join([
    'error:STRING',
])


class ParseMessage(beam.DoFn):
    OUTPUT_ERROR_TAG = 'error'
    def process(self, line):
        try:
            parsed_row = ast.literal_eval(line)
            logging.info("Running")
            yield parsed_row

        except Exception as error:
            logging.info("error")
            error_row = {'error':str(error)}
            yield beam.pvalue.TaggedOutput(self.OUTPUT_ERROR_TAG, error_row)

# Setting the streaming pipeline  (streaming=True)
def run(args, input_subscription, output_table, output_error_table):
    options = PipelineOptions(args, save_main_section=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        # Read the message from Pub/Sub & Process
        rows, error_rows = (
            pipeline
            | 'Read from Pub/Sub' >> beam.io.ReadFromPubSub(
                subscription=input_subscription).with_output_types(bytes)
            | 'UTF-8 bytes to string' >> beam.Map(lambda msg: msg.decode('utf-8'))
            | 'Parse JSON messages' >> beam.ParDo(ParseMessage()).with_outputs(ParseMessage.OUTPUT_ERROR_TAG,
                                                                               main='rows')
        )

        # Output the results into BigQuery Table
        _ = (rows | 'Write to BigQuery'
             >> beam.io.WriteToBigQuery(output_table,
                                        schema=schema,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
            )
        
        _ = (error_rows | 'Write errors to BigQuery'
             >> beam.io.WriteToBigQuery(output_error_table,
                                        schema=ERROR_SCHEMA,
                                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                                        insert_retry_strategy=RetryStrategy.RETRY_ON_TRANSIENT_ERROR
                                        )
            )
        
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input_subscription', required=True,
        help='Input Pub/Sub subscription of the form "/subscriptions/<PROJECT>/<SUBSCRIPTION>".')
    parser.add_argument(
        '--output_table', required=True,
        help='Output BigQuery table for results specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
    parser.add_argument(
        '--output_error_table', required=True,
        help='Output BigQuery table for errors specified as: PROJECT:DATASET.TABLE or DATASET.TABLE.')
    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args, known_args.input_subscription, known_args.output_table, known_args.output_error_table)
    