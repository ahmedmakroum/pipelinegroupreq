import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.transforms import PTransform

class ParseCSV(beam.DoFn):
    def process(self, element):
        import csv
        from io import StringIO

        # Create a CSV reader
        reader = csv.DictReader(StringIO(element))
        
        for row in reader:
            try:
                # Parse fields and convert to appropriate types
                parsed_row = {
                    'time_micros': int(row['time_micros'].strip('"')),
                    'c_ip': row['c_ip'].strip('"'),
                    'c_ip_type': int(row['c_ip_type'].strip('"')),
                    'c_ip_region': row['c_ip_region'].strip('"'),
                    'cs_method': row['cs_method'].strip('"'),
                    'cs_uri': row['cs_uri'].strip('"'),
                    'sc_status': int(row['sc_status'].strip('"')),
                    'cs_bytes': int(row['cs_bytes'].strip('"')),
                    'sc_bytes': int(row['sc_bytes'].strip('"')),
                    'time_taken_micros': int(row['time_taken_micros'].strip('"')),
                    'cs_host': row['cs_host'].strip('"'),
                    'cs_referer': row['cs_referer'].strip('"'),
                    'cs_user_agent': row['cs_user_agent'].strip('"'),
                    's_request_id': row['s_request_id'].strip('"'),
                    'cs_operation': row['cs_operation'].strip('"'),
                    'cs_bucket': row['cs_bucket'].strip('"'),
                    'cs_object': row['cs_object'].strip('"'),
                }
                yield parsed_row
            except ValueError as e:
                # Log and skip rows with parsing errors
                print(f"Skipping row due to error: {e}")

class ConvertToMonthlyMetrics(beam.DoFn):
    def process(self, element):
        from datetime import datetime
        
        # Convert time_micros to seconds and format to month
        timestamp = element['time_micros'] / 1e6
        month = datetime.fromtimestamp(timestamp).strftime('%Y-%m')
        api_call = element['cs_uri'].startswith('/api/')
        
        # Prepare output format
        yield {
            'month': month,
            'api_call': api_call,
            'cs_bytes': element['cs_bytes'],
            'sc_bytes': element['sc_bytes'],
        }

class AggregateMonthlyMetrics(beam.PTransform):
    def expand(self, pcoll):
        return (
            pcoll
            | 'GroupByMonth' >> beam.GroupByKey()
            | 'ComputeMonthlyMetrics' >> beam.Map(lambda kv: {
                'month': kv[0],
                'api_calls': sum(1 for x in kv[1] if x['api_call']),
                'total_cs_bytes': sum(x['cs_bytes'] for x in kv[1]),
                'total_sc_bytes': sum(x['sc_bytes'] for x in kv[1]),
            })
        )

def run(argv=None):
    # Define pipeline options
    pipeline_options = PipelineOptions(argv)
    
    # Create the pipeline
    p = beam.Pipeline(options=pipeline_options)
    
    # Define the Dataflow pipeline
    (
        p
        | 'Read CSV File' >> ReadFromText('gs://bucketboti1/bucket1.csv')
        | 'Parse CSV' >> beam.ParDo(ParseCSV())
        | 'Convert to Monthly Metrics' >> beam.ParDo(ConvertToMonthlyMetrics())
        | 'Aggregate Monthly Metrics' >> AggregateMonthlyMetrics()
        | 'Write to BigQuery' >> WriteToBigQuery(
            table='arcane-boulder-429415-s1:test.test',
            schema='month:STRING,api_calls:INTEGER,total_cs_bytes:INTEGER,total_sc_bytes:INTEGER',
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        )
    )
    
    # Run the pipeline and wait for it to finish
    result = p.run()
    result.wait_until_finish()

if __name__ == '__main__':
    run()




#Access Google Data Studio: Go to Google Data Studio.