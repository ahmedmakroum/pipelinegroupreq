import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import re
from datetime import datetime

class ExtractSchoolAndBytes(beam.DoFn):
    def __init__(self):
        self.pattern = re.compile(r'/boti_bucket/assets/schools/([^/]+)/')

    def process(self, element):
        fields = element.split(',')
        if len(fields) > 8:
            cs_uri = fields[5]
            sc_bytes = fields[8]
            time_micros = fields[0]
            match = self.pattern.search(cs_uri)
            if match:
                school = match.group(1)
                timestamp = int(time_micros) / 1e6
                date = datetime.utcfromtimestamp(timestamp)
                month = date.strftime('%Y-%m')
                yield {
                    'school': school,
                    'month': month,
                    'sc_bytes': int(sc_bytes)
                }

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'arcane-boulder-429415-s1'
    google_cloud_options.job_name = 'parsegroupreq'
    google_cloud_options.staging_location = 'gs://bucket1/staging'
    google_cloud_options.temp_location = 'gs://bucket1/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    input_file = 'gs://bucket1/bucket.csv'
    output_table = 'arcane-boulder-429415-s1:test.testyour_table'

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
         | 'Extract School and Bytes' >> beam.ParDo(ExtractSchoolAndBytes())
         | 'Aggregate Data' >> beam.CombinePerKey(lambda elements: {
                'request_count': len(elements),
                'total_bytes': sum(e['sc_bytes'] for e in elements)
            })
         | 'Format Results' >> beam.Map(lambda kv: {
                'school': kv[0][0],
                'month': kv[0][1],
                'request_count': kv[1]['request_count'],
                'total_bytes': kv[1]['total_bytes']
            })
         | 'Write to BigQuery' >> beam.io.WriteToBigQuery(
                output_table,
                schema='school:STRING, month:STRING, request_count:INTEGER, total_bytes:INTEGER',
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            ))

if __name__ == '__main__':
    run()
