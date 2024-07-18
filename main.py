import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
import re

class ExtractSchoolAndBytes(beam.DoFn):
    def __init__(self):
        self.pattern = re.compile(r'/boti_bucket/assets/schools/([^/]+)/')

    def process(self, element):
        fields = element.split(',')
        if len(fields) > 8:
            cs_uri = fields[5]
            sc_bytes = fields[8]
            match = self.pattern.search(cs_uri)
            if match:
                school = match.group(1)
                yield (school, int(sc_bytes))

def format_result(element):
    school, total_bytes = element
    return f"{school},{total_bytes}"

def run(argv=None):
    pipeline_options = PipelineOptions(argv)
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'arcane-boulder-429415-s1'
    google_cloud_options.job_name = 'parsegroupreq'
    google_cloud_options.staging_location = 'gs://bucket1/staging'
    google_cloud_options.temp_location = 'gs://bucket1/temp'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    input_file = 'gs://bucket1/input-data.csv'
    output_file = 'gs://test/output-data/result'

    with beam.Pipeline(options=pipeline_options) as p:
        (p
         | 'Read CSV' >> beam.io.ReadFromText(input_file, skip_header_lines=1)
         | 'Extract School and Bytes' >> beam.ParDo(ExtractSchoolAndBytes())
         | 'Sum Bytes Per School' >> beam.CombinePerKey(sum)
         | 'Format Results' >> beam.Map(format_result)
         | 'Write Results' >> beam.io.WriteToText(output_file, file_name_suffix='.csv', shard_name_template=''))

if __name__ == '__main__':
    run()
