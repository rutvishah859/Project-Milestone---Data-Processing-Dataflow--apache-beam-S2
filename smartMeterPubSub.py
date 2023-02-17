#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A Smart Meter Workflow"""

# pytype: skip-file

# beam-playground:
#   name: SmartMeter
#   description: An example that outputs smart meter measurements.
#   multifile: false
#   pipeline_options: --output output.txt
#   categories:
#     - Combiners
#     - Options

import argparse
import logging
import re
import json

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class CalculateDoFn(beam.DoFn):

  def process(self, element):
    pressure_data = float(element['pressure'])
    pressure_psi = pressure_data/6.895

    temperature_data = float(element['temperature'])
    temp_faren = (temperature_data * 1.8) + 32
       
    result = {}
    result['profile_name'] = element['profile_name']
    result['time'] = element['time']
    result['pressure'] = pressure_psi
    result['temperature'] = temp_faren

    return [result]



def run(argv=None):
  parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--input', dest='input', required=True,
                      help='Input file to process.')
  parser.add_argument('--output', dest='output', required=True,
                      help='Output file to write results to.')
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  # We use the save_main_session option because one or more DoFn's in this
  # workflow rely on global context (e.g., a module imported at module level).
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True

  # The pipeline will be run on exiting the with block.
  with beam.Pipeline(options=pipeline_options) as p:

    # Read the text file[pattern] into a PCollection.
    lines = (p | "Read from Pub/Sub" >> beam.io.ReadFromPubSub(topic=known_args.input)
        | "toDict" >> beam.Map(lambda x: json.loads(x)))
    
    conversions=(lines 
        | 'Filter' >> beam.Filter(lambda x: x is not None)
        | 'Convert' >> beam.ParDo(CalculateDoFn()))

    (conversions | 'to byte' >> beam.Map(lambda x: json.dumps(x).encode('utf8'))
        |   'Write to Pub/sub' >> beam.io.WriteToPubSub(topic=known_args.output))

if _name_ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
