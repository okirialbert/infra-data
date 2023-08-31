from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import pandas as pd
import json
import re
import os
import numpy as np

class OECDTransformOperator(BaseOperator):
    """
    Operator transforms JSON data from OECD in SDMX format to CSV in output path.

    Parameters
    ----------
    response : http.response
        HTTP JSON response from OECD in SDMX format.
    output_path : str
        Path to write the transformed table to including the .csv file extension.
    """

    template_fields = ("_response", "_output_path")

    @apply_defaults
    def __init__(
        self,
        response,
        output_path,
        **kwargs,
    ):
        super(OECDTransformOperator, self).__init__(**kwargs)

        self._response = response
        self._output_path = output_path

    def execute(self, context):
        response = self._response

        responseJson = json.loads(response)
        dataJson = responseJson.get('data')
        metaJson = responseJson.get('meta')

        obsList = dataJson.get('dataSets')[0].get('observations')

        if (len(obsList) > 0):

            timeList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'TIME_PERIOD'][0]['values']
            refArea = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'REF_AREA'][0]['values']
            freqList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'FREQ'][0]['values']
            subjectList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'ACTIVITY'][0]['values']
            measureList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'MEASURE'][0]['values']
            unitMeasureList = [item for item in dataJson.get('structure').get('dimensions').get('observation') if item['id'] == 'UNIT_MEASURE'][0]['values']

            obs = pd.DataFrame(obsList).transpose()
            obs.rename(columns = {0: 'series'}, inplace = True)
            obs['id'] = obs.index
            obs = obs[['id', 'series']]
            obs['dimensions'] = obs.apply(lambda x: re.findall('\d+', x['id']), axis = 1)
            obs['ref_area'] = obs.apply(lambda x: refArea[int(x['dimensions'][0])]['id'], axis = 1)
            obs['frequency'] = obs.apply(lambda x: freqList[int(x['dimensions'][1])]['id'], axis = 1)
            obs['subject'] = obs.apply(lambda x: subjectList[int(x['dimensions'][4])]['id'], axis = 1)
            obs['measure'] = obs.apply(lambda x: measureList[int(x['dimensions'][2])]['id'], axis = 1)
            obs['unit_measure'] = obs.apply(lambda x: unitMeasureList[int(x['dimensions'][3])]['id'], axis = 1)
            obs['time'] = obs.apply(lambda x: timeList[int(x['dimensions'][5])]['id'], axis = 1)
            obs['names'] = obs['subject'] + '_' + obs['measure']
            obs = obs.reset_index()
            obs = obs[['series', 'ref_area', 'frequency', 'subject', 'measure', 'unit_measure', 'time']]
            obs = obs.astype({'series': 'float32', 'ref_area': 'string', 'frequency': 'string', 'subject': 'string', 'measure': 'string', 'unit_measure': 'string'})
            obs['time']= pd.to_datetime(obs['time'], format='%Y')

            tmp_path = os.path.join(self._output_path)
            obs.to_csv(tmp_path ,header=None, index=False)

            