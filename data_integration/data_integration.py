import pandas as pd 
import numpy as np


class DataIntegration():
    def __init__(self, data_partial, partial_data_info):    
        self.data_partial = data_partial
        self.integration_info = partial_data_info.column_meta
        self.partial_data_type = partial_data_info.integrated_data_type

        
    def simple_integration(self):
        data_key_list = list(self.data_partial.keys())
        merged_data_list =[]
        for data_name in data_key_list:
            partial_data = self.data_partial[data_name].sort_index(axis=1)
            merged_data_list.append(partial_data)
        merged_data = pd.concat(merged_data_list, axis=1, join='outer', sort=True)#inner
        duration = self.integration_info['overap_duration']
        print(duration['start_time'])
        print(duration['end_time'])
        print(merged_data)
        merged_data = merged_data_list[str(duration['start_time']):str(duration['end_time'])]
        return merged_data
    
    def restructured_data_with_new_frequency(self, data, frequency, method):

        if (self.partial_data_info.integrated_data_type == 'AllNumeric'):
            print('All column data are numeric')
            reStructuredData = data.resample(frequency).apply(method)
        else:
            #if (None not in self.partial_data_frequency):
            print('Having Category or string data')
            reStructuredData = data
            
        return reStructuredData  