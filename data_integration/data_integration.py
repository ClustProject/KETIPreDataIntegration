import pandas as pd 
import numpy as np


class DataIntegration():
    def __init__(self, data_partial):    
        self.data_partial = data_partial

        
    def simple_integration(self, duration):
        data_key_list = list(self.data_partial.keys())
        merged_data_list =[]
        for data_name in data_key_list:
            partial_data = self.data_partial[data_name].sort_index(axis=1)
            merged_data_list.append(partial_data)
        merged_data = pd.concat(merged_data_list, axis=1, join='outer', sort=True)#inner

        start_time = duration['start_time']
        end_time = duration['end_time']
        merged_data = merged_data[start_time:end_time]
        self.merged_data = merged_data 
        return self.merged_data
    
    def restructured_data_with_new_frequency(self, re_frequency, column_characteristics, partial_data_type):
        column_function={}
        for column_name in column_characteristics:
            #reStructuredData = data.resample(frequency).apply(np.mean)
            column_info = column_characteristics[column_name]
            origin_frequency = column_info['column_frequency']
            if origin_frequency <= re_frequency: #down_sampling
                sampling_method = column_info['downsampling_method']
            if origin_frequency > re_frequency: #upsampling
                sampling_method = column_info['upsampling_method']
            column_function[column_name]  = sampling_method

        if (partial_data_type == 'AllNumeric'):
            print('All column data are numeric')
            
        else:
            print('Having Category or string data')
        
        reStructuredData = self.merged_data.resample(re_frequency).agg(column_function)     
        return reStructuredData 

    def restructured_data_fillna(self, origin_data, column_characteristics,re_frequency):
        column_function={}
        reStructuredData = origin_data.copy()
        for column_name in column_characteristics:
            #reStructuredData = data.resample(frequency).apply(np.mean)
            column_info = column_characteristics[column_name]
            origin_frequency = column_info['column_frequency']
            fillna_function = column_info['fillna_function']
            limit_num = column_info['fillna_limit']
            if origin_frequency > re_frequency: #upsampling
                reStructuredData[column_name] = self._fillna_for_upsampling(reStructuredData[column_name], fillna_function, limit_num)
            else:
                fillna_function='none'
            column_function[column_name]  = fillna_function
        return reStructuredData 

    def _fillna_for_upsampling(self, data, fillna_function, limit_num):

        if fillna_function =='interpolate':
            filled_data = data.interpolate(limit=limit_num)
        else:
            filled_data = data.fillna(method=fillna_function, limit=limit_num)
        return filled_data

