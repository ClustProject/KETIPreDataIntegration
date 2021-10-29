import pandas as pd 
import numpy as np

def get_integrated_dataset(data_set, re_frequency_min=4):
    from KETIToolDataExploration.partial_meta_data import partial_data_meta
    partial_data_info = partial_data_meta.PartialMetaData(data_set)
    # Integration
    import datetime
    re_frequency = datetime.timedelta(seconds= re_frequency_min*60)
    from KETIPreDataIntegration.data_integration import data_integration
    integrated_data_resample = data_integration.partial_data_integration(re_frequency, data_set, partial_data_info.column_meta)
    
    return integrated_data_resample 
    
def temp(x):
    return lambda x: x.max() - x.min()
    
def partial_data_integration(re_frequency, partial_data_set, column_meta):
    data_int = DataIntegration(partial_data_set)
    integrated_data = data_int.simple_integration(column_meta['overap_duration'])
    
    #integrated_data_resample = data_int.restructured_data_with_new_frequency(integrated_data, partial_data_info.partial_frequency_info['max_frequency'], partial_data_info.column_meta['column_characteristics'])
    column_characteristics = column_meta['column_characteristics']
    
    """
    for columncharacteristicstic in column_characteristics:
        print(column_characteristics[columncharacteristicstic])
        pass
    #column_characteristics['data0']['upsampling_method']=np.interp
    """
    integrated_data_resample = data_int.restructured_data_with_new_frequency(re_frequency, column_characteristics)
    integrated_data_resample_fillna = data_int.restructured_data_fillna(integrated_data_resample, column_characteristics,re_frequency )
    return integrated_data_resample_fillna

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
    
        
    def restructured_data_with_new_frequency(self, re_frequency, column_characteristics):
        column_function={}
        for column_name in column_characteristics:
            #reStructuredData = data.resample(frequency).apply(np.mean)
            column_info = column_characteristics[column_name]
            origin_frequency = column_info['column_frequency']
            if origin_frequency <= re_frequency: #down_sampling
                sampling_method_string = column_info['downsampling_method']
            if origin_frequency > re_frequency: #upsampling
                sampling_method_string = column_info['upsampling_method']
            sampling_method = self.converting_sampling_method(sampling_method_string)
            column_function[column_name] = sampling_method
            #sampling_method = sampling_method_string

        reStructuredData = self.merged_data.resample(re_frequency).agg(column_function)     
        return reStructuredData 

    def converting_sampling_method(self, sampling_method_string):
        if sampling_method_string =='mean':
            sampling_method = np.mean
        elif sampling_method_string =='median':
            sampling_method = np.median
        elif sampling_method_string == 'ffill':
            sampling_method="ffill"
        elif sampling_method_string =='bfill':
            sampling_method = 'bfill'
        #elif sampling_method_string == ''
        return sampling_method
        
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

