import pandas as pd
from pandas.tseries.frequencies import to_offset
import numpy as np


class PartialMetaData():
    def __init__(self, partial_data_set, freq_check_length = 5):
        self.freq_check_length= freq_check_length
        self.partial_data_set = partial_data_set
        self.partial_data_frequency = self._get_partial_data_freqeuncy_list(self.freq_check_length)
        self.partial_data_frequency_info = self._get_partial_data_frequency_info()
        self.overapped_duration = self._get_partial_data_set_start_end()
        self.integrated_data_type = self._get_partial_data_type()
    
    def _get_partial_data_freqeuncy_list(self, freq_check_length):
    
        data_length = len(self.partial_data_set)
        column_list=[]
        for i in range(data_length):
            data = self.partial_data_set[i]
            columns = data.columns
            for column in columns:
                column_info = {"column_name":'', "column_frequency":'', "column_type":''}
                column_info['column_name'] = column
                freq = to_offset(pd.infer_freq(data[:freq_check_length].index))
                freq = pd.to_timedelta(freq, errors='coerce')
                column_info['column_frequency'] = freq
                column_info['column_type'] = data[column].dtype
                column_list.append(column_info)
        return column_list
     
    def _get_partial_data_frequency_info(self):
        
        partialFreqList=[]
        data_length = len(self.partial_data_set)
        for i in range(data_length):
            freq = to_offset(pd.infer_freq(self.partial_data_set[i][:self.freq_check_length].index))
            freq = pd.to_timedelta(freq, errors='coerce')
            partialFreqList.append(freq)
            
        frequency={}
        frequency['frequency_list']= partialFreqList
        frequency['min_frequency'] = min(partialFreqList)
        frequency['max_frequency'] = max(partialFreqList)
        frequency['frequency_is_same'] = self._check_same_freq(partialFreqList)
        frequency['average_frequency'] = np.mean(partialFreqList)
        frequency['median_frequency'] = np.median(partialFreqList)
        return frequency
    
    def _get_partial_data_set_start_end(self):
        start_list=[]
        end_list =[]
        duration={}
        data_length = len(self.partial_data_set)
        for i in range(data_length):
            data =self.partial_data_set[i]
            start = data.index[0]
            end = data.index[-1]
            start_list.append(start)
            end_list.append(end)
        duration['start'] = max(start_list)
        duration['end'] = min(end_list)
        return duration
    
    # AllNumeric, AllCategory, NumericCategory
    def _get_partial_data_type(self):
        numCols_list =[]
        catCols_list=[]
        data_length = len(self.partial_data_set)
        for i in range(data_length):
            data =self.partial_data_set[i]
            numCols = data.select_dtypes("number").columns
            catCols = data.select_dtypes(include=["object", "bool", "category"]).columns
            numCols_list.extend(numCols.values)
            catCols_list.extend(catCols.values)
        if len(numCols_list) >  0 :
            if len(catCols_list) == 0 :
                datatype = "AllNumeric"
            else:
                datatype ="NumericCategory"
        else:
            datatype = "AllCategory"
          
        return datatype
    
    
    def _check_same_freq(self, item_list):
        return(len(set(item_list))) < 2
        
    