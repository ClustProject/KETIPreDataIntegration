import pandas as pd
from pandas.tseries.frequencies import to_offset
import numpy as np


                
class PartialMetaData():
    def __init__(self, partial_data_set, freq_check_length = 5):
        self.partial_data_set = partial_data_set
        self.partial_meta={}
        self.freq_check_length= freq_check_length
        self.partial_freq_list = self._get_partial_data_freqeuncy_list(partial_data_set, self.freq_check_length)
        self.frequency = self._get_partial_data_frequency_info(self.partial_freq_list)
        self.duration = self._get_partial_data_set_start_end(partial_data_set)
        self.partialSet_data_type = self._get_partial_data_type(partial_data_set)
        
    def _get_partial_data_freqeuncy_list(self, partial_data_set, freq_check_length):
        partialFreqList=[]
        data_length = len(partial_data_set)
        for i in range(data_length):
            freq = to_offset(pd.infer_freq(partial_data_set[i][:freq_check_length].index))
            freq = pd.to_timedelta(freq, errors='coerce')
            partialFreqList.append(freq)
        return partialFreqList
        
    def _get_partial_data_frequency_info(self, partial_freq_list):
        frequency={}
        frequency['min'] = min(partial_freq_list)
        frequency['max'] = max(partial_freq_list)
        frequency['same'] = self._check_same_freq(partial_freq_list)
        frequency['average'] = np.mean(partial_freq_list)
        frequency['meidan'] = np.median(partial_freq_list)
        return frequency
    
    def _get_partial_data_set_start_end(self, partial_data_set):
        start_list=[]
        end_list =[]
        duration={}
        data_length = len(partial_data_set)
        for i in range(data_length):
            data =partial_data_set[i]
            start = data.index[0]
            end = data.index[-1]
            start_list.append(start)
            end_list.append(end)
        duration['start'] = max(start_list)
        duration['end'] = min(end_list)
        return duration
    
    # AllNumeric, AllCategory, NumericCategory
    def _get_partial_data_type(self, partial_data_set):
        numCols_list =[]
        catCols_list=[]
        data_length = len(partial_data_set)
        for i in range(data_length):
            data =partial_data_set[i]
            numCols = data.select_dtypes("number").columns
            catCols = data.select_dtypes("object").columns
            numCols_list.append(numCols)
            catCols_list.append(catCols)
    
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
        
    