
import pandas as pd

from pandas.tseries.frequencies import to_offset
class PartialMetaData():
    def __init__(self, partial_data_set):
        self.partial_data_set = partial_data_set
        self.partial_meta={}
        
    def set_partial_freq(self, length=5):
        partialFreq=[]
        data_length = len(self.partial_data_set)
        for i in range(data_length):
            freq = to_offset(pd.infer_freq(self.partial_data_set['partial_'+str(i)][:length].index))
            freq = pd.to_timedelta(freq, errors='coerce')
            partialFreq.append(str(freq))
        self.partial_meta['partial_freq'] = partialFreq
        return self.partial_meta
    
    def get_min_freq(self):
        return min(self.partial_meta['partial_freq'])
    
    def get_max_freq(self):
        return max(self.partial_meta['partial_freq'])