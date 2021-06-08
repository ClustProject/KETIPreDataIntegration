import pandas as pd 
from KETIPreDataIntegration.partial_data_information import partial_data_meta
class DataIntegration():
    def __init__(self, data_partial, freq_check_length = 5):    
        self.partial_dataset_info = partial_data_meta.PartialMetaData(data_partial, freq_check_length)
        self.data_partial = data_partial
        
    def simple_integration(self):
        data_key_list = list(self.data_partial.keys())
        merged_data_list =[]
        partial_data={}
        for data_name in data_key_list:
            partial_data = self.data_partial[data_name].sort_index(axis=1)
            partial_data = partial_data.fillna("missingData")
            merged_data_list.append(partial_data)
        merged_data = pd.concat(merged_data_list, axis=1, join='inner', sort=True)

        return merged_data
        
       
#
"""
selected_data = valid_data.drop(drop_features, axis=1, errors='ignore')
"""