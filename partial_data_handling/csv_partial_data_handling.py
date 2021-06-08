import os
import pandas as pd
class CSVPartialDataHandling():
    def __init__(self, directory, file_head):
        self.directory = directory
        self.file_head = file_head
        
    def save_multiple_partial_data_to_csv(self, data_origin_set):
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)
        for key, data in data_origin_set.items():
            data.to_csv(self.directory+self.file_head+str(key)+'.csv')
    
    def load_multiple_partial_data_from_csv(self):
        data_partial_raw={}
        dir_address =self.directory + self.file_head+ '*.csv'
        import glob
        for i, file_name in enumerate(glob.glob(dir_address)):
            data_partial_raw[i] = pd.read_csv(file_name,  index_col = 0, parse_dates = True)
            
        return data_partial_raw