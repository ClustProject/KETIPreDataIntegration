 def data_integration(self, data, resample_min):
        resample_freq = str(resample_min)+'min'
        data_key_list = list(data.keys())
        merged_data_list =[]
        data_resample={}
        for data_name in data_key_list:
            data_resample[data_name] = data[data_name].resample(resample_freq, label='left').mean()
            data_resample[data_name] = data_resample[data_name].fillna(method='bfill', limit=1)
            data_resample[data_name] = data_resample[data_name].fillna(method='ffill', limit=1)
            merged_data_list.append(data_resample[data_name])
        
        merged_data = pd.concat(merged_data_list, axis=1, join='inner')
        merged_data=merged_data.sort_index(axis=1)
       
        return merged_data