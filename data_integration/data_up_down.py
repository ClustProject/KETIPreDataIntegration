def data_up_sampling(data, freq):
    data = data.sort_index(axis=1)
    data_up = data.resample(freq, label='left').mean()
    data_up = data_up.interpolate(method='values')
    return data_up
    
def data_down_sampling(data, freq):
    data = data.sort_index(axis=1)
    data_down = data.resample(freq, label='left').mean()
    return data_down