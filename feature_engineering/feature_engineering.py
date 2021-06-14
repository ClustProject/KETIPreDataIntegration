import pandas as pd
import numpy as np


import KETIPreDataIntegration.feature_engineering.feature_extension as fe
        
class FeatureEngineering():
    def __init__(self):
        #data_range_level
        #ratio_feature ={'Humidity':'out_humid', 'Temperature':'out_temp'}
        pass
    
    def feature_extension(self, data, vector_feature_list, nation):
        fe_e = fe.FeatureExtension()
        #Data_Ex = fe_e.add_level_features(data, data_range_level)
        Data_Ex = fe_e.add_vector_features(data, vector_feature_list)
        Data_Ex = fe_e.add_time_Sine_Cosine_features(Data_Ex)
        Data_Ex = fe_e.workday_feature_genaration(Data_Ex, nation)

        return Data_Ex
        
    
    

"""
def get_scaledD(self, Data):
    mD = pre.ProcessedData()
    scaler, data_scale = mD.data_scaling(Data, )
    return scaler, data_scale
"""
