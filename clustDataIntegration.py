import sys
sys.path.append("../")
sys.path.append("../..")

# CLUST Project based custom function

class ClustIntegration():
    def __init__(self):
        pass

    def clustIntegrationFromInfluxSource(self, intDataInfo, process_param, integration_param):
        ## Connect DB
        from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
        from KETIPreDataIngestion.data_influx import influx_Client
        db_client = influx_Client.influxClient(ins.CLUSTDataServer)
        ## multiple dataset
        multiple_dataset  = db_client.get_MeasurementDataSet(intDataInfo)
        ## Preprocessing
        from KETIPrePartialDataPreprocessing import data_preprocessing
        #process_param = {'refine_param':refine_param, 'outlier_param':outlier_param, 'imputation_param':imputation_param}
        partialP = data_preprocessing.packagedPartialProcessing(process_param)
        multiple_dataset = partialP.MultipleDatasetallPartialProcessing(multiple_dataset)
        ## Integration
        from KETIPreDataIntegration.meta_integration import data_integration
        imputed_datas = {}
        integrationMethod = integration_param['method']
        for key in multiple_dataset.keys():
            imputed_datas[key]=(multiple_dataset[key]["imputed_data"])
        if integrationMethod=="meta":
            result = self.getIntegratedDataSetByMeta(imputed_datas, integration_param['granularity_sec'])
        elif integrationMethod=="ML":
            #result = self.getIntegratedDataSetByML(imputed_datas, integration_param['granularity_sec'], integration_param['param'])
            result = self.getIntegratedDataSetByML(imputed_datas, integration_param['param'])
        else:
            result = self.getIntegratedDataSetByMeta(imputed_datas, integration_param['granularity_sec'])

        return result

    def getIntegratedDataSetByML(self, data_set, param):
        from KETIPreDataIntegration.ml_integration import RNNAEAlignment
        # 필요시 모 함수에서 받아오는 파라미터가 수정 및 확장되어야함 황지수씨 시작점
        model = param["model"]
        if model == "RNN_AE":
            alignment_result = RNNAEAlignment.Alignment().RNN_AE(data_set, param['parameter'])
        else :
            print('Not Available')
            
        return alignment_result

    def getIntegratedDataSetByMeta(self, data_set, integration_freq_second):
        from KETIPreDataIntegration.meta_integration import partialDataInfo
        partial_data_info = partialDataInfo.PartialData(data_set)
        # Integration
        from KETIPreDataIntegration.meta_integration import data_integration
        data_it = data_integration.DataIntegration(data_set)
        
        import datetime
        re_frequency = datetime.timedelta(seconds= integration_freq_second)
        integrated_data_resample = data_it.dataIntegrationByMeta(re_frequency, partial_data_info.column_meta)
        
        return integrated_data_resample 




