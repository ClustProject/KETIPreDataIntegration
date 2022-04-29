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
        ## get partialDataInfo
        from KETIPreDataIntegration.meta_integration import partialDataInfo
        partial_data_info = partialDataInfo.PartialData(multiple_dataset)
        ## set refine frequency parameter
        if not integration_param['granularity_second']:
            process_param["refine_param"]["staticFrequency"]["frequency"] = partial_data_info.partial_frequency_info['GCDs']
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
            result = self.getIntegratedDataSetByMeta(imputed_datas, integration_param['granularity_sec'], partial_data_info)
        elif integrationMethod=="ML":
            #result = self.getIntegratedDataSetByML(imputed_datas, integration_param['granularity_sec'], integration_param['param'])
            result = self.getIntegratedDataSetByML(imputed_datas, integration_param['param'], partial_data_info)
        else:
            result = self.getIntegratedDataSetByMeta(imputed_datas, integration_param['granularity_sec'], partial_data_info)

        return result

    def getIntegratedDataSetByML(self, data_set, param, partial_data_info):
        from KETIPreDataIntegration.ml_integration import RNNAEAlignment
        from KETIPreDataIntegration.meta_integration import data_integration
        
        ## simple integration
        data_int = data_integration.DataIntegration(data_set)
        dintegrated_data = data_int.simple_integration(partial_data_info.column_meta["overlap_duration"])
        
        model = param["model"]
        if model == "RNN_AE":
            alignment_result = RNNAEAlignment.Alignment().RNN_AE(dintegrated_data, param['model_parameter'])
        else :
            print('Not Available')
            
        return alignment_result

    def getIntegratedDataSetByMeta(self, data_set, integration_freq_second, partial_data_info):
        ## Integration
        from KETIPreDataIntegration.meta_integration import data_integration
        data_it = data_integration.DataIntegration(data_set)
        
        import datetime
        re_frequency = datetime.timedelta(seconds= integration_freq_second)
        integrated_data_resample = data_it.dataIntegrationByMeta(re_frequency, partial_data_info.column_meta)
        
        return integrated_data_resample 




