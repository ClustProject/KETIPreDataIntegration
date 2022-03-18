# CLUST Project based custom function
def clustIntegration(intDataInfo, process_param, re_frequency):
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
    from KETIPreDataIntegration.data_integration import data_integration
    imputed_datas = {}
    for key in multiple_dataset.keys():
        imputed_datas[key]=(multiple_dataset[key]["imputed_data"])
    result = get_integrated_dataset(imputed_datas, re_frequency)
    return result



def get_integrated_dataset(data_set, re_frequency_min):
    from KETIPreDataIntegration.meta_integration import partialDataInfo
    partial_data_info = partialDataInfo.PartialData(data_set)
    # Integration
    import datetime
    re_frequency = datetime.timedelta(seconds= re_frequency_min*60)
    from KETIPreDataIntegration.data_integration import data_integration
    integrated_data_resample = data_integration.dataIntegrationByMeta(re_frequency, data_set, partial_data_info.column_meta)
    
    return integrated_data_resample 