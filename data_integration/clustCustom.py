# CLUST Project based custom function
def clustIntegration(intDataInfo, process_param, re_frequency):
    ## Connect DB
    from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
    from KETIPreDataIngestion.data_influx import influx_Client
    db_client = influx_Client.influxClient(ins)
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
    result = data_integration.get_integrated_dataset(imputed_datas, re_frequency)
    return result