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
    multiple_dataset = data_preprocessing.get_preprocessed_Multipledataset(multiple_dataset, process_param)
    ## Integration
    from KETIPreDataIntegration.data_integration import data_integration
    result = data_integration.get_integrated_dataset(multiple_dataset, re_frequency)
    return result