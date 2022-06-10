from functools import partial
import sys
sys.path.append("../")
sys.path.append("../..")

# CLUST Project based custom function

class ClustIntegration():
    """
    Data Integration Class
    """
    def __init__(self):
        pass

    def clustIntegrationFromInfluxSource(self, intDataInfo, process_param, integration_param):
        """ 
        사용자가 입력한 Parameter에 따라 데이터를 병합하는 함수
        1. intDataInfo 에 따라 InfluxDB로 부터 데이터를 읽어와 DataSet을 생성
        2. 병합에 필요한 partialDataInfo(column characteristics)를 추출
        3. Refine Frequency 진행
        4. 입력 method에 따라 ML(transformParam) 혹은 Meta(column characteristics)으로 데이터 병합

        :param  intDataInfo: 병합하고 싶은 데이터의 정보로 DB Name, Measuremen Name, Start Time, End Time를 기입
        :type intDataInfo: json
            
        :param  process_param: Refine Frequency를 하기 위한 Preprocessing Parameter
        :type process_param: json
        
        :param  integration_param: Integration을 위한 method, transformParam이 담긴 Parameter
        :type integration_param: json
        >>> integration_param = {
                "granularity_sec":"",
                "transformParam":{
                                "model": 'RNN_AE',
                                "model_parameter": {
                                    "window_size": 10, # 모델의 input sequence 길이, int(default: 10, 범위: 0 이상 & 원래 데이터의 sequence 길이 이하)
                                    "emb_dim": 5, # 변환할 데이터의 차원, int(범위: 16~256)
                                    "num_epochs": 50, # 학습 epoch 횟수, int(범위: 1 이상, 수렴 여부 확인 후 적합하게 설정)
                                    "batch_size": 128, # batch 크기, int(범위: 1 이상, 컴퓨터 사양에 적합하게 설정)
                                    "learning_rate": 0.0001, # learning rate, float(default: 0.0001, 범위: 0.1 이하)
                                    "device": 'cpu' # 학습 환경, ["cuda", "cpu"] 중 선택
                                }
                            },
                "method":"ML" #["ML", "meta", "simple]
            }
            integrationFreq_min= 30
            integration_freq_sec = 60 * integrationFreq_min# 분
            integration_param = {
                "granularity_sec":integration_freq_sec,
                "param":{},
                "method":"meta"
            }
                
        :return: integrated_data
        :rtype: DataFrame    
        """
        ## Connect DB
        from KETIPreDataIngestion.KETI_setting import influx_setting_KETI as ins
        from KETIPreDataIngestion.data_influx import influx_Client
        db_client = influx_Client.influxClient(ins.CLUSTDataServer)
        ## multiple dataset
        multiple_dataset  = db_client.get_MeasurementDataSet(intDataInfo)
        ## get partialDataInfo
        from KETIPreDataIntegration.meta_integration import partialDataInfo
        partial_data_info = partialDataInfo.PartialData(multiple_dataset)
        overlap_duration = partial_data_info.column_meta["overlap_duration"]
        integration_freq_sec = integration_param["granularity_sec"]
        ## set refine frequency parameter
        if not integration_freq_sec:
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
            result = self.getIntegratedDataSetByMeta(imputed_datas, integration_freq_sec, partial_data_info)
        elif integrationMethod=="ML":
            result = self.getIntegratedDataSetByML(imputed_datas, integration_param['transformParam'], overlap_duration)
        elif integrationMethod=="simple":
            result = self.getIntegratedDataSetByML(imputed_datas, integration_param['transformParam'], overlap_duration)
        else:
            result = self.IntegratedDataSetBySimple(imputed_datas, overlap_duration)

        return result

    def getIntegratedDataSetByML(self, data_set, transform_param, overlap_duration):
        """ 
        ML을 활용한 데이터 병합 함수
        1. 병합한 데이터에 RNN_AE 를 활용해 변환된 데이터를 반환

        :param  data_set: 병합하고 싶은 데이터들의 셋
        :type intDataInfo: json
            
        :param  transform_param: RNN_AE를 하기 위한 Parameter
        :type process_param: json
        >>> transformParam = {
                "model": 'RNN_AE',
                "model_parameter": {
                    "window_size": 10, # 모델의 input sequence 길이, int(default: 10, 범위: 0 이상 & 원래 데이터의 sequence 길이 이하)
                    "emb_dim": 5, # 변환할 데이터의 차원, int(범위: 16~256)
                    "num_epochs": 50, # 학습 epoch 횟수, int(범위: 1 이상, 수렴 여부 확인 후 적합하게 설정)
                    "batch_size": 128, # batch 크기, int(범위: 1 이상, 컴퓨터 사양에 적합하게 설정)
                    "learning_rate": 0.0001, # learning rate, float(default: 0.0001, 범위: 0.1 이하)
                    "device": 'cpu' # 학습 환경, ["cuda", "cpu"] 중 선택
                }
            }
        
        :param  overlap_duration: 병합하고 싶은 데이터들의 공통 시간 구간
        :type integration_param: json
        >>> overlap_duration = {'start_time': Timestamp('2018-01-03 00:00:00'), 'end_time': Timestamp('2018-01-05 00:00:00')}
                
        :return: integrated_data by transform
        :rtype: DataFrame    
        """
        from KETIPreDataIntegration.ml_integration import RNNAEAlignment
        from KETIPreDataIntegration.meta_integration import data_integration
        
        ## simple integration
        data_int = data_integration.DataIntegration(data_set)
        dintegrated_data = data_int.simple_integration(overlap_duration)
        
        model = transform_param["model"]
        transfomrParam = transform_param['model_parameter']
        if model == "RNN_AE":
            alignment_result = RNNAEAlignment.Alignment().RNN_AE(dintegrated_data, transfomrParam)
        else :
            print('Not Available')
            
        return alignment_result

    def getIntegratedDataSetByMeta(self, data_set, integration_freq_sec, partial_data_info):
        """ 
        Meta(column characteristics)을 활용한 데이터 병합 함수

        :param  data_set: 병합하고 싶은 데이터들의 셋
        :type intDataInfo: json
            
        :param  integration_freq_sec: 조정하고 싶은 second 단위의 Frequency
        :type process_param: json
        
        :param  partial_data_info: column characteristics의 info
        :type integration_param: json
      
        :return: integrated_data
        :rtype: DataFrame    
        """
        ## Integration
        from KETIPreDataIntegration.meta_integration import data_integration
        data_it = data_integration.DataIntegration(data_set)
        
        import datetime
        re_frequency = datetime.timedelta(seconds= integration_freq_sec)
        integrated_data_resample = data_it.dataIntegrationByMeta(re_frequency, partial_data_info.column_meta)
        
        return integrated_data_resample 
    
    def IntegratedDataSetBySimple(self, data_set, integration_freq_sec, overlap_duration):
        """ 
        Simple한 병합

        :param  data_set: 병합하고 싶은 데이터들의 셋
        :type intDataInfo: json

        :param  integration_freq_sec: 조정하고 싶은 second 단위의 Frequency
        :type process_param: json
            
        :param  overlap_duration: 조정하고 싶은 second 단위의 Frequency
        :type overlap_duration: json
      
        :return: integrated_data
        :rtype: DataFrame    
        """
        ## Integration
        from KETIPreDataIntegration.meta_integration import data_integration
        ## simple integration
        data_int = data_integration.DataIntegration(data_set)
        dintegrated_data = data_int.simple_integration(overlap_duration)
        dintegrated_data = dintegrated_data.resample(integration_freq_sec).mean()
        
        return dintegrated_data





