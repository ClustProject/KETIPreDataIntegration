import sys
sys.path.append("../")
sys.path.append("../..")



if __name__ == "__main__":
    # TODO ST OH
    # 임의로 3개 DB에 대한 데이터를 불러와 전처리를 하고 정합합니다. (intDataInfo)
    # refine, outlier, imputation parameter에 따라 순차적으로 Cleaning 합니다.
    # 각각은 따로따로 사용할 수도 있기 때문에 처음에는 통합했는데 지금은 분리했습니다.
    # outlier 에서 uncertainOutlierToNaN을 불러오면 에러가 나와서 False 처리 했어요. 봐주세요.
    intDataInfo = { "db_info":[ { "db_name":"farm_inner_air", "measurement":"HS1", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" },
     { "db_name":"farm_outdoor_air", "measurement":"sangju", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" }, 
     { "db_name":"farm_outdoor_weather", "measurement":"sangju", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" } ] }

    refine_param = {'removeDuplication':True, 'staticFrequency':True}
    outlier_param= {'certainOutlierToNaN':True, 'uncertainOutlierToNaN':False, 'data_type':'air'}
    imputation_param ={
    "imputation_method":[
        {"min":0,"max":1,"method":"mean"},
        {"min":2,"max":4,"method":"linear"},
        {"min":5,"max":10,"method":"brits"}],
    "totalNanLimit":0.3}
    process_param = {'refine_param':refine_param, 'outlier_param':outlier_param, 'imputation_param':imputation_param}
    re_frequency = 4    
    from KETIPreDataIntegration.data_integration import clustCustom
    result = clustCustom.clustIntegration(intDataInfo, process_param, re_frequency)


