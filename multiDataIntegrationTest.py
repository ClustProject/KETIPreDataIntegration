import sys
sys.path.append("../")
sys.path.append("../..")



if __name__ == "__main__":
    intDataInfo = { "db_info":[ { "db_name":"farm_inner_air", "measurement":"HS1", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" },
     { "db_name":"farm_outdoor_air", "measurement":"sangju", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" }, 
     { "db_name":"farm_outdoor_weather", "measurement":"sangju", "start":"2020-09-11 00:00:00", "end":"2020-10-18 00:00:00" } ] }

    refine_param = {
        "removeDuplication":{"flag":True},
        "staticFrequency":{"flag":True, "frequency":None}
    }
    outlier_param  = {
        "certainErrorToNaN":{"flag":True},
        "unCertainErrorToNaN":{
            "flag":True,
            "param":{"neighbor":0.5}
        },
        "data_type":"air"
    }
    imputation_param = {
        "serialImputation":{
            "flag":True,
            "imputation_method":[{"min":0,"max":50,"method":"linear", "parameter":{}}],
            "totalNonNanRatio":80
        }
    }
    process_param = {'refine_param':refine_param, 'outlier_param':outlier_param, 'imputation_param':imputation_param}
    re_frequency = 4    
    from KETIPreDataIntegration.data_integration import clustCustom
    result = clustCustom.clustIntegration(intDataInfo, process_param, re_frequency)


