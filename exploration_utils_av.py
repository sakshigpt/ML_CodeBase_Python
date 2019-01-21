### Adding utilities which will help us understand data better
import os,json
import pdb

import pandas as pd
import numpy as np

import data_utils, common_utils, feature_utils

def get_datadict(filename, get_csv=True, **kwargs):
    """
    Gives column wise details about the dataset to help with better processing the data
    Arguments:
        filename: str, location of file to read
        get_csv: print the dictionary out to csv, the name of csv will be (orig_filename)_datadict.csv
    returns:
        data_dict: Pandas dataframe
    """
    
    if "delimiter" in kwargs.keys():
        data = data_utils.read_file(filename, delimiter = kwargs["delimiter"])
    else:
        data = data_utils.read_file(filename)
    
    data_dict = pd.DataFrame(index = data.columns)
    try:
        sample = data[data.isnull().sum(axis=1)==0].iloc[0]
    except:
        print ("{} had no rows with zero blanks, saving the first row instead".format(filename))
        sample = data.iloc[0]
    data_dict["example"] = sample
    
    dtype_per_col = data.dtypes
    data_dict["datatype"] = dtype_per_col
    
    unique_per_col = data.nunique()
    data_dict["Unique_values"] = unique_per_col
    
    nulls_if_any = data.isnull().any()
    data_dict["nulls_present"] = nulls_if_any
    
    nulls_amount = data.isnull().sum(axis=0)
    data_dict["nulls_amount"] = nulls_amount
    
    nulls_fraction = nulls_amount/data.shape[0]
    data_dict["nulls_fraction"] = nulls_fraction
    
    if get_csv:
        data_loc = ".".join(filename.split(".")[:-1])
        data_dict.to_csv("{}_dictionary.csv".format(data_loc))
    
    return data_dict
    
def get_datadicts(filenames, data_folder):
    """
    Thin wrapper around get_datadict, it runs a loop around different filenames
    Arguments:
        filenames: list, list of files to read
        data_folder: str, location of file to read from
    """
    for filename in filenames:
        file_loc = os.path.join(data_folder, filename)
##TODO shift delimiter as arg. Maybe convert list to dictionary
        _ = get_datadict(file_loc, delimiter ="|")

def PTP_Source_analysis(PTP_data, val):
    """
    Get distribution by PA source, representative vs non-representative
    Arguments:
        PTP_data: Pandas DataFrame, Raw PTP Data
    Returns:
        REP_analysis: dict
    """
    REP_analysis = {}
    
    REP_present = {}
    
    PTP_data = PTP_data.applymap(lambda x: x.strip() if type(x) == str else x)

    get_diff_days = lambda x: (x[1] - x[0]).days 
    
    PTP_dt_columns = ["PROMISE_DATETIME", "START_DATETIME"]
    if "PROMISE_DATETIME" not in PTP_data.columns:
        PTP_data["PROMISE_DATETIME"] = pd.to_datetime(PTP_data["PROMISE_MADE_DATE"], errors='coerce')
       
    if "START_DATETIME" not in PTP_data.columns:
        PTP_data["START_DATETIME"] = pd.to_datetime(PTP_data["start_dt"], errors='coerce')
    
    if "promised_extention" not in PTP_data.columns:
        PTP_data["promised_extention"] = PTP_data[PTP_dt_columns].apply(get_diff_days, 
                                                                    axis = 1)
    PTP_data_REP = PTP_data[PTP_data["PA_Source"] == val]
    
    REP_keptpromise = PTP_data_REP[PTP_data_REP["pmt_pln_dspt_cd"] == "KPTPROM"]
    REP_brokenpromise = PTP_data_REP[PTP_data_REP["pmt_pln_dspt_cd"] == "BRKPROM"]
    
    REP_calls = PTP_data_REP.shape[0]
    REP_keptpromise_calls = REP_keptpromise.shape[0]
    REP_brokenpromise_calls = REP_brokenpromise.shape[0]
    
    REP_amount = PTP_data_REP["tot_prms_amt"].mean()
    REP_keptpromise_amount = REP_keptpromise["tot_prms_amt"].mean()
    REP_brokenpromise_amount = REP_brokenpromise["tot_prms_amt"].mean()
    
    single_installments_REP = PTP_data_REP[PTP_data_REP["pln_type_cd"] == 1].shape[0]
    multiple_installments_REP = PTP_data_REP[PTP_data_REP["pln_type_cd"] != 1].shape[0]
    
    REP_pmt_mthd_cnt = PTP_data_REP["pmt_mthd_cd"].value_counts().to_dict()
    
    REP_extention_mean = PTP_data_REP["promised_extention"].mean()
    REP_extention_median = PTP_data_REP["promised_extention"].quantile(q=0.5)
    
    #EDIT (12/04) Adding parameters for extention when Promises are kept
    REP_kptprom_extention_mean = REP_keptpromise["promised_extention"].mean()
    REP_kptprom_extention_median = REP_keptpromise["promised_extention"].quantile(q=0.5)
    
    REP_brkprom_extention_mean = REP_brokenpromise["promised_extention"].mean()
    REP_brkprom_extention_median = REP_brokenpromise["promised_extention"].quantile(q=0.5)
    
    REP_present["Total Calls"] = REP_calls
    REP_present["Calls Kept Promise"] = REP_keptpromise_calls
    REP_present["Calls Broken Promise"] = REP_brokenpromise_calls
    REP_present["Mean amount"] = REP_amount
    REP_present["Kept Promise amount"] = REP_keptpromise_amount
    REP_present["Broken Promise amount"] = REP_brokenpromise_amount
    REP_present["Single installments"] = single_installments_REP
    REP_present["Multiple installments"] = multiple_installments_REP
    REP_present["Payment Method count"] = REP_pmt_mthd_cnt
    REP_present["Mean extention"] = REP_extention_mean
    REP_present["Median extention"] = REP_extention_median
    REP_present["Kept Promise extention"] = REP_kptprom_extention_mean
    REP_present["Broken Promise extention"] = REP_brkprom_extention_mean
    
    REP_analysis["{}_present".format(val)] = REP_present
    
    REP_absent = {}
    
    PTP_data_othersource = PTP_data[PTP_data["PA_Source"] != val]
    othersource_keptpromise = PTP_data_othersource[PTP_data_othersource["pmt_pln_dspt_cd"] == "KPTPROM"]
    othersource_brokenpromise = PTP_data_othersource[PTP_data_othersource["pmt_pln_dspt_cd"] == "BRKPROM"]
    
    othersource_calls = PTP_data_othersource.shape[0]
    othersource_keptpromise_calls = othersource_keptpromise.shape[0]
    othersource_brokenpromise_calls = othersource_brokenpromise.shape[0]
    
    othersource_amount = PTP_data_othersource["tot_prms_amt"].mean()
    othersource_keptpromise_amount = othersource_keptpromise["tot_prms_amt"].mean()
    othersource_brokenpromise_amount = othersource_brokenpromise["tot_prms_amt"].mean()
    single_installments_othersource = PTP_data_othersource[PTP_data_othersource["pln_type_cd"] == 1].shape[0]
    multiple_installments_othersource = PTP_data_othersource[PTP_data_othersource["pln_type_cd"] != 1].shape[0]
    
    othersource_pmt_mthd_cnt = PTP_data_othersource["pmt_mthd_cd"].value_counts().to_dict()
    
    othersource_extention_mean = PTP_data_othersource["promised_extention"].mean()
    othersource_extention_median = PTP_data_othersource["promised_extention"].quantile(q=0.5)
    
    #EDIT (12/04) Adding parameters for extention when Promises are kept
    othersource_kptprom_extention_mean = othersource_keptpromise["promised_extention"].mean()
    othersource_kptprom_extention_median = othersource_keptpromise["promised_extention"].quantile(q=0.5)
    
    othersource_brkprom_extention_mean = othersource_brokenpromise["promised_extention"].mean()
    othersource_brkprom_extention_median = othersource_brokenpromise["promised_extention"].quantile(q=0.5)
    
    REP_absent["Total Calls"] = othersource_calls
    REP_absent["Calls Kept Promise"] = othersource_keptpromise_calls
    REP_absent["Calls Broken Promise"] = othersource_brokenpromise_calls
    REP_absent["Mean amount"] = othersource_amount
    REP_absent["Kept Promise amount"] = othersource_keptpromise_amount
    REP_absent["Broken Promise amount"] = othersource_brokenpromise_amount
    REP_absent["Single installments"] = single_installments_othersource
    REP_absent["Multiple installments"] = multiple_installments_othersource
    REP_absent["Payment Method Count"] = othersource_pmt_mthd_cnt
    REP_absent["Mean extention"] = othersource_extention_mean
    REP_absent["Median extention"] = othersource_extention_median
    REP_absent["Kept Promise extention"] = othersource_kptprom_extention_mean
    REP_absent["Broken Promise extention"] = othersource_brkprom_extention_mean
    
    REP_analysis["{}_absent".format(val)] = REP_absent
    
    return REP_analysis

def PAR_DEL_analysis(PAR_data, PTP_data):
    """
    Analysis on Payment behaviour for delinquent vs non-delinquent customers
    Arguments:
        PAR_data: Pandas Dataframe, processed PAR data
        PTP_data: Pandas Dataframe, raw PTP data
    Returns:
        DEL_analysis: dict
    """
    PTP_data = PTP_data.applymap(lambda x: x.strip() if type(x) == str else x)

    DEL_analysis = {}
    
    DEL_present = {}
    
    DEL_calls = PAR_data[PAR_data["TOTAL_DELINQUENT_AMOUNT"] > 0]
    DEL_im_amount = DEL_calls["MINIMUM_IMMEDIATE_PAYMENT_AMOUNT"].mean()
    
    get_date = lambda x: x.date()
    DEL_calls["PAYMENT_ARRANGEMENT_RECOMMENDATION_DATE_TIME_date"] = DEL_calls["PAYMENT_ARRANGEMENT_RECOMMENDATION_DATE_TIME_datetime"].apply(get_date)
    
    PTP_data["PROMISE_MADE_DATE_date"] = pd.to_datetime(PTP_data["PROMISE_MADE_DATE"]).apply(get_date)
    
    DEL_complete = DEL_calls.merge(PTP_data, how = "inner", left_on = ["BAN","PAYMENT_ARRANGEMENT_RECOMMENDATION_DATE_TIME_date"], right_on = ["ban", "PROMISE_MADE_DATE_date"])
    
    if DEL_complete.shape[0] == 0:
        print ("Merge failed, going into pdb mode")
        pdb.set_trace()
    
    DEL_REP_count = DEL_complete[DEL_complete["PA_Source"] == "REP"].shape[0]
    DEL_singleinstallment_count = DEL_complete[DEL_complete["pln_type_cd"] == 1].shape[0]
    DEL_multipleinstallments_count = DEL_complete[DEL_complete["pln_type_cd"] > 1].shape[0] 
    DEL_keptpromise_count = DEL_complete[DEL_complete["pmt_pln_dspt_cd"] == "KPTPROM"].shape[0]
    DEL_brokenpromise_count = DEL_complete[DEL_complete["pmt_pln_dspt_cd"] == "BRKPROM"].shape[0]                                   
                                   
    DEL_pmt_mthd_cnt = DEL_complete["pmt_mthd_cd"].value_counts().to_dict()
                                   
    DEL_present["Immediate Amount"] = DEL_im_amount
    DEL_present["Representative calls"] = DEL_REP_count
    DEL_present["Single Installments"] = DEL_singleinstallment_count
    DEL_present["Multiple Installments"] = DEL_multipleinstallments_count
    DEL_present["Kept Promise"] = DEL_keptpromise_count
    DEL_present["Broken Promise"] = DEL_brokenpromise_count
    DEL_present["Payment Method count"] = DEL_pmt_mthd_cnt
                                   
    DEL_analysis["DEL_present"] = DEL_present
    
    DEL_absent = {}
                                   
    non_DEL_calls = PAR_data[PAR_data["TOTAL_DELINQUENT_AMOUNT"] <= 0]
    non_DEL_im_amount = non_DEL_calls["MINIMUM_IMMEDIATE_PAYMENT_AMOUNT"].mean()
    
    non_DEL_calls["PAYMENT_ARRANGEMENT_RECOMMENDATION_DATE_TIME_date"] = non_DEL_calls["PAYMENT_ARRANGEMENT_RECOMMENDATION_DATE_TIME_datetime"].apply(get_date)
    
    non_DEL_complete = non_DEL_calls.merge(PTP_data, how = "inner", left_on = ["BAN", "PAYMENT_ARRANGEMENT_RECOMMENDATION_DATE_TIME_date"], right_on = ["ban", "PROMISE_MADE_DATE_date"])
    
    if non_DEL_complete.shape[0] == 0:
        print ("Merge failed, going into pdb mode")
        pdb.set_trace()
    
    PTP_data.drop("PROMISE_MADE_DATE_date", axis=1, inplace=True)
    
    non_DEL_REP_count = non_DEL_complete[non_DEL_complete["PA_Source"] == "REP"].shape[0]
    non_DEL_singleinstallment_count = non_DEL_complete[non_DEL_complete["pln_type_cd"] == 1].shape[0]
    non_DEL_multipleinstallments_count = non_DEL_complete[non_DEL_complete["pln_type_cd"] > 1].shape[0]
    non_DEL_keptpromise_count = non_DEL_complete[non_DEL_complete["pmt_pln_dspt_cd"] == "KPTPROM"].shape[0]
    non_DEL_brokenpromise_count = non_DEL_complete[non_DEL_complete["pmt_pln_dspt_cd"] == "BRKPROM"].shape[0]
                                   
    non_DEL_pmt_mthd_cnt = non_DEL_complete["pmt_mthd_cd"].value_counts().to_dict()
                                   
    DEL_absent["Immediate Amount"] = non_DEL_im_amount
    DEL_absent["Representative calls"] = non_DEL_REP_count
    DEL_absent["Single Installments"] = non_DEL_singleinstallment_count
    DEL_absent["Multiple Installments"] = non_DEL_multipleinstallments_count
    DEL_absent["Kept Promise"] = non_DEL_keptpromise_count
    DEL_absent["Broken Promise"] = non_DEL_brokenpromise_count
    DEL_absent["Payment Method Count"] = non_DEL_pmt_mthd_cnt
                                           
    DEL_analysis["DEL_absent"] = DEL_absent
    
    return DEL_analysis

def PAR_EXT_analysis(PAR_data, PTP_data, **kwargs):
    if "col_file" in kwargs.keys():
        col_file = kwargs["col_file"]
        col_dict = common_utils.text_to_dict(col_file)
    
    PAR_analysis = {}
    label_col = col_dict["label_col"][0]
    cols_to_dummies = col_dict["cols_to_dummies"] 
    get_sign_flag = lambda x: 0 if x <= 0 else 1
    
    PTP_data = PTP_data.applymap(lambda x: x.strip() if type(x) == str else x)

    ### Getting labels for analysis
    print ("shape of PAR data before merge: {}".format(PAR_data.shape))
    PAR_labels = feature_utils.get_PAR_ratio(PAR_data, PTP_data, subset=False)
    print ("shape of PAR data before merge: {}".format(PAR_labels.shape))
    
    #EDIT (11/06) Analysing data only for kept promises
    PAR_labels = PAR_labels[PAR_labels["pmt_pln_dspt_cd"] == "KPTPROM"]
    print (PAR_labels.shape)
    PAR_labels = pd.get_dummies(PAR_labels,prefix=cols_to_dummies,columns=cols_to_dummies)
    
    PAR_labels = PAR_labels.replace([np.inf, -np.inf], np.nan)
    #PAR_labels.fillna(0, inplace=True)
    
#TODO, need to handle payment disposition code
    for analysis_col in col_dict["analysis_cols_cat"]:
        if analysis_col == "TOTAL_DELINQUENCY_AMOUNT":
            PAR_labels["DEL_FLAG"] = PAR_labels[analysis_col].apply(get_sign_flag)
            PAR_subset = PAR_labels[["DEL_FLAG", label_col]]
            PAR_analysis["DEL_analysis"] = common_utils.get_analysis(PAR_subset, label_col=label_col, analysis_col="DEL_FLAG")
            PAR_labels.drop("DEL_FLAG", axis=1, inplace=True)
        
        relevant_cols = [col for col in PAR_labels.columns if analysis_col in col]
        
        for relevant_col in relevant_cols:
            PAR_subset = PAR_labels[[relevant_col, label_col]]
            col_analysis = common_utils.get_analysis(PAR_subset, label_col=label_col, analysis_col=relevant_col)
            PAR_analysis["{}_analysis".format(relevant_col)] = col_analysis
    
    #We split the extention_ratio into 4 quantiles, and then analyse the following columns according to value taken by them
    qcut_num = 4
    if "qcut" in kwargs.keys():
        qcut_num = kwargs["qcut"]
    
    quantile_col = "{}_quantile".format(label_col)
    PAR_labels[quantile_col] = pd.qcut(PAR_labels[label_col], q=qcut_num, labels=False)
    
    for numeric_col in col_dict["analysis_cols_num"]:
        if len(numeric_col) > 0: 
            PAR_subset = PAR_labels[[numeric_col, quantile_col]]
            PAR_analysis["{}_analysis".format(numeric_col)] = common_utils.get_analysis(PAR_subset, label_col=numeric_col,
                                                                                        analysis_col=quantile_col, num_col=True)
        
    return PAR_analysis

def explore_PAR(PAR_data, PTP_data, results_loc="../data_prep/PAR_analysis", rep_analysis=False, del_analysis=False, ext_analysis=False, **kwargs):
    """
    Gets the split for PAR data to understand how different paramters affect customer-agent interactions 
    Arguments:
        PAR_data: Pandas Dataframe, Processed PAR Recommender data
        PTP_data: Pandas Dataframe, Raw PTP data
        results_loc: str, place to save results
        rep_analysis: boolean, flag to carry out representative level analysis 
        del_analysis: boolean, flag to carry out delinquency level analysis
        ext_analysis: boolean, flag to carry out extention ratio-level analysis
    Returns: 
        None
    """
    #Representative vs other channels' analysis
    if rep_analysis:
        for val in pd.unique(PTP_data["PA_Source"]):
            REP_analysis = PTP_Source_analysis(PTP_data, val)
            REP_filename = "{}_analysis.json".format(val)
            common_utils.write_results(REP_analysis, REP_filename, results_loc, csv_mode = True)
            
    #Delinquency Analysis
    if del_analysis:
        DEL_analysis = PAR_DEL_analysis(PAR_data, PTP_data)
        DEL_filename = "delinquent_analysis.json"
        common_utils.write_results(DEL_analysis, DEL_filename, results_loc, csv_mode = True)
        
    #EDIT (11/05) Analysis on extention ratio, added on the basis of Nancy/Sid meeting
    if ext_analysis:
        EXT_analysis = PAR_EXT_analysis(PAR_data, PTP_data, **kwargs)
        EXT_filename = "extention_analysis.json"
        common_utils.write_results(EXT_analysis, EXT_filename, results_loc, csv_mode = True)
    
    return True