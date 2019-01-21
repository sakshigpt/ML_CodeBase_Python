import pandas as pd
#------------------------------------------------------------------------------
# Function 1 - Counting number of occurences and flags
#------------------------------------------------------------------------------
# Input: dataframe and col. will give counts and flags of all values of a given column
# Returns: A dataframe with new columns for each unique row (per BAN)
#
# Note: Input dataframe should not have duplicates 
#------------------------------------------------------------------------------
# Version 1 - Original 
#------------------------------------------------------------------------------
def counts_and_flags(df,col):
    
    # Create count column names 
    col_names = ['BAN'] + [(i + '_count') for i in df[col].unique()] + [(i + '_flag') for i in df[col].unique()]
    
    # Populate columns
    
    # -- variable init
    col_values = df[col].unique()                                   # Unique column values
    value_dict = {key: [] for key in col_names}                     # Dictionary to hold the values for unique BANS
    
    for ban in df['ban'].unique():
        ban_df = df.loc[df.ban == ban]
        val = ban_df[col].value_counts()
        value_dict['BAN'].append(ban)
        for i in col_values:
            try:
                value = val[i]
                value_dict[i + '_count'].append(value)
                value_dict[i + '_flag'].append(int(1))
            except:
                value = 0
                value_dict[i + '_count'].append(value)
                value_dict[i + '_flag'].append(int(0))
    counts_and_flags = pd.DataFrame.from_dict(value_dict)
    return (counts_and_flags)
#------------------------------------------------------------------------------
# Version 2 - Added '_init' before counts and columns of initial version 
#------------------------------------------------------------------------------
def init_counts_and_flags(df,col):
    
    # Create count column names 
    col_names = ['BAN'] + [(i + '_init_count') for i in df[col].unique()] + [(i + '_init_flag') for i in df[col].unique()]
    
    # Populate columns
    
    # -- variable init
    col_values = df[col].unique()                                   # Unique column values
    value_dict = {key: [] for key in col_names}                     # Dictionary to hold the values for unique BANS
    
    for ban in df['ban'].unique():
        ban_df = df.loc[df.ban == ban]
        val = ban_df[col].value_counts()
        value_dict['BAN'].append(ban)
        for i in col_values:
            try:
                value = val[i]
                value_dict[i + '_init_count'].append(value)
                value_dict[i + '_init_flag'].append(int(1))
            except:
                value = 0
                value_dict[i + '_init_count'].append(value)
                value_dict[i + '_init_flag'].append(int(0))
    counts_and_flags = pd.DataFrame.from_dict(value_dict)
    return (counts_and_flags)
#------------------------------------------------------------------------------
# Version 3 
#    - Populate counts and flags when null values are present 
#    - Added '_s' after counts and columns of initial version 
#    - Added counts and flags for non-null values
#------------------------------------------------------------------------------
def subtype_counts(df):
    col_values = df['subtyp_pmt_mthd_cd'].unique()  
    col_values = col_values[~pd.isnull(col_values)]
    second_inslm_cols = ['BAN', 'second_inslm_flag', 'second_inslm_count'] + [(i + '_count_s') for i in col_values] + [(i + '_flag_s') for i in col_values]
    value_dict = {key: [] for key in second_inslm_cols} 
    for ban in df['ban'].unique():
        ban_df = df.loc[df['ban'] == ban]
        count = ban_df['subtyp_start_dt'].notnull().sum(axis =0)                  # Count non null dates
        value_dict['BAN'].append(ban)
        if(count):                                                                # Non-Zero
            val = ban_df['subtyp_pmt_mthd_cd'].value_counts()                     # All values of subtyp_pmt_mthd_cd
            value_dict['second_inslm_flag'].append(int(1))
            value_dict['second_inslm_count'].append(ban_df['subtyp_start_dt'].nunique())
            for i in col_values:
                try:
                    value = int(val[i])
                    value_dict[i + '_count_s'].append(value)
                    value_dict[i + '_flag_s'].append(int(1))
                except:
                    value = 0
                    value_dict[i + '_count_s'].append(value)
                    value_dict[i + '_flag_s'].append(int(0))
        else:
            for k in second_inslm_cols[1:]:
                value_dict[k].append(None)
        del(ban_df)
    subtype_counts = pd.DataFrame.from_dict(value_dict)
    return(subtype_counts)




#------------------------------------------------------------------------------
# Function 2 - Adding unique counts 
#------------------------------------------------------------------------------
# Input: dataframe, unique ID and (optional) col for which unique value should be caculated
# Returns: A dataframe with new column for each unique/ consensed row (per BAN)
#
# Note: Input dataframe should not have duplicates 
#------------------------------------------------------------------------------
# Version 1 - Original 
#------------------------------------------------------------------------------
def add_counts(df, uid):
    tot_counts = []
    for ban in df[uid].unique():
        tot_counts.append((ban, len(df.loc[df[uid] == ban])))
    df_w_counts = pd.DataFrame(tot_counts, columns = [uid, 'TOT_PROM'])
    return (df_w_counts)
#------------------------------------------------------------------------------
# Version 2 - Calculates the unique number of entries for any column
#------------------------------------------------------------------------------
def add_day_counts(df, uid, col_name):
    tot_counts = []
    for ban in df[uid].unique():
        ban_df = df.loc[df[uid] == ban]
        tot_counts.append((ban, ban_df[col_name].nunique()))
    df_w_day_counts = pd.DataFrame(tot_counts, columns = [uid, col_name + '_count'])
    return (df_w_day_counts)



#------------------------------------------------------------------------------
# Function 3 - Calculating change in two columns for the same row.
#------------------------------------------------------------------------------
# Input: dataframe, column 1 and column 2
# Returns: A dataframe with new column for # of time value was unchanged
#
# Note: Input dataframe should not have duplicates 
#------------------------------------------------------------------------------
# Version 1 - Original 
#------------------------------------------------------------------------------
def promise_change_cnt(df,col1,col2):
    promise_match_counts = []
    for ban in df['ban'].unique():
        ban_df = df.loc[df['ban'] == ban]
        count = 0
        for index, row in ban_df.iterrows():
            if(row[col1] == row[col2]):
                count = count +1
        promise_match_counts.append((ban, count))
        del(ban_df)
    promise_change = pd.DataFrame(promise_match_counts, columns=['BAN', 'promise_unchanged_cnt'])
    return (promise_change)
