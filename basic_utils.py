import pandas as pd
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Formatting
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# 1. Display
#------------------------------------------------------------------------------
def display_set():
    pd.options.display.max_rows = 4000
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

#------------------------------------------------------------------------------
# Read/ Save/ Delete Files
#------------------------------------------------------------------------------
# 1. Read
#  - Input - filename with path of a text file
#  - Returns - data frame
#------------------------------------------------------------------------------
def read_txt(filename):
    df = pd.read_csv(filename, sep = '|', low_memory = False)
    return (df)
#------------------------------------------------------------------------------
# 2. Save
#  - Input - Dataframe, name
#  - Returns - saves dataframe in the path without index
#------------------------------------------------------------------------------
def save_as_csv(df, name):
    path = '../../0.Data/1.Interim/' + name + '.csv'
    df.to_csv(path, index = False)
#------------------------------------------------------------------------------
# 3. Delete a column in a dataframe
#  - Input - Dataframe, column to drop 
#  - Returns - Dataframe with dropped column
#------------------------------------------------------------------------------
def delete(df, col):
    df = df.drop(col, axis = 1)
    return(df)