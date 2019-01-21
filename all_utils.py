#------------------------------------------------------------------------------
# Data Pre-Processing 
#------------------------------------------------------------------------------
# 1. Display
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
def display_set():
    pd.options.display.max_rows = 4000
    pd.set_option('display.float_format', lambda x: '%.2f' % x)

#------------------------------------------------------------------------------
# Data Pre-Processing 
#------------------------------------------------------------------------------
# 1. Nulls 
# 2. Column Names with blanks 
# 3. Dates to date format 
# 4. Remove duplicates 
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

# All columns with date 
def conv_all_dates(df):
    dates_indicator = 'DATE'
    for col in df.columns.tolist():
        if (dates_indicator in col):
            df = to_date(df, col)

def to_date(df,col):
    df[col] = pd.to_datetime(df[col],errors = 'coerce')
    return df

def duplicated_values(df,col):
    duplicated = df.loc[df.duplicated(subset = col, keep = False)]
    return (duplicated) 

#def remove_blanks(df):
#    return [p.replace(' ', '_') for p df.columns.tolist()]

#------------------------------------------------------------------------------
# Data Analysis
#------------------------------------------------------------------------------
# 1. EDA 
# 2. Overview 
# 3.  
# 4.  
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Find exploratory characteristics of a column
#------------------------------------------------------------------------------
def overview(col):
    print ('Min: ', df[col].min())
    print ('Max: ', df[col].max())
    #print ('Mean: ', df[col].mean())
    print (df[col].value_counts()/len(df))
    print (df[col].value_counts())

def outlier(df,col,range_):
    print (len(df.loc[(df[col] > range_) | (df[col] < -range_)]))
    
#------------------------------------------------------------------------------
# Compare two files and find the difference in columns 
#------------------------------------------------------------------------------
def comp_cols(f1,f2):
    df1 = pd.read_csv(f1, sep ='|', low_memory=False)
    df2 = pd.read_csv(f2, sep ='|', low_memory=False)
    f1_cols = set(df1.columns.tolist())
    f2_cols = set(df2.columns.tolist())
#     union = f1_cols.union(f2_cols)
#     intersection = f1_cols.intersection(f2_cols)
    diff = f2_cols.difference(f1_cols)
    del(df1)
    del(df2)
    return diff

#---------------------------------- Method to create EDA files -------------------------------------
# Inputs - list of file, index of filename i.e. the position in the path where the filename resides
# Output - csv files in the target folder with EDA on each file
#----------------------------------------------------------------------------------------------------

def writeToFile_EDA(files, locFilename, targetFolder):
    for f in files:
        print(f)
        df = pd.read_csv(f, sep = '|')

        ### Populate column
        ### column name | sample value | any nulls | # of nulls | % of nulls 
        cols = df.columns.tolist()
        uniques = df.nunique()
        max_val = df.max()
        min_val = df.min()
        sample_value = df.values[0].tolist()
        is_null = df.isnull().any().tolist()
        num_nulls = df.isnull().sum(axis = 0)
        per_nulls = df.isnull().sum(axis = 0)*100/len(df)
        
        ### -- Zip together all columns
        rows = zip(cols,uniques,max_val,min_val,sample_value,is_null,num_nulls,per_nulls)

        # Extract file name from the file path
        start = '\\'    
        end = '.'
        filename = (f.split(start))[locFilename].split(end)[0]

        newfilePath = targetFolder + filename  + '_EDA.csv'
        
        header = ['Columns', 'Unique #', 'Max', 'Min', 'Sample', \
                 'Null?', 'Null #', 'Null %']

        # Write to the EDA file
        with open(newfilePath, "w+") as file:
            writer = csv.writer(file)
            writer.writerow(header)
            for row in rows:
                writer.writerow(row)
        del(df)
        
#_____________________________________________________________________________
# File Operations
# 1. Unzip all files  
# 2. Read text files 
# 3.  
# 4.  
#_____________________________________________________________________________

#------------------------------------------------------------------------------
# Unzip all files in a directory
#------------------------------------------------------------------------------
def zip_all_files_in_a_directory(path_to_zip_file,directory_to_extract_to):
    import zipfile
    # Loop to create names for all files in the directory 
    for i in range(1,32):
        path_to_zip_file_add = path_to_zip_file + str(i) + str('.zip')
        with zipfile.ZipFile(path_to_zip_file_add, 'r') as zip_ref:
            zip_ref.extractall(directory_to_extract_to)



#_____________________________________________________________________________
# Data Science
# 1. Calculate KS  
# 2.  
# 3.  
# 4.  
#_____________________________________________________________________________

# Bads = 1 and Goods = 0
def calc_ks(Y, pred):
    data = np.column_stack((Y, pred))
    data = data[data[:,1].argsort(),]
    flag_bads = data[:, 0]
    total_bads = flag_bads.sum()
    flag_goods = 1 - data[:, 0]
    total_goods = flag_goods.sum()
    bads_det = flag_bads.cumsum() / total_bads
    goods_det = flag_goods.cumsum() / total_goods
    ks_bad_good = goods_det -bads_det
    ks = ks_bad_good.max(0)
    return (ks)

