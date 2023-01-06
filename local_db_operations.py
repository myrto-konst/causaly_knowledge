import pandas as pd

def read_local_data(file_name):
    return pd.read_csv(file_name)

def rename_column_names(df, column_map):    
    return df.rename(columns=column_map)
