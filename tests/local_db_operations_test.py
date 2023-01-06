from ..local_db_operations import read_local_data, rename_column_names
from os.path import exists

test_file = 'data/metadata.csv'
server_to_local_columns={'article_name': 'name', 
    'article_ID': 'ID', 
    'journal_name': 'journal'
}

def test_read_local_file_existence():
    actual = exists(test_file)
    expected = type(read_local_data(file_name=test_file)) != None
    
    assert actual == expected

def test_read_local_data_existence():
    actual = read_local_data(file_name= test_file).shape[0]
    unexpected = 0
    
    assert actual != unexpected

def test_read_local_data_column_names():
    expected = sorted(['article_ID', 'article_name', 'journal_name'])
    
    actual = sorted(list(read_local_data(file_name= test_file)))
    # actual_db_columns = sorted(actual.columns.to_list())
    
    assert actual == expected

def test_read_local_data_row_count():
    expected= 10
    
    actual = read_local_data(file_name= test_file).shape[0]
    
    assert actual == expected

def test_rename_column_names_existence():
    actual = exists(test_file)
    expected = type(rename_column_names(df=read_local_data(file_name= test_file), column_map=server_to_local_columns)) != None
    
    assert actual == expected

def test_rename_column_names():
    actual = sorted(['name', 'ID', 'journal'])
    expected = sorted(list(rename_column_names(df= read_local_data(file_name= test_file), column_map=server_to_local_columns)))
    
    assert actual == expected

