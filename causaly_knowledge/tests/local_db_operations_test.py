from local_db_operations import read_local_data, rename_column_names, get_unique_rows, get_duplicate_rows,\
 get_latest_rows,keep_latest_unique_rows, add_empty_column, add_uuid_column, get_outdated_rows,get_filtered_data,\
    assign_new_column_value
from os.path import exists
import pandas as pd

test_file = '../test_data/metadata.csv'
test_file_with_duplicates = '../test_data/duplicates_metadata.csv'
test_file_with_duplicates_removed = '../test_data/dropped_all_duplicates_metadata.csv'
test_file_duplicates = '../test_data/duplicate_metadata.csv'
test_file_outdated = '../test_data/outdated_metadata.csv'
test_file_deduplicated = '../test_data/deduplicated_metadata.csv'
test_file_added_empty_column = '../test_data/metadata_added_empty_column.csv'
test_file_added_uuid_column = '../test_data/metadata_uuid_column.csv'
test_file_assign_new_value_column = '../test_data/duplicates_metadata_all_columns.csv'

server_to_local_columns={'article_name': 'name', 
    'article_ID': 'ID', 
    'journal_name': 'journal',
    'citation_status': 'article_status',
    'citation_version': 'article_version',
    'article_uuid': 'uuid',
    'operation_type': 'operation_type'
}

id_column_name = 'article_ID'
condition_1_column_name = 'citation_version'
condition_2_column_name = 'citation_status'
condition_2_dict= {
  'In-Data-Review':     1,
  'In-Process':         2, 
  'MEDLINE':            5,
  'OLDMEDLINE':         0, 
  'PubMed-not-MEDLINE': 4, 
  'Publisher':          1
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

def test_get_unique_rows_existence():
    actual = exists(test_file)
    expected = type(get_unique_rows(df=read_local_data(file_name= test_file), id_column_name=id_column_name)) != None
    
    assert actual == expected

def test_get_unique_rows():
    actual = read_local_data(file_name= test_file_with_duplicates_removed)
    expected = get_unique_rows(df=read_local_data(file_name= test_file_with_duplicates), id_column_name=id_column_name)
    print(actual.reset_index(drop=True))
    print(expected.reset_index(drop=True))
    assert actual.reset_index(drop=True).equals(expected.reset_index(drop=True))

def test_get_duplicate_rows_existence():
    actual = exists(test_file)
    expected = type(get_duplicate_rows(df=read_local_data(file_name= test_file), id_column_name=id_column_name)) != None
    
    assert actual == expected

def test_get_duplicate_rows():
    actual = read_local_data(file_name= test_file_duplicates)
    expected = get_duplicate_rows(df=read_local_data(file_name= test_file_with_duplicates), id_column_name=id_column_name)

    assert actual.reset_index(drop=True).equals(expected.reset_index(drop=True))

def test_get_outdated_rows_existence():
    actual = exists(test_file_with_duplicates)
    expected = type(get_outdated_rows(df=read_local_data(file_name= test_file_with_duplicates), id_column_name=id_column_name, condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name, condition_2_dict=condition_2_dict)) != None
    
    assert actual == expected

def test_get_outdated_rows():
    actual = read_local_data(file_name= test_file_outdated).sort_values(by=id_column_name)
    expected = get_outdated_rows(df=read_local_data(file_name= test_file_with_duplicates), id_column_name=id_column_name, condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name, condition_2_dict=condition_2_dict).sort_values(by=id_column_name)
  
    assert actual.reset_index(drop=True).equals(expected.reset_index(drop=True))

def test_get_latest_rows_existence():
    actual = exists(test_file_with_duplicates)
    expected = type(get_latest_rows(df=read_local_data(file_name= test_file_with_duplicates), id_column_name=id_column_name, condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name, condition_2_dict=condition_2_dict)) != None
    
    assert actual == expected

def test_get_latest_rows():
    actual = read_local_data(file_name= test_file_deduplicated).sort_values(by=id_column_name)
    expected = get_latest_rows(df=read_local_data(file_name= test_file_with_duplicates), id_column_name=id_column_name, condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name, condition_2_dict=condition_2_dict).sort_values(by=id_column_name)
    
    assert actual.reset_index(drop=True).equals(expected.reset_index(drop=True))

def test_keep_latest_unique_rows_existence():
    actual = exists(test_file_with_duplicates)
    expected = type(keep_latest_unique_rows(df=read_local_data(file_name= test_file_with_duplicates), id_column_name=id_column_name, condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name, condition_2_dict=condition_2_dict)) != None
    
    assert actual == expected

def test_keep_latest_unique_rows():
    actual = read_local_data(file_name= test_file_deduplicated).sort_values(by=id_column_name)
    expected = keep_latest_unique_rows(df=read_local_data(file_name= test_file_with_duplicates), id_column_name=id_column_name, condition_1_column_name=condition_1_column_name, condition_2_column_name=condition_2_column_name, condition_2_dict=condition_2_dict).sort_values(by=id_column_name)
    
    assert actual.reset_index(drop=True).equals(expected.reset_index(drop=True))

def test_get_filtered_data():
    actual = [35135239, 35135239]
    expected = get_filtered_data(data=read_local_data(file_name= test_file_with_duplicates), column_name='article_ID', column_value=35135239)['article_ID'].values.tolist()

    assert actual == expected

def test_assign_new_column_value():
    actual = [2]*15
    expected = assign_new_column_value(data=read_local_data(file_name= test_file_assign_new_value_column), column_name='citation_version', old_column_value=1, new_column_value=2)['citation_version'].values.tolist()

    assert actual == expected

def test_add_empty_column_existence():
    actual = exists(test_file_with_duplicates)
    expected = type(add_empty_column(df=read_local_data(file_name= test_file_with_duplicates), column_name=id_column_name)) != None
    
    assert actual == expected

def test_add_empty_column():
    actual = read_local_data(file_name=test_file_added_empty_column)['article_uuid'].empty
    expected = add_empty_column(df=read_local_data(file_name= test_file), column_name='article_uuid')['article_uuid'].empty

    assert actual == expected

def test_add_uuid_column_existence():
    actual = exists(test_file_added_uuid_column)
    expected = type(add_uuid_column(df=read_local_data(file_name= test_file_with_duplicates),  id_column_name=id_column_name, uuid_column_name='article_uuid', status_column_name=condition_2_column_name, version_column_name=condition_1_column_name)) != None
    
    assert actual == expected

def test_add_uuid_column():
    actual = read_local_data(file_name=test_file_added_uuid_column)
    expected = add_uuid_column(df=read_local_data(file_name= test_file_deduplicated),  id_column_name=id_column_name, uuid_column_name='article_uuid', status_column_name=condition_2_column_name, version_column_name=condition_1_column_name)
    print(actual)
    print(expected)
    assert actual.equals(expected)