import pandas as pd
# import mysql.connector
from causaly_knowledge.tests.mock_db import MockDB
from causaly_knowledge.server_db_operations import query_db,fetch_all_data,fetch_all_active_duplicate_data, insert_data_to_server,update_server_data,add_local_data_to_server_db, perform_deduplication

db_name = 'testdb'
table_name = 'test_table'
local_to_server_columns={ 
    'uuid': 'article_uuid',
    'name': 'article_name', 
    'ID': 'article_ID', 
    'journal': 'journal_name',
    'article_status': 'citation_status',
    'article_version': 'citation_version',
    'operation_type': 'operation_type'
}

override_row_3= {
    'article_uuid': '27258656_OLDMEDLINE_1',
    'article_name': 'A new science of happiness: the paradox of pleasure',
    'article_ID':27258656,
    'journal_name': 'Ann N Y Acad Sci',
    'citation_status': 'OLDMEDLINE',
    'citation_version': '1',
    'operation_type': 'OVERRIDE'
}
active_row_1 = {
        'article_uuid': '27258656_MEDLINE_1',
        'article_name': 'A new science of happiness: the paradox of pleasure',
        'article_ID': 27258656,
        'journal_name': 'Ann N Y Acad Sci',
        'citation_status': 'MEDLINE',
        'citation_version': '1',
        'operation_type': 'ACTIVE'
    }
override_row_1 = {
        'article_uuid': '27258656_MEDLINE_1',
        'article_name': 'A new science of happiness: the paradox of pleasure',
        'article_ID': 27258656,
        'journal_name': 'Ann N Y Acad Sci',
        'citation_status': 'MEDLINE',
        'citation_version': '1',
        'operation_type': 'OVERRIDE'
    }
active_row_2 = {
        'article_uuid': '36343268_MEDLINE_1',
        'article_name': 'Wealth redistribution promotes happiness',
        'article_ID':36343268,
        'journal_name': 'Proc Natl Acad Sci U S A',
        'citation_status': 'MEDLINE',
        'citation_version': '1',
        'operation_type': 'ACTIVE'
    }
new_active_row_2 = {
        'article_uuid': '36343268_MEDLINE_2',
        'article_name': 'Wealth redistribution promotes happiness',
        'article_ID':36343268,
        'journal_name': 'Proc Natl Acad Sci U S A',
        'citation_status': 'MEDLINE',
        'citation_version': '2',
        'operation_type': 'ACTIVE'
    }
override_row_2 = {
        'article_uuid': '36343268_MEDLINE_1',
        'article_ID':36343268,
        'article_name': 'Wealth redistribution promotes happiness',
        'journal_name': 'Proc Natl Acad Sci U S A',
        'citation_status': 'MEDLINE',
        'citation_version': '1',
        'operation_type': 'OVERRIDE'
    }
active_row_4 = {
        'article_uuid': '31722899_In-Data-Review_2',
        'article_name': 'Prescriptions for happiness',
        'article_ID':31722899,
        'journal_name': 'Can Fam Physician',
        'citation_status': 'In-Data-Review',
        'citation_version': '2',
        'operation_type': 'ACTIVE'
    }
local_row_1 = {
        'uuid': '27258656_MEDLINE_1',
        'name': 'A new science of happiness: the paradox of pleasure',
        'ID':27258656,
        'journal': 'Ann N Y Acad Sci',
        'article_status': 'MEDLINE',
        'article_version': '1',
        'operation_type': '',
    }

local_row_2 = {
        'uuid': '36343268_MEDLINE_2',
        'name': 'Wealth redistribution promotes happiness',
        'ID':36343268,
        'journal': 'Proc Natl Acad Sci U S A',
        'article_status': 'MEDLINE',
        'article_version': '2',
        'operation_type': ''
    }

local_row_4 = {
        'uuid': '31722899_In-Data-Review_2',
        'name': 'Prescriptions for happiness',
        'ID':31722899,
        'journal': 'Can Fam Physician',
        'article_status': 'In-Data-Review',
        'article_version': '2',
        'operation_type': ''
    }
incoming_row_1 = {
        'uuid': '27258656_MEDLINE_1',
        'name': 'A new science of happiness: the paradox of pleasure',
        'ID':27258656,
        'journal': 'Ann N Y Acad Sci',
        'article_status': 'MEDLINE',
        'article_version': '1',
        'operation_type': 'ACTIVE',
    }
old_incoming_row_2 = {
        'uuid': '36343268_MEDLINE_1',
        'name': 'Wealth redistribution promotes happiness',
        'ID':36343268,
        'journal': 'Proc Natl Acad Sci U S A',
        'article_status': 'MEDLINE',
        'article_version': '1',
        'operation_type': 'ACTIVE'
    }
incoming_row_2 = {
        'uuid': '36343268_MEDLINE_2',
        'name': 'Wealth redistribution promotes happiness',
        'ID':36343268,
        'journal': 'Proc Natl Acad Sci U S A',
        'article_status': 'MEDLINE',
        'article_version': '2',
        'operation_type': 'ACTIVE'
    }

incoming_row_4 = {
        'uuid': '31722899_In-Data-Review_2',
        'name': 'Prescriptions for happiness',
        'ID':31722899,
        'journal': 'Can Fam Physician',
        'article_status': 'In-Data-Review',
        'article_version': '2',
        'operation_type': 'ACTIVE'
    }


def test_query_db():
    db = MockDB(db_name=db_name)
    db.setUpClass()
    expected = tuple(active_row_1.values())

    columns = ','.join([str(i) for i in list(active_row_1.keys())])
    insert_query = f'INSERT INTO {db_name}.{table_name} ({columns})  VALUES (' + '%s,'*(len(active_row_1)-1) + '%s)'
    input = list(active_row_1.values())
    query_db(db_name=db_name,input=input, query=insert_query)
    
    fetch_query = f'SELECT * FROM {db_name}.{table_name}'
    actual = query_db(db_name=db_name, query=fetch_query, return_results=True)[0]
    db.tearDownClass()
    
    assert expected == actual

def test_fetch_all_active_duplicate_data():
    db = MockDB(db_name=db_name)
    db.setUpClass()

    columns = list(active_row_1.keys())
    expected = pd.DataFrame([active_row_1,active_row_2], columns=columns)

    columns_str = ','.join([str(i) for i in list(active_row_1.keys())])
    insert_query = f'INSERT INTO {db_name}.{table_name} ({columns_str})  VALUES (' + '%s,'*(len(active_row_1)-1) + '%s)'
    input = [list(active_row_1.values()),list(active_row_2.values()),list(override_row_3.values())]
    query_db(db_name=db_name,input=input, query=insert_query, batch=True)
    
    actual = fetch_all_active_duplicate_data(incoming_data=expected, db_name=db_name, table_name=table_name, id_column_name='article_ID')
    db.tearDownClass()

    assert expected.equals(actual)

def test_fetch_all_data():
    db = MockDB(db_name=db_name)
    db.setUpClass()
    columns = list(active_row_1.keys())
    expected = pd.DataFrame([ active_row_1,override_row_3, active_row_2], columns=columns)

    columns_str = ','.join([str(i) for i in list(active_row_1.keys())])
    insert_query = f'INSERT INTO {db_name}.{table_name} ({columns_str})  VALUES (' + '%s,'*(len(active_row_1)-1) + '%s)'
    input = [list(active_row_1.values()),list(active_row_2.values()), list(override_row_3.values())]
    query_db(db_name=db_name,input=input, query=insert_query, batch=True)
    
    actual, _ = fetch_all_data(db_name=db_name, table_name=table_name, columns=columns)
    db.tearDownClass()

    assert expected.equals(actual)

def test_insert_data_to_server():
    db = MockDB(db_name=db_name)
    db.setUpClass()
    expected = [tuple(active_row_1.values()),tuple(override_row_3.values()), tuple(active_row_2.values())].sort()

    input = pd.DataFrame([ active_row_1,override_row_3, active_row_2], columns=list(active_row_1.keys()))
    insert_data_to_server(db_name=db_name, table_name=table_name, columns=input.columns, data=input)
    
    fetch_query = f'SELECT * FROM {db_name}.{table_name}'
    actual = query_db(db_name=db_name, query=fetch_query, return_results=True).sort()
    db.tearDownClass()
    
    assert expected == actual

def test_update_server_data():
    db = MockDB(db_name=db_name)
    db.setUpClass()
    expected = [tuple(override_row_1.values())].sort()

    input = pd.DataFrame([incoming_row_1], columns=list(active_row_1.keys()))
    insert_data_to_server(db_name=db_name, table_name=table_name, columns=input.columns, data=input)
    update_server_data(db_name=db_name, table_name=table_name, data=input, column_name_to_update='operation_type', id_column_name='article_ID', old_value='ACTIVE', updated_value='OVERRIDE')
    
    fetch_query = f'SELECT * FROM {db_name}.{table_name}'
    actual = query_db(db_name=db_name, query=fetch_query, return_results=True).sort()
    db.tearDownClass()

    assert expected == actual

def test_add_local_data_to_server_db():
    db = MockDB(db_name=db_name)
    db.setUpClass()
    expected = [tuple(active_row_1.values())].sort()

    input = pd.DataFrame([incoming_row_1], columns=list(local_row_1.keys()))
    add_local_data_to_server_db(db_name=db_name, table_name=table_name, local_to_server_columns=local_to_server_columns, data=input)
    
    fetch_query = f'SELECT * FROM {db_name}.{table_name}'
    actual = query_db(db_name=db_name, query=fetch_query, return_results=True).sort()
    db.tearDownClass()
    
    assert expected == actual

def test_perform_deduplication():
    db = MockDB(db_name=db_name)
    db.setUpClass()
    expected = [tuple(active_row_1.values()),tuple(new_active_row_2.values()),tuple(active_row_4.values()), tuple(override_row_2.values())].sort()

    input = pd.DataFrame([incoming_row_1, incoming_row_2], columns=list(local_row_1.keys()))
    add_local_data_to_server_db(db_name=db_name, table_name=table_name, local_to_server_columns=local_to_server_columns, data=input)

    incoming_data = pd.DataFrame([local_row_1, local_row_2, local_row_4], columns=list(local_row_1.keys()))
    perform_deduplication(incoming_data=incoming_data,db_name=db_name, table_name=table_name, local_to_server_columns=local_to_server_columns)
    
    fetch_query = f'SELECT * FROM {db_name}.{table_name}'
    actual = query_db(db_name=db_name, query=fetch_query, return_results=True).sort()
    db.tearDownClass()
    
    assert expected == actual


