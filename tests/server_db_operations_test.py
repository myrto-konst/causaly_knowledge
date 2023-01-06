from unittest.mock import MagicMock
import pandas as pd
import mysql.connector

from server_db_operations import insert_row_to_server

db_name = 'causaly_knowledge'
table_name = 'metadata'
local_to_server_columns={ 
    'name': 'article_name', 
    'ID': 'article_ID', 
    'journal': 'journal_name'
}
new_row = {
        'article_ID':27258656,
        'article_name': 'A new science of happiness: the paradox of pleasure',
        'journal_name': 'Ann N Y Acad Sci'
    }

def insert_row(mock_cursor, row):
    mock_cursor.configure_mock(
        **{
        "fetch_all": lambda: [row] if row not in mock_cursor.rows else [mysql.connector.errors.IntegrityError]
    })

def generate_mock_connection():
    mock_connection = MagicMock()
    mock_connection.configure_mock(
        **{
            'commit': lambda : print("Mock Commit!")
    })

    return mock_connection

def test_insert_row_to_server_successful():
    expected = tuple(pd.Series(new_row))

    mock_cursor = MagicMock()
    mock_cursor.configure_mock(
    **{
        "rows": [],
        "execute": lambda sql, row: insert_row(mock_cursor=mock_cursor, row=row)

    }) 
    mock_connection = generate_mock_connection()
    
    insert_row_to_server(db_name=db_name, table_name=table_name, columns=local_to_server_columns, row=pd.Series(new_row), connection=mock_connection, cursor=mock_cursor)
    actual = mock_cursor.fetch_all()[-1]

    assert expected == actual

def test_insert_row_to_server_duplicate():
    expected = mysql.connector.errors.IntegrityError
    # mock connection and cursor
    mock_cursor = MagicMock()
    mock_cursor.configure_mock(
    **{
        "rows": [tuple(pd.Series(new_row))],
        "execute": lambda sql, row: insert_row(mock_cursor=mock_cursor, row=row)

    }) 
    
    mock_connection = generate_mock_connection()

    insert_row_to_server(db_name=db_name, table_name=table_name, columns=local_to_server_columns, row=pd.Series(new_row), connection=mock_connection, cursor=mock_cursor)
    actual = mock_cursor.fetch_all()[-1]

    assert expected == actual