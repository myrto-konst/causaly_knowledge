# from unittest.mock import MagicMock
# import pandas as pd
# import mysql.connector

# from causaly_knowledge.server_db_operations import insert_row_to_server, compare_articles

# db_name = 'test_data'
# table_name = 'metadata'
# local_to_server_columns={ 
#     'name': 'article_name', 
#     'ID': 'article_ID', 
#     'journal': 'journal_name',
#     'article_status': 'citation_status',
#     'article_version': 'citation_version',
#     'uuid': 'article_uuid',
#     'operation_type': 'operation_type'
# }

# existing_row_1_overridden= {
#     'article_ID':27258656,
#     'article_name': 'A new science of happiness: the paradox of pleasure',
#     'journal_name': 'Ann N Y Acad Sci',
#     'citation_status': 'MEDLINE',
#     'citation_version': '1',
#     'article_uuid': '27258656_MEDLINE_1',
#     'operation_type': 'OVERRIDE'
# }
# existing_row_1 = {
#         'article_ID':27258656,
#         'article_name': 'A new science of happiness: the paradox of pleasure',
#         'journal_name': 'Ann N Y Acad Sci',
#         'citation_status': 'MEDLINE',
#         'citation_version': '1',
#         'article_uuid': '27258656_MEDLINE_1',
#         'operation_type': 'ACTIVE'
#     }
# existing_row_2 = {
#         'article_ID':27258656,
#         'article_name': 'A new science of happiness: the paradox of pleasure',
#         'journal_name': 'Ann N Y Acad Sci',
#         'citation_status': 'In-Process',
#         'citation_version': '1',
#         'article_uuid': '27258656_In-Process_1',
#         'operation_type': 'ACTIVE'
#     }
# existing_row_3 = {
#         'article_ID':27258656,
#         'article_name': 'A new science of happiness: the paradox of pleasure',
#         'journal_name': 'Ann N Y Acad Sci',
#         'citation_status': 'MEDLINE',
#         'citation_version': '2',
#         'article_uuid': '27258656_MEDLINE_1',
#         'operation_type': 'ACTIVE'
#     }
# incoming_row = {
#         'article_ID':27258656,
#         'article_name': 'A new science of happiness: the paradox of pleasure',
#         'journal_name': 'Ann N Y Acad Sci',
#         'citation_status': 'MEDLINE',
#         'citation_version': '1',
#         'article_uuid': '27258656_MEDLINE_1',
#         'operation_type': 'ACTIVE'
#     }
# updated_row = {
#         'article_ID':27258656,
#         'article_name': 'A new science of happiness: the paradox of pleasure',
#         'journal_name': 'Ann N Y Acad Sci',
#         'citation_status': 'MEDLINE',
#         'citation_version': '1',
#         'article_uuid': '27258656_MEDLINE_1',
#         'operation_type': 'OVERRIDE'
#     }
# incoming_data = {
#         'article_ID':[27258656],
#         'article_name':[ 'A new science of happiness: the paradox of pleasure'],
#         'journal_name':[ 'Ann N Y Acad Sci'],
#         'citation_status':[ 'MEDLINE'],
#         'citation_version':[ '1'],
#         'article_uuid':[ '27258656_MEDLINE_1'],
#         'operation_type': ['ACTIVE']
#     }

# # def append_row(mock_cursor, row):
# #     if row not in mock_cursor.rows:
# #         mock_cursor.rows.append(row) 
# #         return mock_cursor.rows
# #     else: 
# #         return [mysql.connector.errors.IntegrityError]

# # def insert_row(mock_cursor, row):
# #     mock_cursor.configure_mock(
# #         **{
# #         "fetch_all": lambda: append_row(mock_cursor=mock_cursor, row=row)
# #     })

# # # def update_row(mock_cursor, query):
# # #     mock_cursor.configure_mock(
# # #         **{
# # #         "fetch_all": lambda query: how to extract info from query? is there a better way?
# # #     })

# # def generate_mock_connection():
# #     mock_connection = MagicMock()
# #     mock_connection.configure_mock(
# #         **{
# #             'commit': lambda : print("Mock Commit!")
# #     })

# #     return mock_connection

# # def test_insert_row_to_server_successful():
# #     expected = tuple(pd.Series(incoming_row))

# #     mock_cursor = MagicMock()
# #     mock_cursor.configure_mock(
# #     **{
# #         "rows": [],
# #         "execute": lambda sql, row: insert_row(mock_cursor=mock_cursor, row=row)

# #     }) 
# #     mock_connection = generate_mock_connection()
    
# #     insert_row_to_server(db_name=db_name, table_name=table_name, columns=local_to_server_columns, row=pd.Series(incoming_row), connection=mock_connection, cursor=mock_cursor)
# #     actual = mock_cursor.fetch_all()[-1]

# #     assert expected == actual

# # def test_insert_row_to_server_duplicate():
# #     expected = mysql.connector.errors.IntegrityError
# #     # mock connection and cursor
# #     mock_cursor = MagicMock()
# #     mock_cursor.configure_mock(
# #     **{
# #         "rows": [tuple(pd.Series(incoming_row))],
# #         "execute": lambda sql, row: insert_row(mock_cursor=mock_cursor, row=row)

# #     }) 
    
# #     mock_connection = generate_mock_connection()

# #     insert_row_to_server(db_name=db_name, table_name=table_name, columns=local_to_server_columns, row=pd.Series(incoming_row), connection=mock_connection, cursor=mock_cursor)
# #     actual = mock_cursor.fetch_all()[-1]

# #     assert expected == actual

# # # not sure how to extract info from query in order to mock it
# # def test_update_row_in_server_successful():
# #     expected = tuple(pd.Series(existing_row_1_overridden))


# # # cursor and connection are instantiated within the function - can't mock SAME WITH 
# # # def test_add_local_data_to_server_db_successful():
# # #     expected = [tuple(pd.Series(incoming_row)),tuple(pd.Series(existing_row))]
    
# # #     add_local_data_to_server_db(db_name=db_name, table_name=table_name, local_to_server_columns=local_to_server_columns, data=pd.DataFrame.from_dict(incoming_data), server_credentials={'user': 'root', 'password':'rootroot'})

# # #     assert expected.sort() == expected.sort()

# # def test_compare_articles_incoming_new():
# #     expected = 0
# #     actual = compare_articles(matched_article=existing_row_2, incoming_article=incoming_row)

# #     assert expected == actual

# # def test_compare_articles_incoming_exists():
# #     expected = 1
# #     actual = compare_articles(matched_article=existing_row_1, incoming_article=incoming_row)

# #     assert expected == actual

# # def test_compare_articles_incoming_overriden():
# #     expected = 2
# #     actual = compare_articles(matched_article=existing_row_3, incoming_article=incoming_row)

# #     assert expected == actual

