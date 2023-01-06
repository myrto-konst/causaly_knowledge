import mysql.connector
from local_db_operations import rename_column_names
from constants import server_credentials


def insert_row_to_server(db_name, table_name, columns, row, cursor, connection):
    sql = f'INSERT INTO {db_name}.{table_name} ({columns})  VALUES (' + '%s,'*(len(row)-1) + '%s)'
    try:
        cursor.execute(sql, tuple(row))
        connection.commit()
    except Exception as e:
        # should we have cases for the different errors, eg servernot found, operation timed out, table doesn't exist etc?
        print(f'Error message: {e.msg}')
        print(f'Error code: {e.errno}')
        print(f'SQL state value: {e.sqlstate}')
        print(f'Error type: {type(e)}')
        match type(e):
            case mysql.connector.errors.IntegrityError:
                # iter 1:
                # add both
                # iter 2:
                print(f'Row already exists in {table_name}. Aborting insertion of row.')
                # iter 3:
                # remove old one (or assign overwritten status) and add the new one
                pass
            

            

def add_local_data_to_server_db(db_name, table_name, data, local_to_server_columns):
    renamed_data = rename_column_names(data, column_map=local_to_server_columns)
    with mysql.connector.connect(database=db_name, user=server_credentials['user'], password=server_credentials['password']) as connection:
        cursor = connection.cursor()

        server_columns = ','.join([str(i) for i in renamed_data.columns.tolist()])
        for i,row in renamed_data.iterrows():
            insert_row_to_server(db_name=db_name, table_name=table_name,columns=server_columns, row=row, cursor=cursor, connection=connection)
        cursor.close() # not sure if the cursor gets closed with the `with` function, so this is just to make sure
    

