from server_db_operations import add_local_data_to_server_db
from local_db_operations import read_local_data, rename_column_names
from constants import extra_metadata_file, server_to_local_columns, server_name, table_name, local_to_server_columns

local_data = rename_column_names(df=read_local_data(file_name=extra_metadata_file), column_map=server_to_local_columns)
add_local_data_to_server_db(db_name=server_name, table_name=table_name, data=local_data, local_to_server_columns=local_to_server_columns)