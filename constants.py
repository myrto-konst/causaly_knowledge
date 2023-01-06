metadata_file = 'data/metadata.csv'
extra_metadata_file = 'data/extra_metadata.csv'

server_name = 'causaly_knowledge'
table_name = 'metadata'

local_to_server_columns={ 
    'name': 'article_name', 
    'ID': 'article_ID', 
    'journal': 'journal_name'
}

server_to_local_columns={'article_name': 'name', 
    'article_ID': 'ID', 
    'journal_name': 'journal'
}

server_credentials = {
     'user': 'root', 
     'password': 'root'
}