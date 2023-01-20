local_to_server_columns={ 
    'name': 'article_name', 
    'ID': 'article_ID', 
    'journal': 'journal_name',
    'article_status': 'citation_status',
    'article_version': 'citation_version',
    'operation_type': 'operation_type',
    'uuid': 'article_uuid',
}

server_to_local_columns={
    'article_name': 'name', 
    'article_ID': 'ID', 
    'journal_name': 'journal',
    'citation_status': 'article_status',
    'citation_version': 'article_version',
    'operation_type': 'operation_type',
    'article_uuid': 'uuid'
}

medline_status = {
  'In-Data-Review':     1,
  'In-Process':         2, 
  'MEDLINE':            5,
  'OLDMEDLINE':         0, 
  'PubMed-not-MEDLINE': 4, 
  'Publisher':          1
}