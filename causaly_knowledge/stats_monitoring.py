import sys
from datetime import datetime


stats = {
    'total_articles_in_db': {
        'OVERRIDE':0,
        'ACTIVE': 0,
        'DELETED': 0
    },
    'total_incoming_articles': 0,
    'outdated_incoming_articles': 0,
    'active_duplicates_in_db': 0, 
    'new_incoming_articles': 0,
    'existing_articles_now_outdated': 0,
    'latest_version_incoming_articles': 0,
}

def update_existing_articles_stats(operation_type, value) -> None:
    stats['total_articles_in_db'][operation_type] = value

def update_incoming_article_stats(total=0, duplicate=0, new=0, outdated=0, latest=0,outdated_incoming=0) -> None:
    stats['total_incoming_articles'] += total
    stats['active_duplicates_in_db'] += duplicate
    stats['new_incoming_articles'] += new
    stats['existing_articles_now_outdated'] += outdated
    stats['latest_version_incoming_articles'] += latest
    stats['outdated_incoming_articles'] += outdated


def log_stats(stats_dict=stats, indent=0, message=''):
    print(message)
    for key, value in stats_dict.items():
        if isinstance(value, dict):
            print('\t' * indent + f'{key}: ')
            log_stats(value, indent+1)
        else:
            print('\t' * (indent)+ f'{key}:  {value}')
       


def log_stats_to_file(stats_dict=stats, indent=0, message='', log_file_dir=''):
   if log_file_dir != '':
    original_stdout = sys.stdout
    with open(log_file_dir, 'a') as f:
        print('\n')
        print(f'Deduplication done at {datetime.now()}')
        sys.stdout = f 
        log_stats(stats_dict=stats_dict, indent=indent, message=message)
        print('\n')
        sys.stdout = original_stdout