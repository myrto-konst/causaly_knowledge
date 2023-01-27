def print_error_messages(e):
    print(f'Error message: {e.msg}')
    print(f'Error code: {e.errno}')
    print(f'SQL state value: {e.sqlstate}')
    print(f'Error type: {type(e)}')
