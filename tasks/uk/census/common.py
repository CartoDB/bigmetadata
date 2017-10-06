def table_from_id(census_id):
    '''
    Return the table for a census ID, the first 8 characters.
      LC2102EW0016
      ^^^^^^^^
    '''
    return census_id[:8]
