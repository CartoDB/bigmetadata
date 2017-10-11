def copy_from_csv(session, table_name, columns, csv_stream):
    '''
    Creates a table, loading the data from a .csv file.

    :param session: A SQL Alchemy session
    :param table_name: Output table name
    :param columns: Dictionary of columns, keys are named, values are types.
    :param csv_stream: A stream that reads a CSV file. e.g: a file or a
                       :class:`CSVNormalizerStream <lib.csv_stream.CSVNormalizerStream>`
    '''
    with session.connection().connection.cursor() as cursor:
        cursor.execute('CREATE TABLE {output} ({cols})'.format(
            output=table_name,
            cols=', '.join(['{name} {type}'.format(name=k, type=v) for k, v in columns.items()])
        ))

        cursor.copy_expert(
            'COPY {table} ({cols}) FROM stdin WITH (FORMAT CSV, HEADER)'.format(
                cols=', '.join(columns.keys()),
                table=table_name),
            csv_stream)
