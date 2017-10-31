import argparse
from .meta import CurrentSession

REMOVE_FROM_DO = "removefromdo"


def remove_from_do(id):
    MINIMUM_ID_LENGTH = 5
    MAX_COLS_TO_PRINT = 50

    if len(id) < MINIMUM_ID_LENGTH:
        print("The identifier '{}' is too short (minimum {} characters)".format(id, MINIMUM_ID_LENGTH))
        return

    session = CurrentSession().get()

    numtables = session.execute(
        "SELECT count(*) FROM observatory.obs_table WHERE id LIKE '{}%'"
        .format(id)).fetchone()[0]
    print(" --> This will delete {} entries from observatory.OBS_TABLE".format(numtables))
    for table in session.execute("SELECT id FROM observatory.obs_table WHERE id LIKE '{}%'".format(id)).fetchall():
        print("\t" + table[0])

    numcols = session.execute(
        "SELECT count(*) FROM observatory.obs_column WHERE id LIKE '{}%'"
        .format(id)).fetchone()[0]
    print(" --> This will delete {} entries from observatory.OBS_COLUMNS".format(numcols))
    for column in session.execute(
            "SELECT id FROM observatory.obs_column WHERE id LIKE '{}%'".format(id)).fetchmany(MAX_COLS_TO_PRINT):
        print("\t" + column[0])
    if numcols > MAX_COLS_TO_PRINT:
        print("\t... ({} more)".format(numcols - MAX_COLS_TO_PRINT))
    print(" --> This will drop {} tables from the 'observatory' schema".format(session.execute(
        "SELECT count(*) FROM observatory.obs_table WHERE id LIKE '{}%'".format(id)).fetchone()[0]))

    yn = input("Continue? (Y/N) ")

    if yn.lower() != "y":
        return

    for table in session.execute(
            "SELECT tablename FROM observatory.obs_table WHERE id LIKE '{}%'".format(id)).fetchall():
        session.execute("DROP TABLE observatory.{}".format(table[0]))
        print("Table {} dropped".format(table[0]))
    session.execute("DELETE FROM observatory.obs_table WHERE id LIKE '{}%'".format(id))
    print("Deleted {} entries from observatory.OBS_TABLE".format(numtables))
    session.execute("DELETE FROM observatory.obs_column WHERE id LIKE '{}%'".format(id))
    print("Deleted {} entries from observatory.OBS_COLUMNS".format(numcols))

    session.execute("COMMIT")

if __name__ == "__main__":
    parser = argparse.ArgumentParser('python tools.py')
    parser.add_argument('task', help='Task to be executed')
    parser.add_argument('task_parameters', nargs='+', help='Task parameters')
    args = vars(parser.parse_args())
    if args['task'].lower() == REMOVE_FROM_DO and len(args['task_parameters']) == 1:
        remove_from_do(args['task_parameters'][0])
    else:
        parser.print_help()
