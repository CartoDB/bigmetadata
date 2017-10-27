import sys
from .meta import CurrentSession

REMOVE_FROM_DO = "removeFromDO"


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

    if yn != "Y":
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


def print_remove_from_do_help():
    print("$python tools.py removeFromDO id_of_data_to_remove")


if __name__ == "__main__":
    if (len(sys.argv) == 1 or "?" in sys.argv[1]):
        # print all helps here
        print_remove_from_do_help()
    elif REMOVE_FROM_DO == sys.argv[1]:
        if len(sys.argv) == 3:
            remove_from_do(sys.argv[2])
        else:
            print_remove_from_do_help()
