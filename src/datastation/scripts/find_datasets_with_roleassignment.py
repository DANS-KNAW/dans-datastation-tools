import argparse
import logging
import sys
import psycopg

from datastation.config import init


def find_datasets_with_roleassignment(dvndb_conn, role_assignee, role_alias):
    select_statement = \
        "select concat(dvo.protocol, ':', dvo.authority, '/', dvo.identifier) " \
        "from roleassignment ra inner join dataverserole dr on ra.role_id=dr.id " \
        "inner join dvobject dvo on definitionpoint_id=dvo.id " \
        "where dtype='Dataset' and assigneeidentifier='{}' and alias='{}'".format(role_assignee, role_alias)

    with dvndb_conn.cursor() as dvndb_cursor:
        try:
            dvndb_cursor.execute(select_statement)
            result = dvndb_cursor.fetchall()
            if len(result) == 0:
                logging.info("No datasets for user {} with role {}".format(role_assignee, role_alias))
            for r in result:
                print(r[0])
        except psycopg.DatabaseError as error:
            logging.error(error)
            sys.exit("FATAL ERROR: problem accessing database with {} ".format(select_statement))


def main():
    config = init()
    parser = argparse.ArgumentParser(description='Find datasets that are linked to a specific account')
    parser.add_argument("role_assignment", help="Role assignee and alias (example: @dataverseAdmin=contributor)")
    args = parser.parse_args()

    role_assignee = args.role_assignment.split('=')[0]
    role_alias = args.role_assignment.split('=')[1]

    dvndb_conn = None
    try:
        dvndb_conn = connect_to_database(config['dataverse']['db']['host'], config['dataverse']['db']['dbname'],
                                         config['dataverse']['db']['user'], config['dataverse']['db']['password'])
        find_datasets_with_roleassignment(dvndb_conn, role_assignee, role_alias)
    except psycopg.DatabaseError as error:
        logging.error(error)
        sys.exit("FATAL ERROR: No connection with database established: {}".format(error))
    finally:
        if dvndb_conn is not None:
            dvndb_conn.close()


def connect_to_database(host, dbname, user: str, password: str):
    return psycopg.connect(
        "host={} dbname={} user={} password={}".format(host, dbname, user, password))


if __name__ == '__main__':
    main()
