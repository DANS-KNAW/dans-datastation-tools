import argparse
import csv
import json
import logging
import psycopg
import requests
import sys

from datastation.config import init


def import_user(dv_server_url, add_builtin_users_key, user, dvndb_conn, dryrun):
    logging.info("Add {} with email {}".format(user["user_name"], user["email"]))
    dummy_password = "1234AB"
    api_call = "{}/api/builtin-users?password={}&key={}&sendEmailNotification=false".format(
        dv_server_url, dummy_password, add_builtin_users_key)
    header = {'Content-type': 'application/json'}
    if dryrun:
        logging.info("dry-run, not calling {}".format(api_call))
    else:
        dv_resp = requests.get(api_call, data=json.dumps(user), headers=header)
        dv_resp.raise_for_status()
    update_statement = "UPDATE builtinuser SET encryptedpassword = {} , passwordencryptionversion = 0 " \
                       "WHERE username = {}".format(user["encrypted_password"], user["user_name"])
    if dryrun:
        logging.info("dry-run, not updating database with {}".format(update_statement))
    else:
        with dvndb_conn.cursor() as dvndb_cursor:
            try:
                dvndb_cursor.execute(update_statement)
                dvndb_conn.commit()
            except psycopg.DatabaseError as error:
                logging.error(error)
                sys.exit("FATAL ERROR: problem updating dvndb password for user {}".format(user["user_name"]))


def connect_to_database(host: str, db: str, user: str, password: str):
    return psycopg.connect(
        "host={} dbname={} user={} password={}".format(host, db, user, password))


def main():
    config = init()

    parser = argparse.ArgumentParser(description='Import users into a Dataverse using the BUILTIN_USERS_KEY')
    parser.add_argument('--easy', dest='is_easy_format',
                        help="The csv file is exported from EASY and has the following columns: UID, INITIALS, SURNAME,"
                             "PREFIX, EMAIL, ORGANISATION, FUNCTION, PASSWORD-HASH. "
                             "If not set, the following columns are expected: Username, GivenName, FamilyName, Email, "
                             "Affiliation, Position, encryptedpassword",
                        action='store_true')
    parser.add_argument('-k', '--builtin_users_key', help="BUILTIN_USERS_KEY set in Dataverse")
    parser.add_argument('-r', '--dryrun', dest='dry_run', help="only logs the actions, nothing is executed",
                        action='store_true')
    parser.add_argument('-i', '--input_csv', help="the csv file containing the users and hashed passwords")
    args = parser.parse_args()

    if (not args.dry_run) and args.builtin_users_key is None:
        sys.exit("Exiting: Not in dry-run mode but no builtin_users_key provided")

    dvndb_conn = None
    try:
        if not args.dry_run:
            dvndb_conn = connect_to_database(config['dataverse']['db']['host'], config['dataverse']['db']['dbname'],
                                             config['dataverse']['db']['user'], config['dataverse']['db']['password'])

        with open(args.input_csv, "r") as input_file_handler:
            csv_reader = csv.DictReader(input_file_handler, delimiter=',')

            for row in csv_reader:
                if args.is_easy_format:
                    last_name = (row["PREFIX"], row["SURNAME"])
                    user = {"user_name": row["UID"], "given_name": row["INITIALS"], "family_name": " ".join(last_name),
                            "email": row["EMAIL"], "affiliation": row["ORGANISATION"], "position": row["FUNCTION"],
                            "encrypted_password": row["PASSWORD-HASH"]}
                else:
                    user = {"user_name": row["Username"], "given_name": row["GivenName"],
                            "family_name": row["FamilyName"], "email": row["Email"], "affiliation": row["Affiliation"],
                            "position": row["Position"], "encrypted_password": row["encryptedpassword"]}

                import_user(config['dataverse']['server_url'], args.builtin_users_key, user, dvndb_conn, args.dry_run)

        if not args.dry_run:
            dvndb_conn.close()
    except psycopg.DatabaseError as error:
        logging.error(error)
    finally:
        if dvndb_conn is not None:
            dvndb_conn.close()


if __name__ == '__main__':
    main()
