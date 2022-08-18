import argparse
import csv
import json
import logging
import psycopg
import requests
import sys

from datastation.config import init


def import_user(dv_server_url, add_builtin_users_key, user, dvndb_conn, dryrun):
    logging.info("Add user {} with email {}".format(user["userName"], user["email"]))
    dummy_password = "1234AB"

    user_json = json.dumps(user)
    api_call = "{}/api/builtin-users?password={}&key={}&sendEmailNotification=false".format(
        dv_server_url, dummy_password, add_builtin_users_key)
    if dryrun:
        logging.info("dry-run, not calling {}".format(api_call))
        logging.debug(user_json)
    else:
        header = {'Content-Type': 'application/json'}
        dv_resp = requests.post(api_call, data=user_json, headers=header)
        response = dv_resp.json()
        if response["status"] == "ERROR":
            logging.error("response from dataverse API: {}".format(response["message"]))
            logging.info("NOT updating password for user {} in database".format(user["userName"]))
            return

    update_statement = "UPDATE builtinuser SET encryptedpassword = '{}' , passwordencryptionversion = 0 " \
                       "WHERE username = '{}'".format(user["encrypted_password"], user["userName"])
    if dryrun:
        logging.info("dry-run, not updating database with {}".format(update_statement))
    else:
        with dvndb_conn.cursor() as dvndb_cursor:
            try:
                dvndb_cursor.execute(update_statement)
                dvndb_conn.commit()
            except psycopg.DatabaseError as error:
                logging.error(error)
                sys.exit("FATAL ERROR: problem updating dvndb password for user {}".format(user["userName"]))


def connect_to_database(host: str, db: str, user: str, password: str):
    return psycopg.connect(
        "host={} dbname={} user={} password={}".format(host, db, user, password))


def main():
    config = init()

    parser = argparse.ArgumentParser(description='Import users into a Dataverse using the BuiltinUsers.KEY')
    parser.add_argument('--easy', dest='is_easy_format',
                        help="The csv file is exported from EASY and has the following columns: UID, INITIALS, SURNAME,"
                             "PREFIX, EMAIL, ORGANISATION, FUNCTION, PASSWORD-HASH. "
                             "If not set, the following columns are expected: Username, GivenName, FamilyName, Email, "
                             "Affiliation, Position, encryptedpassword",
                        action='store_true')
    parser.add_argument('-k', '--builtin-users-key', help="BuiltinUsers.KEY set in Dataverse")
    parser.add_argument('-r', '--dry-run', dest='dry_run', help="only logs the actions, nothing is executed",
                        action='store_true')
    parser.add_argument('-i', '--input-csv', help="the csv file containing the users and hashed passwords")
    args = parser.parse_args()

    if (not args.dry_run) and args.builtin_users_key is None:
        sys.exit("Exiting: Not in dry-run mode but no builtin_users_key provided")

    dvndb_conn = None
    try:
        if not args.dry_run:
            dvndb_conn = connect_to_database(config['dataverse']['db']['host'], config['dataverse']['db']['dbname'],
                                             config['dataverse']['db']['user'], config['dataverse']['db']['password'])

        with open(args.input_csv, "r") as input_file_handler:
            csv_delimeter = ';' if args.is_easy_format else ','
            csv_reader = csv.DictReader(input_file_handler, delimiter=csv_delimeter)

            for row in csv_reader:
                if args.is_easy_format:
                    last_name = (row["PREFIX"], row["SURNAME"])
                    # the PASSWORD-HASH starts with '{SHA}' which needs to be stripped
                    encrypted_password = row["PASSWORD-HASH"][5:]
                    user = {"userName": row["UID"], "firstName": row["INITIALS"], "lastName": " ".join(last_name),
                            "email": row["EMAIL"], "affiliation": row["ORGANISATION"], "position": row["FUNCTION"],
                            "encrypted_password": encrypted_password}
                else:
                    user = {"userName": row["Username"], "firstName": row["GivenName"],
                            "lastName": row["FamilyName"], "email": row["Email"], "affiliation": row["Affiliation"],
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
