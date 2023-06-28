from datastation.common.csv import CsvInput
from datastation.common.database import Database
from datastation.dataverse.builtin_users import User
from datastation.dataverse.dataverse_client import DataverseClient


def update_password(database, user, encryption_version=1, dry_run=False):
    update_statement = f"UPDATE builtinuser SET encryptedpassword = '{user.encrypted_password}', passwordencryptionversion = {encryption_version} WHERE username = '{user.username}';"
    if dry_run:
        print(f"dry-run, not updating database with {update_statement}")
    else:
        database.update(update_statement)


class UserImport:
    """Imports users from a CSV file into a Dataverse instance. The CSV file can be exported from EASY or manually
    created. The Dataverse instance must have the BuiltinUsers.KEY set. """

    def __init__(self, dataverse_client: DataverseClient, is_easy_format: bool,
                 builtin_users_key: str, dry_run: bool = False):
        self.dataverse_client = dataverse_client
        self.is_easy_format = is_easy_format
        self.builtin_users_key = builtin_users_key
        self.dry_run = dry_run

    def import_users(self, csv_file):
        with Database(self.dataverse_client.db_config) as database:
            with UserCsv(csv_file, is_easy_format=self.is_easy_format) as userCsv:
                for user in userCsv:
                    self.import_user(user, self.builtin_users_key, database)

    def import_user(self, user, builtin_users_key, database, dry_run=False):
        r = self.dataverse_client.built_in_users(builtin_users_key=builtin_users_key).create(user,
                                                                                             send_email_notification=False,
                                                                                             dry_run=self.dry_run)
        if r is None:
            return
        if r.status_code == 200:
            update_password(database, user, 0 if self.is_easy_format else 1, dry_run)
            print(f"Imported user {user.username}")
        else:
            print(f"Error creating user {user}: {r.status_code} {r.json()['message']}")


class UserCsv(CsvInput):

    def __init__(self, csv_file, is_easy_format):
        super().__init__(csv_file, delimiter=";" if is_easy_format else ",")
        self.is_easy_format = is_easy_format

    def __iter__(self):
        for row in super().__iter__():
            if self.is_easy_format:
                last_name = (row["PREFIX"], row["SURNAME"])
                # the PASSWORD-HASH starts with '{SHA}' which needs to be stripped
                encrypted_password = row["PASSWORD-HASH"][5:]
                yield User(row["UID"], row["INITIALS"], " ".join(last_name), row["EMAIL"], row["ORGANISATION"],
                           row["FUNCTION"], encrypted_password)
            else:
                yield User(row["Username"], row["GivenName"], row["FamilyName"], row["Email"], row["Affiliation"],
                           row["Position"], row["encryptedpassword"])
