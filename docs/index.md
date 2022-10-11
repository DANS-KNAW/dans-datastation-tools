dans-datastation-tools
======================

Command line utilities for Data Station application management

SYNOPSIS
--------

```bash
pip3 install dans-datastation-tools

# Available commands, use --help for more information

# Manage a dataverse collection
dv-dataverse-retrieve-pids
dv-dataverse-oai-harvest

# Manage role assignments
dv-dataset-find-with-role-assignment
dv-dataset-add-role-assignment
dv-dataset-delete-role-assignment

# Manage dataset metadata
dv-dataset-replace-metadata-field-values
dv-dataset-retrieve-metadata
dv-dataset-retrieve-metadata-field

# Manage dataset state
dv-dataset-delete-draft
dv-dataset-destroy
dv-dataset-publish
dv-dataset-reindex
dv-dataset-unlock
dv-dataset-update-datacite-record

#manage Banner Messages
dv-banner

# User management
dv-user-import

# Manage dd-ingest-flow
ingest-flow-copy-batch-to-ingest-area
ingest-flow-move-batch-to-ingest-area
ingest-flow-start-migration
ingest-flow-start-import

# Validate bags for compliance with DANS BagIt Profile
dans-bag-validate

# Helper scripts for the migration from EASY
dv-dataset-destroy-migration-placeholder
dv-file-prestage

```

DESCRIPTION
-----------

This module contains a variety of command line scripts to facilitate DANS Data Station management. Each script comes
with a command line help.

### dataset-destroy

Destroying a dataset is irreversible (well, maybe there is a back-up, but still). During a migration or testing it will
sometimes be necessary to destroy datasets, but in a production environment this should be used *very* sparingly. That
is why there is a safety_latch as and extra precaution (the first is that only superusers are allowed to do destroys by
Dataverse). The latch is initially set to `ON`:

```yaml
dataverse:
  api_token: your-api-token-here
  server_url: 'http://localhost:8080'
  files_root: your-files-root-here
  safety_latch: ON # <== safety latch on
  db:
    host: localhost
    dbname: dvndb
    user: dvnuser
    password: your-password-here
```

Note that this setting is a boolean. If you put it in quotes it will always be interpreted as `True`. Unquoted `no`,
`off`, `False` will work to turn it off.

**AFTERWARDS, ALWAYS TURN IT BACK ON**.

EXAMPLES
--------

### dans-bag-validate

The JSON output of this command can be queried with `jq`. This tool has a very good
[manual](https://stedolan.github.io/jq/manual/){:target=_blank}. However, to get you started, here are some example
queries:

```text
dans-bag-validator <target> -o ~/results.json

# Print only the bag location and the violations
cat results.json | jq 'map({location: ."Bag location", violations: ."Rule violations"})'

# Count the number of bags that are non-compliant
cat results.json | jq '[.[] | select(."Is compliant" == false)] | length'

# Get the paths to the *deposits* containing valid bags. Note that "Bag location" is one level too deep, that's why we need to remove the 
# last path element. The detour through to_entries seems necessary to get rid of the array structure around the results.
cat results.json | select(."Is compliant" == true)] | map(."Bag location") | map(split("/") | .[:-1] | join("/")) | to_entries[] | "\(.value)"
```

INSTALLATION & CONFIGURATION
----------------------------

### Installation

* Globally:

  ```bash
  sudo pip3 install dans-datastation-tools
  ```

* For the current user:

  ```bash
  pip3 install --user dans-datastation-tools
  ```
  You may have to add the directory where `pip3` installs the command to the `PATH` manually.

### Configuration

The configuration file is called `.dans-datastation-tools.yml`. Each command starts by looking for this file in the
current working directory and then in the user's home directory. If it is not found in either location it is
instantiated with some default and placeholder values in the current directory. It is recommended that you move this
file to your home directory. Using the configuration file from the current working directory is mainly useful for
development.

For the available configuration options and their meaning, see the explanatory comments in the configuration file
itself.

