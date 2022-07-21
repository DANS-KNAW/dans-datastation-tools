Development
===========

This page contains information for developers about how to contribute to this project.

Set-up
------

The project uses [poetry] for a build system. It is recommended that you install everything, including poetry, in a
virtual environment.

```shell
git clone 
cd dans-datastation-tools
python3 -m venv venv
source venv/bin/activate
python3 -m pip install poetry 
poetry install
```

Testing commands
----------------

After `poetry install` the commands provided by the module can be tested by prepending `poetry run` to the command line,
e.g.:

```shell
poetry run update-datacite-record 10.17026/some-suffix
```

For more information about how to use poetry, see the [poetry] documentation.

[poetry]: https://python-poetry.org/

Debugging commands in IntelliJ
------------------------------

There seems to be no special support for poetry with this respect. The only way to run a command in the debugger I have
found is to just call the entry-point script under `src/datastation/scripts`.

!!! Warning
    
    Change the working directory of the run configuration to the project root directory, so that you don't end up with
    a `.dans-datastation-tools.yml` file inside the `src/datastation/scripts` directory.

Adding to this documentation site
---------------------------------

Edit the markdown pages in the `docs` folder then run the site locally.

In an active `venv`:
```shell
pip3 install -r .github/workflows/mkdocs/requirements.txt # only once
mkdocs serve
```

Browse to <http://127.0.0.1:8000>{:target=_blank} to view your changes.
 
Coding style
------------

* Except for input from configured APIs such as Dataverse and DataCite, prefer reading input from:
    1. The command line (for simple values)
    2. The standard input (for lists o)
* Write output to the standard output
* Write status messages (OK, Oops, ...) for the end user to the standard error
* Write all other information to the logs (at an appropriate level)
* Get all the configuration variables from the `config` dictionary in the `main` function and pass them to down to 