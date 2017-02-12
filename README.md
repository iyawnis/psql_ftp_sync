# psql_ftp_sync
Script for keeping a Postgres database synced with a FTP server


Depedencies:

    $ > sudo apt-get install python3
    $ > sudo apt-get install python3-dev

> (optional step here)

    $ > pip install -r requirements.txt


Optional Step:

Virtual env helps keep your python environment clean, and avoid depedency clashes between projects.

    $ > sudo apt-get install virtualenv
    $ > virtualenv -p python3 env
    $ > source ./env/bin/activate


Running the script:

* Set all the values inside env_params file. FTP_DIR can be left to '.'' if files are on root dir.
* Run the command ```source ./env_params``` to load the parameters on local shell
* Run ```python ftp_sync.py```


A file has already been added to the system using this script, this can be seen in the example.txt.
