#!/usr/bin/env python3

import psycopg2
import os
from ftplib import FTP
from traceback import print_exc

DB_HOST=os.environ['DB_HOST']
DB_NAME=os.environ['DB_NAME']
DB_USER=os.environ['DB_USER']
DB_PASSWD=os.environ['DB_PASSWD']
FTP_HOST=os.environ['FTP_HOST']
FTP_USER=os.environ['FTP_USER']
FTP_PASSWD=os.environ['FTP_PASSWD']

def execute_sql(sql, params):
    with psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWD) as conn:
         with conn.cursor() as cursor:
            cursor.execute(sql, (params,))
            return cursor.fetchall()

def get_ls_numbers():
    """
    Retrieve all the unique ls_number entries from ls table
    """
    SQL = """SELECT DISTINCT(ls.ls_number) FROM ls"""
    return set([row[0] for row in execute_sql(SQL, None)])


def get_uploaded_file_names():
    """
    Retrieve the available uploaded filenames
    """
    files = []
    with FTP(host=FTP_HOST, user=FTP_USER, passwd=FTP_PASSWD) as ftp:
        files = ftp.nlst()
    return files

def filename_to_ls_num(ftp_filename):
    """
    Change a filename, to an ls_number
    """
    try:
        file_name = ftp_filename.split('.')[0]
    except:
        file_name = ftp_filename
        print_exc()
        print('Filename without extension:', file_name)
    return file_name

def find_ls_ftp_matches():
    """
    Find all the filenames that match between ls_number and ftp files
    """
    ls_numbers = get_ls_numbers()
    ftp_files = get_uploaded_file_names()
    matches = []
    for ftp_file in ftp_files:
        file_name = filename_to_ls_num(ftp_file)
        if file_name in ls_numbers:
            matches.append(ftp_file)
    return matches

def find_all_files_not_uploaded(matches):
    """
    For all the file-ls matches, find those that have not been uploaded
    """
    ls_numbers = tuple([filename_to_ls_num(filename) for filename in matches])
    SQL = """SELECT docass.docass_source_id from docass where docass.docass_source_id IN %s AND docass.docass_source_type='LS'"""
    all_existing = set(execute_sql(SQL, ls_numbers))
    return set(ls_numbers) - set(all_existing)


if __name__ == "__main__":
    matches = find_ls_ftp_matches()
    print(find_all_files_not_uploaded(matches))
