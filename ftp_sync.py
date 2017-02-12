#!/usr/bin/env python3

import psycopg2
import os
from io import BytesIO
from ftplib import FTP
from traceback import print_exc
from collections import namedtuple

FTP_HOST=os.environ['FTP_HOST']
FTP_USER=os.environ['FTP_USER']
FTP_PASSWD=os.environ['FTP_PASSWD']
FTP_DIR=os.environ['FTP_DIR']

DocassValues = namedtuple('DocassValues', ['file_id', 'file_title', 'file_descrip', 'item_number', 'ls_id', 'ls_number'])
FileToStore = namedtuple('FileToStore', ['title', 'description', 'stream',])

conn_config = {
    'host': os.environ['DB_HOST'],
    'dbname': os.environ['DB_NAME'],
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWD'],
    'port': os.environ['DB_PORT']
}

def execute_sql(sql, params):
    with psycopg2.connect(**conn_config) as conn:
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
        files = ftp.nlst(FTP_DIR)
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
    stored_diff = set(ls_numbers) - set(all_existing)
    files_to_store = []
    for ls_number in stored_diff:
        filename = filter(lambda ftp_name: filename_to_ls_num(ftp_name) == ls_number, matches)
        files_to_store += filename
    return files_to_store

def load_ftp_file(filename):
    """
    Given a filename, fetch the file from FTP, and return a stream object
    """

    with BytesIO() as byte_stream:
        with FTP(host=FTP_HOST, user=FTP_USER, passwd=FTP_PASSWD) as ftp:
            ftp.retrbinary('RETR {}/{}'.format(FTP_DIR, filename), byte_stream.write)
        ls_number = filename_to_ls_num(filename)
        byte_stream.seek(0)
        return FileToStore(title=ls_number, description='Uploaded file', stream=psycopg2.Binary(byte_stream.read()))


def insert_missing_file_entries(filenames_to_store):
    """
    Given a list of missing filenames, fetch the files and store them in the file table
    """
    files_to_store = [load_ftp_file(filename) for filename in filenames_to_store]
    sql = """INSERT INTO file(file_title, file_descrip, file_stream) VALUES {} RETURNING file_id, file_title, file_descrip;"""
    with psycopg2.connect(**conn_config) as conn:
        with conn.cursor() as cursor:
            records_list_template = ','.join(['%s'] * len(files_to_store))
            insert_query = sql.format(records_list_template)
            cursor.execute(insert_query, files_to_store)
            return cursor.fetchall()

def retrieve_docass_values(file_entries):
    """
    Retrieve all the values necessary to generate the docass entry
    """
    sql = """SELECT ls.ls_number, ls.ls_id, item.item_number FROM ls JOIN item ON ls.ls_item_id = item.item_id WHERE ls.ls_number IN %s;"""
    ls_number_to_file_id = {entry[1]: (entry[0], entry[2],) for entry in file_entries}
    item_ls_data = execute_sql(sql, tuple(ls_number_to_file_id.keys()))
    docass_entries = []
    for result in item_ls_data:
        file_data = ls_number_to_file_id[result[0]]
        new_entry = DocassValues(
            file_id=file_data[0],
            file_title=result[0],
            file_descrip=file_data[1],
            item_number=result[2],
            ls_id=result[1],
            ls_number=result[0])
        docass_entries.append(new_entry)
    return docass_entries

if __name__ == "__main__":
    matches = find_ls_ftp_matches()
    if matches:
        files_to_store = find_all_files_not_uploaded(matches)
        if files_to_store:
            file_entries = insert_missing_file_entries(files_to_store)
            docass_values = retrieve_docass_values(file_entries)
