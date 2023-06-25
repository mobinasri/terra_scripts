import pandas as pd
import numpy as np
import sys
import argparse
import operator
import terra_pandas as tp
import terra_notebook_utils as tnu
from google.cloud import storage
import os
from concurrent.futures import ThreadPoolExecutor 
import datetime

def get_time():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def read_list(file_path):
    if file_path == None:
        return []
    with open(file_path, "r") as f:
        return f.read().split()

def get_bucket_name(uri):
    return uri.split("/")[2]

def get_blob_name(uri):
    prefix_len = len(get_bucket_name(uri)) + 5
    return uri[prefix_len + 1:]

def get_object_name(uri):
    return uri.split("/")[-1]

def is_external(uri, workspace_bucket_name):
    return get_bucket_name(uri) != workspace_bucket_name

def download_uri(x):
    uri = x[0]
    dir_path = x[1]
    os.makedirs(dir_path, exist_ok=True)
    try:
        # make a bucket object for downloading
        storage_client = storage.Client()
        bucket_name = get_bucket_name(uri)
        blob_name = get_blob_name(uri)
        object_name = get_object_name(uri)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        file_path = os.path.join(dir_path, object_name)
        with open(file_path,'wb') as f:
            storage_client.download_blob_to_file(blob, f)
        return object_name
    except Exception as e:
        print(e)
        return False

def get_size_uri(uri):
    storage_client = storage.Client()
    bucket_name = get_bucket_name(uri)
    blob_name = get_blob_name(uri)
    object_name = get_object_name(uri)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    return blob.size


def write_to_text(entities, entity_path):
    with open(entity_path, "w") as f:
        f.write("\n".join(np.array(entities).astype(str)))
        f.write("\n")

def main():
    parser = argparse.ArgumentParser(description='Pull files from a terra table')
    parser.add_argument('--workspace', type=str, help='Workspace')
    parser.add_argument('--workspace-namespace', type=str, help='Workspace namespace (same as billing project)')
    parser.add_argument('--table-name', type=str, help='Table name')
    parser.add_argument('--exclude-columns', type=str, help='[optional] Path to a text file with the list of column names to exclude (one column name per line)')
    parser.add_argument('--exclude-rows', type=str, help='[optional] Path to a text file with the list of row names to exclude (one row name per line)')
    parser.add_argument('--download-external', action='store_true', help='Download files from other workspaces if they exist in the table (and whose row and column are not excluded)')
    parser.add_argument('--dir', type=str, help='Output directory')
    parser.add_argument('--threads', type=int, default=4, help='Number of IO threads [default = 4]')
    parser.add_argument('--no-prompt', action='store_true', help='No prompt for checking size of the whole table')

    args = parser.parse_args()

    # take arguments
    workspace = args.workspace
    workspace_namespace = args.workspace_namespace
    table_name = args.table_name
    row_exclude_path = args.exclude_rows
    column_exclude_path = args.exclude_columns
    download_external = args.download_external
    dir_path = args.dir
    threads = args.threads
    no_prompt = args.no_prompt
    
    # read row names and column names to exclude
    row_exclude = read_list(row_exclude_path)
    column_exclude = read_list(column_exclude_path)


    # pull the table
    table = tp.table_to_dataframe(table_name, None, workspace, workspace_namespace)

    print(table)

    # get the bucket name
    bucket_name = tnu.workspace.get_workspace_bucket(workspace)

    # create a list of tuples for downloading files by a pool of threads
    # each tuple is (uri, dir_path)
    download_list = []

    total_size = 0

    for col_name in table.columns:
        # skip this column if should be excluded
        if col_name in column_exclude:
            continue
        for row_name in table.index:
            # skip this row if should be excluded
            if row_name in row_exclude:
                continue
            entity = table[col_name][row_name]
            # skip it if the entity is not defined
            if not isinstance(entity, list) and pd.isna(entity):
                print(f"[{get_time()}] Entry is NaN so skipped:\t{row_name} | {col_name}", file=sys.stdout)
                continue
            # if the entity is a single gs uri
            if isinstance(entity, str) and entity.startswith("gs://"):
                # check if this uri is external 
                if is_external(entity, bucket_name) and download_external == False:
                    continue
                entity_dir = os.path.join(dir_path, row_name, col_name)
                total_size += get_size_uri(entity)
                # add uri and path to the download list
                download_list.append((entity, entity_dir))
                print(f"[{get_time()}] Entry is added to the download list:\t{row_name} | {col_name}", file=sys.stdout)
            # if the entity is a list of gs uri
            elif isinstance(entity, list) and isinstance(entity[0], str) and entity[0].startswith("gs://"):
                for i, element in enumerate(entity):
                    # check if this uri is external
                    if is_external(element, bucket_name) and download_external == False:
                        continue
                    element_dir = os.path.join(dir_path, row_name, col_name, str(i))
                    total_size += get_size_uri(element)
                    # add uri and path to the download list
                    download_list.append((element, element_dir))
                print(f"[{get_time()}] Entry is added to the download list:\t{row_name} | {col_name}", file=sys.stdout)
            # if the entity is not a gs uri
            # it should be either a numeric or a string
            elif (isinstance(entity, list) and isinstance(entity[0], str) and entity[0] != "") or (isinstance(entity, str) and entity != ""):
                entity_dir = os.path.join(dir_path, row_name, col_name)
                os.makedirs(entity_dir, exist_ok=True)
                entity_path = os.path.join(entity_dir, f'{col_name}.txt')
                if isinstance(entity, list):
                    write_to_text(entity, entity_path)
                else:
                    write_to_text([entity], entity_path)
                print(f"[{get_time()}] Entry is written in a text file:\t{row_name} | {col_name}", file=sys.stdout)
    if not no_prompt:
        proceed = input(f"[{get_time()}] Total size of the objects to be dowloaded = {total_size/1e9} GB. Do you want to proceed [y/n]?")
        while proceed not in ['y', 'n']:
            print(f"[{get_time()}] Please enter either y or n.")
            proceed = input(f"[{get_time()}] Total size of the objects to be dowloaded = {total_size/1e9} GB. Do you want to proceed [y/n]?")
        if proceed == 'n':
            print(f"[{get_time()}] Downloading aborted.")
            sys.exit()

    # make a pull of threads for downloading files in parallel
    with ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(download_uri, download_list)
    print(f"[{get_time()}] Total size of the objects to be dowloaded = {total_size/1e9} GB. Initiating thread pool for downloading ...", file=sys.stdout)
    for res in results:
        print(f"[{get_time()}] Object is downloaded:\t{res}", file=sys.stdout)
        sys.stdout.flush()
        pass
if __name__ == "__main__":
    main()
