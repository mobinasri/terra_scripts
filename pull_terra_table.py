import pandas as pd
import numpy as np
import sys
import argparse
import operator
import terra_pandas as tp
import terra_notebook_utils as tnu
from google.cloud import storage
import os

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

def download_uri(uri, dir_path):
    try:
        # make a bucket object for downloading
        storage_client = storage.Client()
        bucket_name = get_bucket_name(uri)
        blob_name = get_blob_name(uri)
        object_name = get_object_name(uri)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        file_path = os.path.join(dir_path, object_name)
        with open(file_path,'wb') as f:
            storage_client.download_blob_to_file(blob, f)
    except Exception as e:
        print(e)
        return False


def write_to_text(entities, entity_path):
    with open(entity_path, "w") as f:
        f.write("\n".join(np.array(entities).astype(str)))
        f.write("\n")

def main():
    parser = argparse.ArgumentParser(description='Pull terra table')
    parser.add_argument('--workspace', type=str, help='Workspace')
    parser.add_argument('--workspace-namespace', type=str, help='Workspace namespace (same as billing project)')
    parser.add_argument('--table-name', type=str, help='Table name')
    parser.add_argument('--exclude-columns', type=str, help='[optional] Path to a text file with the list of column names to exclude (one column name per line)')
    parser.add_argument('--exclude-rows', type=str, help='[optional] Path to a text file with the list of row names to exclude (one row name per line)')
    parser.add_argument('--download-external', action='store_true', help='Download files from other workspaces if they exist in the table (and whose row and column are not excluded)')
    parser.add_argument('--dir', type=str, help='Output directory')
    args = parser.parse_args()
    workspace = args.workspace
    workspace_namespace = args.workspace_namespace
    table_name = args.table_name
    row_exclude_path = args.exclude_rows
    column_exclude_path = args.exclude_columns
    download_external = args.download_external
    dir_path = args.dir
    
    # read row names and column names to exclude
    row_exclude = read_list(row_exclude_path)
    column_exclude = read_list(column_exclude_path)

    # pull the table
    table = tp.table_to_dataframe(table_name, None, workspace, workspace_namespace)

    print(table)

    # get the bucket name
    bucket_name = tnu.workspace.get_workspace_bucket(workspace)

    for col_name in table.columns:
        # skip this column if should be excluded
        if col_name in column_exclude:
            continue
        for row_name in table.index:
            # skip this row if should be excluded
            if row_name in row_exclude:
                continue
            entity = table[col_name][row_name]
            print(entity, type(entity))
            # skip it if the entity is not defined
            if not isinstance(entity, list) and pd.isna(entity):
                continue
            # if the entity is a single gs uri
            if isinstance(entity, str) and entity.startswith("gs://"):
                # check if this uri is external 
                if is_external(entity, bucket_name) and download_external == False:
                    continue
                entity_dir = os.path.join(dir_path, row_name, col_name)
                os.makedirs(entity_dir, exist_ok=True)
                download_uri(entity, entity_dir)
            # if the entity is a list of gs uri
            elif isinstance(entity, list) and isinstance(entity[0], str) and entity[0].startswith("gs://"):
                for i, element in enumerate(entity):
                    # check if this uri is external
                    if is_external(element, bucket_name) and download_external == False:
                        continue
                    element_dir = os.path.join(dir_path, row_name, col_name, str(i))
                    os.makedirs(element_dir, exist_ok=True)
                    download_uri(element, element_dir)
            # if the entity is not a gs uri
            # it should be either a numeric or a string
            else:
                entity_dir = os.path.join(dir_path, row_name, col_name)
                os.makedirs(entity_dir, exist_ok=True)
                entity_path = os.path.join(entity_dir, f'{col_name}.txt')
                if isinstance(entity, list):
                    write_to_text(entity, entity_path)
                else:
                    write_to_text([entity], entity_path)
            print(f"*{row_name}:{col_name} is pulled.")


if __name__ == "__main__":
    main()
