## Download all files from a Terra table 

1. Install Anaconda3 locally using this link
https://docs.anaconda.com/free/anaconda/install/index.html

2. Make a conda environment and install neccessary packages
```
conda create --name terra_env python=3.6
conda activate terra_env
pip install --upgrade pip pandas numpy google-cloud-storage terra_notebook_utils terra_pandas
```

If you encounter issues about initiating your shell before activating your environemnt you have to put the command below in ~/.bash_profile and login again.
```
source ${ANACONDA3_PATH}/etc/profile.d/conda.sh
```
You can find `${ANACONDA3_PATH}` using the base environment in `conda info`

3. Before runing the script make sure to login to your google account and set your `${PROJECT_ID}` (the name of the billing project)
```
gcloud auth login
gcloud config set project ${PROJECT_ID}
```

4. Clone repo and run `pull_terra_table.py`

```
git clone https://github.com/mobinasri/terra_scripts
python3 terra_scripts/pull_terra_table.py \
  --workspace ${WORKSPACE} \
  --workspace-namespace ${WORKSPACE_NAMESPACE} \
  --table-name ${TABLE_NAME} \
  --exclude-columns col_names.txt \
  --exclude-rows row_names.txt \
  --dir ${DIRECTORY}
  --threads ${THREADS_IO}
  --no-prompt
```
`--exclude-columns col_names.txt` and `--exclude-rows row_names.txt` are optional. The program will write the total size of the files to be downloaded and ask you in a prompt if you want proceed. Use `--no-prompt` when the code is not run in an interactive environment (like submitted by sbatch).
The full documentation is as below
```
Pull files from a terra table

optional arguments:
  -h, --help            show this help message and exit
  --workspace WORKSPACE
                        Workspace
  --workspace-namespace WORKSPACE_NAMESPACE
                        Workspace namespace (same as billing project)
  --table-name TABLE_NAME
                        Table name
  --exclude-columns EXCLUDE_COLUMNS
                        [optional] Path to a text file with the list of column
                        names to exclude (one column name per line)
  --exclude-rows EXCLUDE_ROWS
                        [optional] Path to a text file with the list of row
                        names to exclude (one row name per line)
  --download-external   Download files from other workspaces if they exist in
                        the table (and whose row and column are not excluded)
  --dir DIR             Output directory
  --threads THREADS     Number of IO threads [default = 4]
  --no-prompt           No prompt for checking size of the whole table
```
