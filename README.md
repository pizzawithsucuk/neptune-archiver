# neptune-archiver

Small python script for crawling through a neptune.ai project and store data locally archive-like. In addition, 
the script provides functionality to retrieve an archived project and upload all data to a neptune.ai project. Useful 
to archive projects and free-up space in your workspace. Under development, code is a draft.

## Usage
Make sure you have neptune installed and your neptune api-token is set as an environment variable. 
```
# Archiving a nepunte project
python cli.py archive --destination /path/to/local/dir --project-id workspace/project-name

# Restoring an archived project
python cli.py retrieve --source /path/to/archived/project --workspace workspace --project-name project-name
```
