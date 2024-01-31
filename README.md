# neptune-archiver

Small python script for crawling through a neptune.ai project and store data locally archive-like. In addition,
the script provides functionality to retrieve an archived project and upload all data to a neptune.ai project. Useful
to archive projects and free-up space in your workspace. Under development, code is a draft. Feedback and contributions
welcome.

## Usage

Make sure you have neptune installed and your neptune api-token is set as an environment variable.

```
# Archiving a nepunte project
python cli.py archive --destination /path/to/local/dir --project-id workspace/project-name

# Restoring an archived project
python cli.py retrieve --source /path/to/archived/project --workspace workspace
```

## Warning

The script is still under development. Please ensure your neptune project was transferred correctly before deleting
it in your neptune workspace. This script does not support GitRef, Notebooks, and Models. When archiving FileSeries,
description and name attributes are lost and the order of files may change. When restoring a project to a neptune
workspace, a number of sys attributes such as monitoring time can not be set through the neptune api. These attributes
are set in the backend and, thus, differ from the archived values. You can upload the values of the archived version to 
an alternative namespace by using the --alternative-sys-namespace argument.


 