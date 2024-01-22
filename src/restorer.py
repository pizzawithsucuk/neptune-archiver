import neptune
from neptune import management
import config
from pathlib import Path
import json


class Restorer:
    def __init__(self, source):
        with (source / config.PROJECT_STRUCTURE).open('r') as file:
            project_info = json.load(file)
        pass

    def create_project(self, name, key, workspace, visibility):
        management.create_project(workspace=workspace, name=name, key=key, visibility=visibility)


if __name__ == '__main__':
    Restorer(source=Path('/Users/rathjjgf/Desktop/tns/scratch'))


