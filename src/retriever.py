import neptune
from neptune import management
import src.utils as utils
from pathlib import Path
import json
import pandas as pd
from src.utils import RemoteKeys, is_read_only_field, is_value_in_class_attributes
from neptune.types import File
from datetime import datetime


# TODO make upload of runs same order as in original neptune workspace
# TODO make upload of file series same order as in original neptune workspace
# TODO think about how to properly address coherent run ids
# TODO implement notebooks support
# TODO implement models support
# TODO setup test project

class Retriever:

    def __init__(self, source: Path, workspace: str, project_name: str, alternative_sys_namespace=None):
        self.source = source
        self.workspace = self.get_workspace(workspace)
        self.project_name = self.get_project_name(project_name)
        self.project_id = self.workspace + '/' + self.project_name
        self.alternative_sys_namespace = alternative_sys_namespace

    def restore(self, create_project=True, visibility=None, key=None):
        if create_project:
            self.create_project(workspace=self.workspace, name=self.project_name, key=key, visibility=visibility)
        project_structure, project = self.setup_project_upload()
        self.traverse_local_structure(project_structure, project, self.source)
        # TODO implement multiprocessing
        for source_run in self.source.iterdir():
            if source_run.is_dir():
                run_structure, run = self.setup_run_upload(source_run)
                self.traverse_local_structure(run_structure, run, source_run)
                run.stop()

    def create_project(self, workspace, name=None, key=None, visibility=None):
        with (self.source / utils.PROJECT_STRUCTURE).open('r') as file:
            project_info = json.load(file)
        if not name:
            name = project_info['atoms']['sys/name']
        if not key:
            key = project_info['atoms'].get('sys/key')
        if not visibility:
            visibility = project_info['atoms']['sys/visibility']
        # check values
        if not is_value_in_class_attributes(visibility, management.ProjectVisibility):
            print(f'Value for visibility "{visibility}" is not allowed. Setting to '
                  f'"{management.ProjectVisibility.PRIVATE}"')
            visibility = management.ProjectVisibility.PRIVATE
        management.create_project(workspace=workspace, name=name, key=key, visibility=visibility)

    def setup_run_upload(self, source):
        with (source / utils.RUN_STRUCTURE).open('r') as file:
            run_structure = json.load(file)
        run = neptune.init_run(project=self.project_id, mode='async', capture_stderr=False, capture_traceback=False,
                               capture_stdout=False, capture_hardware_metrics=False, source_files=[], git_ref=False)
        return run_structure, run

    def setup_project_upload(self):
        with (self.source / utils.PROJECT_STRUCTURE).open('r') as file:
            project_structure = json.load(file)
        project = neptune.init_project(self.project_id)
        return project_structure, project

    def traverse_local_structure(self, remote_structure, neptune_object, source):
        self.traverse_atoms(remote_structure[RemoteKeys.ATOMS.value], neptune_object)
        self.traverse_timestamps(remote_structure[RemoteKeys.TIME_STAMPS.value], neptune_object)
        self.traverse_float_series(remote_structure[RemoteKeys.FLOAT_SERIES.value], neptune_object, source)
        self.traverse_string_series(remote_structure[RemoteKeys.STRING_SERIES.value], neptune_object, source)
        self.traverse_files(remote_structure[RemoteKeys.FILES.value], neptune_object, source)
        self.traverse_string_sets(remote_structure[RemoteKeys.STRING_SETS.value], neptune_object)
        self.traverse_file_sets(remote_structure[RemoteKeys.FILE_SETS.value], neptune_object, source)
        self.traverse_file_series(remote_structure[RemoteKeys.FILE_SERIES.value], neptune_object, source)

    def traverse_atoms(self, atoms, neptune_object):
        for key in atoms.keys():
            if not is_read_only_field(key):
                neptune_object[key] = atoms[key]
            elif self.alternative_sys_namespace:
                neptune_object[self.alternative_sys_namespace + '/' + key] = atoms[key]

    def traverse_timestamps(self, atoms, neptune_object):
        for key in atoms.keys():
            if not is_read_only_field(key):
                neptune_object[key] = datetime.fromtimestamp(atoms[key])
            elif self.alternative_sys_namespace:
                neptune_object[self.alternative_sys_namespace + '/' + key] = datetime.fromtimestamp(atoms[key])

    @staticmethod
    def traverse_string_sets(string_sets, neptune_object):
        for key in string_sets.keys():  # only supported sting set field is sys/tags
            string_set = string_sets[key]
            if len(string_set) > 0:
                neptune_object[key].add(string_set)

    @staticmethod
    def traverse_files(files, neptune_object, source):
        for key in files.keys():
            neptune_object[key].upload(str(source / files[key]))

    @staticmethod
    def traverse_file_sets(file_sets, neptune_object, source):
        for key in file_sets.keys():
            neptune_object[key].upload_files(str(source / file_sets[key]))

    @staticmethod
    def traverse_file_series(file_series, neptune_object, source):
        for key in file_series.keys():
            for file in (source / file_series[key]).iterdir():
                if file.is_file():
                    neptune_object[key].append(File(str(file)))

    @staticmethod
    def traverse_string_series(series, neptune_object, source):
        for key in series.keys():
            if not series[key] is None:  # see comment on fetch_series in archiver.py
                series_df = pd.read_csv(filepath_or_buffer=source / series[key], na_filter=False)
                neptune_object[key].extend(values=series_df['value'].tolist(), steps=series_df['step'].tolist(),
                                           timestamps=series_df['timestamp'].tolist())

    @staticmethod
    def traverse_float_series(series, neptune_object, source):
        for key in series.keys():
            if not series[key] is None:  # see comment on fetch_series in archiver.py
                series_df = pd.read_csv(filepath_or_buffer=source / series[key])
                neptune_object[key].extend(values=series_df['value'].tolist(), steps=series_df['step'].tolist(),
                                           timestamps=series_df['timestamp'].tolist())

    def get_project_name(self, project_name):
        if project_name is None:
            print('No project-name argument given. Fetching project-name argument from archive.')
            with (self.source / utils.PROJECT_STRUCTURE).open('r') as file:
                project_structure = json.load(file)
            project_name = project_structure['atoms']['sys/name']
            print(f'Using {project_name} as project name.')
        return project_name

    def get_workspace(self, workspace):
        if workspace is None:
            print('No workspace argument given. Fetching workspace argument from archive.')
            with (self.source / utils.ARCHIVE_INFO).open('r') as file:
                archive_info = json.load(file)
            workspace = archive_info['workspace']
            print(f'Using {workspace} as project name.')
        return workspace

