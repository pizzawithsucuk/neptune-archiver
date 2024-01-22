import neptune
from pathlib import Path
from neptune.attributes import FileSet, Boolean, Datetime, File, Float, GitRef, Integer, NotebookRef, RunState, String, \
    Artifact, FloatSeries, StringSeries, FileSeries, StringSet
import uuid
import json
import pandas as pd
from src import __version__
from datetime import datetime
import config


class RunArchiver:

    def __init__(self, project_id, run_id, project_path):
        self.project_id = project_id
        self.run_id = run_id
        self.run_path = Path(project_path / run_id)
        self.run_path.mkdir(exist_ok=True)

    def archive(self):
        run = neptune.init_run(with_id=self.run_id, project=self.project_id, mode='read-only')
        run_structure = run.get_structure()
        traversed_run_structure = traverse_structure(run_structure, path=self.run_path)
        with (self.run_path / config.RUN_STRUCTURE).open(mode='w') as json_file:
            json.dump(traversed_run_structure, json_file, indent=4)
        run.stop()


def traverse_structure(structure, path):
    cloned_structure = {}
    for key in structure.keys():
        if isinstance(structure[key], dict):
            cloned_structure[key] = traverse_structure(structure[key], path)
        elif isinstance(structure[key], (RunState, GitRef)):
            pass  # run state is not mutable on client side and should not be stored
            # ignore gitref for now
        else:
            cloned_structure[key] = {'type': str(type(structure[key])), 'value': fetch(structure[key], path)}
    return cloned_structure


def fetch_series(series, destination: Path):
    values = series.fetch_values()
    file_id = str(uuid.uuid4()) + '.csv'
    values.to_csv(path_or_buf=destination / file_id, index=False)
    return file_id


def fetch_fileset(fileset, destination: Path):
    file_id = str(uuid.uuid4()) + '.zip'
    fileset.download(str(destination / file_id))
    return str(file_id)


def fetch_file(file, destination: Path):
    file_id = str(uuid.uuid4())
    file.download(str(destination / file_id))
    return str(file_id)


def fetch(value, destination: Path):
    if isinstance(value, (Boolean, Float, Integer, String)):
        return value.fetch()
    if isinstance(value, Datetime):
        return value.fetch().timestamp()
    if isinstance(value, StringSet):
        return list(value.fetch())
    elif isinstance(value, (FloatSeries, StringSeries)):
        return fetch_series(value, destination)
    elif isinstance(value, File):
        return fetch_file(value, destination)
    elif isinstance(value, FileSet):
        return fetch_fileset(value, destination)
    else:
        raise NameError("Unknown Type", value, " ", type(value))


class Archiver:

    def __init__(self, project_id, destination):
        self.project_id = project_id
        self.project = neptune.init_project(project=project_id, mode='read-only')
        self.runs_table = self.project.fetch_runs_table().to_pandas()
        self.run_ids = self.runs_table.loc[:, 'sys/id'].tolist()
        self.project_path = destination / self.project_id
        self.project_path.mkdir(parents=True, exist_ok=True)

    def archive(self):
        # TODO implement multiprocessing
        self.make_archive_info()
        self.archive_metadata()
        for run_id in self.run_ids:
            run_archiver = RunArchiver(self.project_id, run_id, self.project_path)
            run_archiver.archive()

    def archive_metadata(self):
        project_structure = self.project.get_structure()
        traversed_project_structure = traverse_structure(project_structure, path=self.project_path)
        with (self.project_path / config.PROJECT_STRUCTURE).open(mode='w') as json_file:
            json.dump(traversed_project_structure, json_file, indent=4)

    def make_archive_info(self):
        archive_info = {'archiver_version': __version__, 'datetime': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'neptune_version': neptune.__version__}
        with (self.project_path / config.ARCHIVE_INFO).open(mode='w') as json_file:
            json.dump(archive_info, json_file, indent=4)


if __name__ == '__main__':
    archiver = Archiver(project_id='tns/scratch', destination=Path('/Users/rathjjgf/Desktop'))
    archiver.archive()
