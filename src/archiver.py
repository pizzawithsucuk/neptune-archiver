import neptune
from pathlib import Path
from neptune.attributes import FileSet, Boolean, Datetime, File, Float, GitRef, Integer, NotebookRef, RunState, String, \
    Artifact, FloatSeries, StringSeries, FileSeries, StringSet
import uuid
import json
from src import __version__
import zipfile
import src.utils as utils
from src.utils import RemoteKeys
from typing import Optional
from neptune.exceptions import FetchAttributeNotFoundException
from concurrent.futures import ThreadPoolExecutor
import os
import logging


os.environ["TQDM_DISABLE"] = "1"  # disables TQDM output to make sys prints clearer


class Archiver:
    def __init__(self, destination: Path, archive_name=None, project_id: Optional[str] = None, num_threads=1):
        self.project_id = project_id
        self.project = neptune.init_project(project=self.project_id, mode='read-only')
        self.runs_table = self.project.fetch_runs_table().to_pandas()
        self.run_ids = self.runs_table.loc[:, 'sys/id'].tolist()
        if not archive_name:
            archive_name = self.project['sys/name'].fetch()
        self.destination = destination / archive_name
        self.num_threads = num_threads
        self.destination.mkdir()
        utils.configure_logging(self.destination / 'archiving.log')

    def archive(self, store_runs_table=True):
        self.make_archive_log()
        self.archive_project()
        self.archive_runs()
        if store_runs_table:
            self.runs_table.to_csv(path_or_buf=self.destination / utils.RUNS_TABLE, index=False)

    def archive_project(self):
        project_neptune_structure = self.project.get_structure()
        neptune_obj_archiver = NeptuneObjArchiver(self.destination)
        neptune_obj_archiver.archive(project_neptune_structure, utils.PROJECT_STRUCTURE)

    def archive_runs(self):
        with ThreadPoolExecutor(self.num_threads) as executor:
            for run_id in self.run_ids:
                executor.submit(self.archive_run, run_id)

    def archive_run(self, run_id):
        logging.info(f'Start archiving {run_id}')
        run = neptune.init_run(with_id=run_id, project=self.project_id, mode='read-only')
        run_neptune_structure = run.get_structure()
        (self.destination / run_id).mkdir()
        neptune_obj_archiver = NeptuneObjArchiver(destination=self.destination / run_id)
        neptune_obj_archiver.archive(run_neptune_structure, utils.RUN_STRUCTURE)
        run.stop()
        logging.info(f'Finished archiving {run_id}')

    def make_archive_log(self):
        logging.info(f'archiver_version: {__version__}')
        logging.info(f'neptune_version: {neptune.__version__}')
        logging.info(f'workspace:{self.project_id.split("/")[0]}')


class NeptuneObjArchiver:
    # Class is used to recursively crawl through a neptune object (run or project) and store all data at a local
    # directory
    def __init__(self, destination):
        self.local_structure = {remote_key.value: {} for remote_key in RemoteKeys}
        self.destination = destination

    def archive(self, neptune_structure, string_id):
        self.traverse_neptune_structure(neptune_structure)
        with (self.destination / string_id).open(mode='w') as json_file:
            json.dump(self.local_structure, json_file, indent=4)

    def traverse_neptune_structure(self, neptune_structure, concatenated_key=''):
        for key in neptune_structure.keys():
            if isinstance(neptune_structure[key], dict):
                self.traverse_neptune_structure(neptune_structure[key], concatenated_key + '/' + key)
            else:
                self.fetch(neptune_structure[key], concatenated_key + '/' + key)

    def fetch(self, value, concatenated_key):
        concatenated_key = concatenated_key[1:]  # remove first /
        if isinstance(value, (Boolean, Float, Integer, String)):
            self.local_structure[RemoteKeys.ATOMS.value][concatenated_key] = value.fetch()
        elif isinstance(value, Datetime):
            self.local_structure[RemoteKeys.TIME_STAMPS.value][concatenated_key] = value.fetch().timestamp()
        elif isinstance(value, StringSet):
            # TODO group tags bugged for some reason
            try:
                string_set = list(value.fetch())
            except FetchAttributeNotFoundException as exception:
                logging.warning(  f'During fetching {concatenated_key}, an exception has occurred. '
                                  f'Setting {concatenated_key} to empty list: {exception}')
                string_set = []
            self.local_structure[RemoteKeys.STRING_SETS.value][concatenated_key] = string_set
        elif isinstance(value, FloatSeries):
            self.local_structure[RemoteKeys.FLOAT_SERIES.value][concatenated_key] = self.fetch_series(value)
        elif isinstance(value, StringSeries):
            self.local_structure[RemoteKeys.STRING_SERIES.value][concatenated_key] = self.fetch_series(value)
        elif isinstance(value, File):
            self.local_structure[RemoteKeys.FILES.value][concatenated_key] = self.fetch_file(value)
        elif isinstance(value, FileSet):
            self.local_structure[RemoteKeys.FILE_SETS.value][concatenated_key] = self.fetch_fileset(value)
        elif isinstance(value, FileSeries):
            #  TODO Figure out how to deal with descriptions/names of file series elements
            self.local_structure[RemoteKeys.FILE_SERIES.value][concatenated_key] = self.fetch_file(value)
        elif isinstance(value, RunState):
            pass  # RunState should not be logged as it is not mutable on client side
        elif isinstance(value, GitRef):
            pass
            #  TODO neptune currently does not seem to support GitRref Querying
        else:
            raise NameError("Unknown Type", value, " ", type(value))

    def fetch_series(self, series):
        series_df = series.fetch_values()
        file_id = str(uuid.uuid4()) + '.csv'
        if not len(series_df.columns) == 0:  # neptune returns an empty dataframe with no columns for when a monitoring
            # string series is empty. Not sure what happens to other series empty series, so the condition is if there
            # are no column names. Then return None such that Restorer knows what to do.
            series_df['timestamp'] = series_df['timestamp'].apply(lambda x: x.to_pydatetime().timestamp())
            series_df.to_csv(path_or_buf=self.destination / file_id, index=False)
            return file_id
        return None

    def fetch_file_series(self, file_series):
        file_id = str(uuid.uuid4())
        file_series.download(str(self.destination / file_id))
        return file_id

    def fetch_fileset(self, fileset):
        file_id = str(uuid.uuid4())
        fileset.download(str(self.destination / (file_id + '.zip')))
        with zipfile.ZipFile(self.destination / (file_id + '.zip'), 'r') as zip_ref:
            zip_ref.extractall(self.destination / file_id)
        (self.destination / (file_id + '.zip')).unlink()
        return file_id

    def fetch_file(self, file):
        file_id = str(uuid.uuid4())
        file.download(str(self.destination / file_id))
        return file_id
