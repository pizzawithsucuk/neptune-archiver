from enum import Enum


class RemoteKeys(Enum):
    ATOMS = 'atoms'
    FILES = 'files'
    FILE_SETS = 'file_sets'
    FILE_SERIES = 'file_series'
    TIME_STAMPS = 'time_stamps'
    FLOAT_SERIES = 'float_series'
    STRING_SERIES = 'string_series'
    STRING_SETS = 'string_sets'


PROJECT_STRUCTURE = 'project_structure.json'
ARCHIVE_INFO = '.archive_info'
RUN_STRUCTURE = 'run_structure.json'
RUNS_TABLE = 'runs_table.csv'

# TODO check if this is correct, project/run may be different
NEPTUNE_READ_ONLY_FIELDS = {'sys/id', 'sys/monitoring_time', 'sys/owner', 'sys/running_time', 'sys/size', 'sys/trashed',
                            'sys/name', 'sys/visibility', 'sys/creation_time', 'sys/modification_time', 'sys/ping_time'}


def is_read_only_field(field):
    return field in NEPTUNE_READ_ONLY_FIELDS


def is_value_in_class_attributes(value, cls):
    for attr_name, attr_value in vars(cls).items():
        if attr_value == value:
            return True
    return False
