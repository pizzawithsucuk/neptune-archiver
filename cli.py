import argparse
from src.archiver import Archiver
from src.retriever import Retriever
from src.utils import *
from pathlib import Path


# TODO: implement option to store and reload form .zip etc. files
# TODO: exception handling

def archive(args):
    destination = args.destination
    destination = Path(destination) if destination else Path.cwd()
    archiver = Archiver(destination=destination, project_id=args.project_id, archive_name=args.archive_name)
    archiver.archive(store_runs_table=args.store_runs_table)


def retrieve(args):
    retriever = Retriever(Path(args.source), args.workspace, args.project_name, args.alternative_sys_namespace)
    retriever.restore((not args.no_project_creation), args.visibility, args.key)


def main():
    parser = argparse.ArgumentParser(description='neptune-archiver CLI')
    subparsers = parser.add_subparsers(dest='command')
    archive_parser = subparsers.add_parser('archive', help='Archive project')
    retrieve_parser = subparsers.add_parser('retrieve', help='Retrieve project from archive and upload it to neptune')

    # archive_parser arguments
    archive_parser.add_argument('--project-id', type=str, help="Name of a project in the form "
                                                               "`workspace-name/project-name`.If left empty,"
                                                               " the value of the NEPTUNE_PROJECT environment"
                                                               " variable is used.", default=None)
    archive_parser.add_argument('--destination', type=str, default=None,
                                help='directory where to store archive. if none provided use current working directory')
    archive_parser.add_argument('--archive_name', type=str, help='name of the archive, default name of '
                                                                 'the project is used', default=None)

    archive_parser.add_argument('--store-runs-table', action='store_true',
                                help='whether to include a copy of the runs_table')

    # retrieve_parser arguments
    retrieve_parser.add_argument('--source', type=str,
                                 help='path to neptune archive')
    retrieve_parser.add_argument('--alternative-sys-namespace', type=str, default=None,
                                 help='Namespace for read-only attributes of the archived project. If None, read-only '
                                      'attributes are not uploaded. Applies to both project and run data.')
    retrieve_parser.add_argument('--workspace', type=str, help='Workspace to upload')
    retrieve_parser.add_argument('--project-name', type=str, help='Project name to upload. If None, fetches project '
                                                                  'name from archive.', default=None)

    retrieve_parser.add_argument('--no-project-creation', action='store_true', help='whether to create a new project')

    retrieve_parser.add_argument('--key', type=str, default=None,
                                 help=f'Key to use when creating object. '
                                      f'If None, uses sys/key from {PROJECT_STRUCTURE}. Default: None')

    retrieve_parser.add_argument('--visibility', type=str, default=None,
                                 help=f'Visibility. If None, uses sys/visibility from {PROJECT_STRUCTURE}. '
                                      f'Default: None')
    args = parser.parse_args()

    if args.command == 'archive':
        archive(args)
    elif args.command == 'retrieve':
        retrieve(args)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
