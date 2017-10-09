import json
import os
import subprocess
import sys
import tempfile

import difflib
import itertools
import logging
import pprint
from decimal import Decimal
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
)

logger = logging.getLogger(__name__)


def init_logger(log) -> None:
    log_level = logging.DEBUG
    log.setLevel(log_level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    formatter = logging.Formatter('%(asctime)s at=%(levelname)-8s logger=%(name)s %(message)s')
    ch.setFormatter(formatter)
    log.addHandler(ch)


init_logger(logger)


PSQL = '/usr/local/bin/psql'  # type: str
HOST = 'ec2-54-197-230-161.compute-1.amazonaws.com'  # type: str
PORT = '5432'  # type: str
USER = 'lpyddiodqoideu'  # type: str
DATABASE = 'dfotkp65ih0clo'  # type: str

DIFF_CONTEXT = sys.maxsize  # type: int  # 0

DIFF_DIRECTORY = 'output_diffs'  # type: str
DIFF_FULL = 'FULL'  # type: str
DIFF_MINIMAL = 'MINIMAL'  # type: str
DIFF_SIGNIFICANT = 'SIGNIFICANT'  # type: str  # only nodes with significant value differences (>1p)
DIFF_TYPES = (DIFF_FULL, DIFF_MINIMAL, DIFF_SIGNIFICANT)  # type: Tuple[str, str, str]

START_DATE = '2017, 08, 08, 00, 00, 00.0'  # type: str  # 1.5 hours after cache flushing fix was merged to master
UNDEFINED_NODE = 'project.graph.nodes.Undefined'  # type: str
PENCE = Decimal('0.01')  # type: Decimal
SIGNIFICANT_ONLY = True  # type: bool
ID_WHITELIST = set()  # type: Set[int]  # IDs of the only results we care about
ID_BLACKLIST = set()  # type: Set[int]  # IDs of results we don't care about, e.g. duplicates

ID = int
Node = Tuple[Any, ...]
Store = Dict[Node, Any]
StoreDecimal = Dict[Node, Decimal]
Result = Dict[str, Any]
Data = Dict

SQL_COMMAND_COUNT_FAILURES = ' '.join([
    'SELECT count(*)',
    'FROM failures',
    'WHERE t >= make_timestamp({})'.format(START_DATE),
])  # type: str
SQL_COMMAND_GET_IDS = ' '.join([
    'SELECT id',
    'FROM failures',
    'WHERE t >= make_timestamp({})'.format(START_DATE),
    'ORDER BY t DESC',
])  # type: str
SQL_COMMAND_GET_STATS = ' '.join([
    'SELECT sum(success) AS success, sum(failure) AS failure',
    'FROM matches',
    'WHERE date >= make_timestamp({})'.format(START_DATE),
])  # type: str
SQL_COMMAND_GET_DATA_ID = ' '.join([
    'SELECT data',
    'FROM failures',
    'WHERE id = {id}',
])  # type: str
SQL_COMMAND_GET_DATA = ' '.join([  # unused
    'SELECT data',
    'FROM failures',
    'WHERE t >= make_timestamp({})'.format(START_DATE),
    'ORDER BY t DESC',
    'OFFSET {offset}',
    'LIMIT {limit}',
])  # type: str


BASH_COMMAND_NO_OUTPUT_TO_FILE = "{psql} -h {host} -p {port} -U {user} -d {database} -qAt -c '{sql_command}'"
BASH_COMMAND_OUTPUT_TO_FILE = BASH_COMMAND_NO_OUTPUT_TO_FILE + ' > {path}'


def run_bash(bash: str) -> str:  # PSQL commands require credentials in .pgpass
    output_bytes = subprocess.check_output(bash, shell=True)  # type: bytes
    output_string = output_bytes.decode('utf-8')  # type: str
    return output_string


def filter_whitelist(ids: Set[ID], whitelist: Set[ID]) -> Set[ID]:
    if whitelist:
        logger.info('Only querying failure IDs from whitelist: {}'.format(whitelist))
        ids = ids.intersection(whitelist)
    return ids


def filter_blacklist(ids: Set[ID], blacklist: Set[ID]) -> Set[ID]:
    if blacklist:
        logger.info('Ignoring failure IDs from blacklist: {}'.format(blacklist))
        ids = ids.difference(blacklist)
    return ids


def query_failure_ids() -> List[ID]:
    bash = build_bash_command(BASH_COMMAND_NO_OUTPUT_TO_FILE, SQL_COMMAND_GET_IDS)  # type: str
    output = run_bash(bash)  # type: str
    id_strings = set(output.split())  # type: Set[str]
    id_set = {int(id_) for id_ in id_strings}  # type: Set[ID]
    id_set = filter_whitelist(id_set, ID_WHITELIST)
    id_set = filter_blacklist(id_set, ID_BLACKLIST)
    id_list = list(sorted(id_set))  # type: List[ID]
    if id_list:
        logger.info('Failure IDs: {}'.format(id_list))
    else:
        logger.info('No failures found')
    return id_list


def query_failure_data(id_: ID, path: str) -> Result:
    bash = build_bash_command(BASH_COMMAND_OUTPUT_TO_FILE, SQL_COMMAND_GET_DATA_ID.format(id=id_), path)  # type: str
    run_bash(bash)
    data = read_data(path)  # type: Data
    return {
        'id': id_,
        'data': data,
    }


def query_failures() -> int:
    bash = build_bash_command(BASH_COMMAND_NO_OUTPUT_TO_FILE, SQL_COMMAND_COUNT_FAILURES)  # type: str
    output = run_bash(bash)  # type: str
    output = output.strip()
    count = int(output)  # type: int
    return count


def query_successes_failures() -> Tuple[int, int]:
    bash = build_bash_command(BASH_COMMAND_NO_OUTPUT_TO_FILE, SQL_COMMAND_GET_STATS)  # type: str
    output = run_bash(bash)  # type: str
    successes, failures = map(lambda s: '0' if s == '' else s, output.strip().split('|'))  # type: Tuple[str, str]
    return int(successes), int(failures)


def build_bash_command(bash: str, sql: str, path: str='/dev/null') -> str:
    return bash.format(
        psql=PSQL,
        host=HOST,
        port=PORT,
        user=USER,
        database=DATABASE,
        sql_command=sql,
        path=path,
    )


def read_data(path: str) -> Dict:
    with open(path) as f:
        content_lines = f.readlines()
    content = ''.join(content_lines)  # type: str
    data = json.loads(content)  # type: Data
    return data


def get_result(index: int, id_: ID, count: int) -> Result:
    with tempfile.NamedTemporaryFile() as file:
        path = file.name
        logger.info('Fetching data for failure {}, {} of {} in file {}'.format(id_, index + 1, count, path))
        result = query_failure_data(id_, path)  # type: Result
    significant = print_is_significant(result)  # type: bool
    if not significant and SIGNIFICANT_ONLY:
        logger.info('Skipping insignificant result {}'.format(result['id']))
    else:
        diff_result(result, diff_type=DIFF_FULL)
        diff_result(result, diff_type=DIFF_MINIMAL)
        diff_result(result, diff_type=DIFF_SIGNIFICANT)
    return result


def get_results() -> List[Result]:
    print_stats()
    ids = query_failure_ids()  # type: List[ID]
    failures = len(ids)  # type: int
    total_failures = query_failures()  # type: int
    assert total_failures - len(ID_BLACKLIST) <= failures <= total_failures
    diff_subdirectories = [directory.lower() for directory in DIFF_TYPES]
    make_diff_dirs(DIFF_DIRECTORY, diff_subdirectories)
    return [
        get_result(index, id_, failures)
        for index, id_ in enumerate(ids)
    ]


def print_stats() -> None:
    successes, failures = query_successes_failures()  # type: Tuple[int, int]
    total = successes + failures  # type: int
    if total == 0:
        logger.warning('No reports found')
        return
    found_failures = query_failures()  # type: int
    if failures != found_failures:
        logger.warning('{} failures reported, but {} found'.format(failures, found_failures))
    rate = successes / total * 100  # type: float
    logger.info('{successes}/{total} successes ({failures} failures) {rate}% success rate'.format(
        successes=successes,
        failures=failures,
        total=total,
        rate=rate,
    ))


def delete_files_in_directory(directory) -> None:
    for filename in os.listdir(directory):
        file = os.path.join(directory, filename)  # type: str
        try:
            if os.path.isfile(file):
                os.unlink(file)
        except IOError:
            logger.error('Error deleting file ()'.format(os.path.join(directory, filename)))


def make_diff_dir(diff_directory: str) -> None:
    if not os.path.exists(diff_directory):
        os.makedirs(diff_directory)
    else:
        delete_files_in_directory(diff_directory)


def make_diff_dirs(diff_directory: str, diff_subdirectories: List[str]) -> None:
    make_diff_dir(diff_directory)
    for diff_subdirectory in diff_subdirectories:
        diff_subdirectory = os.path.join(diff_directory, diff_subdirectory)
        make_diff_dir(diff_subdirectory)


def write_diff(diff_directory: str, diff_subdirectory: str, id_: int, diff: str) -> None:
    filename = '{}.diff'.format(id_)  # type: str
    path = os.path.join(diff_directory, diff_subdirectory, filename)  # type: str
    if os.path.exists(path):
        os.remove(path)
    logger.info('Writing diff to {}'.format(path))
    diff_file = open(path, 'w')
    diff_file.write(diff + '\n')


def format_value(value: Decimal) -> str:
    return str(float(value)).rstrip('0').rstrip('.')


def format_observation_value(observation_value: StoreDecimal) -> List[str]:
    observation_formatted = {
        key: format_value(value)
        for key, value in observation_value.items()
    }  # type: Dict[Node, str]
    diff = pprint.PrettyPrinter().pformat(observation_formatted)  # type: str
    diff_lines = diff.split('\n')  # type: List[str]
    return diff_lines


def eval_value(value: Optional[str]) -> Decimal:
    if UNDEFINED_NODE in value:
        ret = Decimal('nan')
    else:
        ret = eval(value)
        if not isinstance(ret, Decimal):
            ret = Decimal(ret)
    ret = ret.normalize()
    return ret


def parse_values(store: Store) -> StoreDecimal:
    return {k: eval_value(v) for k, v in store.items()}


def get_result_fields(result: Result, diff_type=DIFF_FULL) -> Tuple[int, Data, StoreDecimal, StoreDecimal]:
    id_ = result['id']  # type: ID
    data = result['data']  # type: Data
    control = data['control']  # type: Result
    observations = data['observations']  # type: List[Result]

    assert len(observations) == 1
    observation = observations[0]  # type: Result

    control_value = control['value']  # type: Store
    observation_value = observation['value']  # type: Store

    if diff_type != DIFF_FULL:
        # Get only common nodes. Only common nodes would use the cache.
        cached_nodes = set(observation_value.keys()).intersection(control_value)  # type: Set[Node]
        control_value = {k: v for k, v in control_value.items() if k in cached_nodes}
        observation_value = {k: v for k, v in observation_value.items() if k in cached_nodes}

    control_value = parse_values(control_value)
    observation_value = parse_values(observation_value)
    if diff_type == DIFF_SIGNIFICANT:
        control_value, observation_value = remove_insignificant(control_value, observation_value)

    return id_, data, control_value, observation_value


def alternate_subtractions_and_additions(diff_lines: List[str]) -> List[str]:
    subtractions = sorted(filter(lambda x: x.startswith('- "'), diff_lines))  # type: List[str]
    additions = sorted(filter(lambda x: x.startswith('+ "'), diff_lines))  # type: List[str]
    assert len(subtractions) == len(additions)
    diff_lines = list(itertools.chain.from_iterable(zip(subtractions, additions)))
    return diff_lines


def additions_and_subtractions_only(diff_lines: List[str]) -> List[str]:
    return [line for line in diff_lines if line.startswith('+ "') or line.startswith('- "')]


def remove_insignificant(control: Store, observation: Store) -> Tuple[Store, Store]:
    assert nodes_are_equal(control, observation)
    significant_control = dict(filter(
        lambda item: not is_about_equal(control[item[0]], observation[item[0]]), control.items(),
    ))  # type: Store
    significant_observation = dict(filter(
        lambda item: not is_about_equal(control[item[0]], observation[item[0]]), observation.items(),
    ))  # type: Store
    return significant_control, significant_observation


def format_diff(diff_lines: List[str], diff_type: str=DIFF_FULL) -> str:
    if diff_type != DIFF_FULL:
        diff_lines = additions_and_subtractions_only(diff_lines)
        diff_lines = alternate_subtractions_and_additions(diff_lines)
    diff = '\n'.join(diff_lines)  # type: str
    return diff


def nodes_are_equal(store1: Store, store2: Store) -> bool:
    return len(store1) == len(store2) == len(set(store1.keys()).union(set(store2.keys())))


def is_about_equal(control: str, observation: str) -> bool:
    return Decimal(control).quantize(PENCE) == Decimal(observation).quantize(PENCE)


def is_significant(control: Store, observation: Store) -> bool:
    assert nodes_are_equal(control, observation)
    return not all(is_about_equal(control[node], observation[node]) for node in control.keys())


def print_is_significant(result: Result) -> bool:
    id_, _, control, observation = get_result_fields(
        result, diff_type=DIFF_MINIMAL)  # type: Tuple[ID, Any, StoreDecimal, StoreDecimal]
    significant = is_significant(control, observation)
    logger.info('Result {} is {}significant'.format(id_, '' if significant else 'not '))
    return significant


def diff_result(result: Result, diff_type: str=DIFF_FULL) -> None:
    """
    Convert the experiment result into a diff and write it to a file.
    :param result: the diff result
    :param diff_type: the diff type
    """
    id_, data, control, observation = get_result_fields(
        result, diff_type=diff_type)  # type: Tuple[ID, Data, StoreDecimal, StoreDecimal]
    control_formatted = format_observation_value(control)
    observation_formatted = format_observation_value(observation)
    diff_lines = list(difflib.unified_diff(
        control_formatted, observation_formatted, fromfile='expected', tofile='actual', n=DIFF_CONTEXT,
    ))  # type: List[str]
    diff = format_diff(diff_lines, diff_type=diff_type)  # type: str
    if diff:
        diff_subdirectory = diff_type.lower()  # type: str
        write_diff(DIFF_DIRECTORY, diff_subdirectory, id_, diff)


results = get_results()  # type: List[Result]
