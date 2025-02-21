import pytest
import datetime
import numpy as np
import subprocess


from ddb.db import DatabaseManager
from ddb.session import Session
from ddb.parser import parse_all

testcase_dir = "tests/qo/"
T = 0 # 9 total number of test cases
times = 1

@pytest.fixture
def session():
    dbm = DatabaseManager(
        db_dir = DatabaseManager.DEFAULT_DB_DIR,
        tmp_dir = DatabaseManager.DEFAULT_TMP_DIR
    )
    s = Session(dbm)
    yield s

def helper(session, capsys, preamble, sql_file, times):
    for parse_tree in parse_all(preamble + '\nSET AUTOCOMMIT OFF;'):
        r = session.request(parse_tree)
        assert r.error is None, f"{r.error}"
    latency = []
    est_io = []
    real_io = []
    result_lines = []
    with open(sql_file) as f:
        for command_id, parse_tree in enumerate(parse_all(f.read())):
            capsys.readouterr()
            r = session.request(parse_tree) # timing of the first execution is ignored
            captured = capsys.readouterr()
            assert r.error is None, f"{r.error}"
            if r.r_pop is None:
                continue
            else:
                result_lines = captured.out.split('\n')
            for i in range(times):
                r = session.request(parse_tree)
                assert r.error is None, f"{command_id}:{i}: {r.error}"
                assert r.r_pop is not None, f"{command_id}:{i}: {r} {parse_tree}"
                latency.append(r.r_pop.measured.ns_elapsed.sum/1000000)
                est_io.append(r.r_pop.estimated_cost)
                real_io.append(r.r_pop.measured.sum_blocks.overall)
    for parse_tree in parse_all('ROLLBACK;'):
        r = session.request(parse_tree)
        assert r.error is None, f"{r.error}"
    return np.median(np.array(latency)), min(est_io), min(real_io), result_lines

def create_table(session):
    file_name = ['create_table_100.sql', 'create_table_1000.sql', 'create_table_10000.sql', 'create_table_100000.sql']
    for file in file_name:
        create_table = open(testcase_dir + file)
        for command_id, parse_tree in enumerate(parse_all(create_table.read())):
            r = session.request(parse_tree)
            assert r.error is None, f"{command_id}: {r.error}"

@pytest.mark.parametrize("t_id", list(range(1, T+1)))
def test_session(session, capsys, t_id):
    subprocess.run(['make', 'clean'], check=True)
    create_table(session)
    fsql = testcase_dir + f"q{t_id}.sql"
    opt_option = 'SMART'
    preamble = f'''
SET PLANNER {opt_option};
ANALYZE;
'''
    preamble_baseline = '''
SET PLANNER BASELINE;
ANALYZE;
'''
    avg_latency, est_io, real_io, result_lines = helper(session, capsys, preamble, fsql, times)
    avg_latency_baseline, est_io_baseline, real_io_baseline, result_lines_baseline = helper(session, capsys, preamble_baseline, fsql, times)
    print(f"Test{t_id}: latency (ms) {opt_option}={avg_latency} BASELINE={avg_latency_baseline}")
    print(f"Test{t_id}: real I/O {opt_option}={real_io} BASELINE={real_io_baseline}")
    print(f"Test{t_id}: est I/O {opt_option}={est_io} BASELINE={est_io_baseline}")
    # assuming only the last line of the result matters:
    print(f"Test{t_id}: query result {opt_option}={result_lines[-1]}")
    print(f"Test{t_id}: query result BASELINE={result_lines_baseline[-1]}")
    assert result_lines[-1] == result_lines_baseline[-1], \
        f"Test{t_id}: query result is wrong:\n" +\
            f"{opt_option}={result_lines[-1]}\n" +\
            f"BASELINE={result_lines_baseline[-1]}"
    assert est_io < est_io_baseline, \
        f"Test{t_id}: est I/O {opt_option}={est_io} BASELINE={est_io_baseline}"
    # skipped measured I/Os and latencies because they are too dependent on cardinality estimation
