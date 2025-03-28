import pytest
import datetime
import subprocess

from ddb.db import DatabaseManager
from ddb.session import Session
from ddb.parser import parse_all

testcase_dir = "tests/aggr/"
T = 12 # 21 total number of test cases

@pytest.fixture
def session():
    dbm = DatabaseManager(
        db_dir = DatabaseManager.DEFAULT_DB_DIR,
        tmp_dir = DatabaseManager.DEFAULT_TMP_DIR
    )
    s = Session(dbm)
    yield s

def load_answer(t_id):
    ans_filename = f"t_aggr_{t_id}.ans"
    answer, aggr_answer = None, None
    with open(testcase_dir + ans_filename, "r") as fans:
        # From t_aggr_{t_id}, load commands' responses and aggr content to answer and aggr_answer respectively
        answer, aggr_answer = [], []
        lines = list(fans.readlines())
        i = 0
        while i < len(lines):
            line = lines[i]
            line = line.lstrip('(').rstrip(')\n')
            answer.append((str(line.split(", ")[0]), eval(line.split(", ")[1])))
            if answer[-1][0] == "SELECT":
                # load all the aggr content
                for j in range(answer[-1][1]):
                    line = lines[i + 1 + j]
                    aggr_answer.append(eval(line[:-1]))
                i = i + answer[-1][1]
            i += 1
    return answer, aggr_answer

@pytest.mark.parametrize("t_id", list(range(T)))
def test_session(session, capsys, t_id):
    subprocess.run(['make', 'clean'], check=True)
    with open(testcase_dir + f"t_aggr_{t_id}.sql") as fsql:
        answer, aggr_answer = load_answer(t_id)
        assert answer is not None and aggr_answer is not None, f"Test{t_id}: faild to load answer file"
        # Check command one by one
        command_id = 0
        for parse_tree in parse_all(fsql.read()):
            r = session.request(parse_tree)
            assert r.error is None, f"Test{t_id}, command{command_id}: got error: {r.error_details}"
            assert r.response.startswith(answer[command_id][0]), f"Test{t_id}, command{command_id}: incorrect command."
            if r.response.startswith("SELECT"):
                # check the aggr count
                assert int(r.response.split("\n")[0].split(" ")[1]) == answer[command_id][1], f"Test{t_id}, command{command_id}: incorrect aggr count."
                # check the aggr content
                aggr_result = capsys.readouterr().out.split("\n")[1:-1]
                aggr_result = [list(eval(item, {'datetime' : datetime})) for item in aggr_result]
                for row_i in range(len(aggr_result)):
                    for col_i in range(len(aggr_result[row_i])):
                        if isinstance(aggr_result[row_i][col_i], datetime.datetime):
                            aggr_result[row_i][col_i] = aggr_result[row_i][col_i].strftime("%Y-%m-%d")
                    aggr_result[row_i] = tuple(aggr_result[row_i])
                aggr_result.sort()
                for i in range(len(aggr_result)):
                    if aggr_result[i] == aggr_answer[i]:
                        continue
                    for j in range(len(aggr_result[i])):
                        if (type(aggr_result[i][j]) != float and aggr_result[i][j] != aggr_answer[i][j]) or (type(aggr_result[i][j]) == float and abs(aggr_result[i][j]-aggr_answer[i][j]) > 1e-6):
                            assert aggr_result == aggr_answer, f"Test{t_id}, command{command_id}: incorrect aggr content"
            command_id += 1
        # check if all commands are fully executed
        assert command_id == len(answer), f"Test{t_id}: executed {command_id} commands <> total {len(answer)} commands"


