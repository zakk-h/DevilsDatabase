import sqlglot
from sqlglot import exp
import logging

SQL_DIALECT = sqlglot.Dialects.POSTGRES

class ParserException(Exception):
    pass

class TweakLogging:
    """A hack to disable ``sqlglot`` warning.
    """
    def __enter__(self):
        self.original_level = logging.getLogger('sqlglot').level
        if logging.getLogger().level > logging.DEBUG:
            logging.getLogger('sqlglot').setLevel(logging.ERROR)

    def __exit__(self, exc_type, exc_val, exc_tb):
        logging.getLogger('sqlglot').setLevel(self.original_level)

def parse(sql_str: str) -> exp.Expression:
    try:
        with TweakLogging():
            return sqlglot.parse_one(sql_str, dialect=SQL_DIALECT)
    except sqlglot.ParseError as e:
        raise ParserException('syntax error') from e

def parse_all(sql_str: str) -> list[exp.Expression]:
    try:
        with TweakLogging():
            trees = sqlglot.parse(sql_str, dialect=SQL_DIALECT)
            if trees is not None:
                return list(tree for tree in trees if tree is not None)
            else:
                return list()
    except sqlglot.ParseError as e:
        raise ParserException('syntax error') from e
