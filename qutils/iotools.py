import json
import os

import pandas as pd
import yaml

try:
    import pypyodbc as pyodbc
except ImportError:
    import pyodbc
pyodbc.pooling = False


def load_yaml(yaml_path):
    with open(yaml_path, 'r') as f:
        loaded = yaml.load(f.read())
    return loaded


def load_json(json_path):
    with open(json_path, 'r') as f:
        loaded = json.load(f)
    return loaded


def save_yaml(data, yaml_path):
    with open(yaml_path, 'w') as f:
        f.write(yaml.dump(data, default_flow_style=False))


def save_json(data, json_path):
    with open(json_path, 'w') as f:
        json.dump(data, f)


def reverse_readline(filename, buf_size=8192):
    """a generator that returns the lines of a file in reverse order"""
    with open(filename) as fh:
        segment = None
        offset = 0
        fh.seek(0, os.SEEK_END)
        total_size = remaining_size = fh.tell()
        while remaining_size > 0:
            offset = min(total_size, offset + buf_size)
            fh.seek(-offset, os.SEEK_END)
            buffer = fh.read(min(remaining_size, buf_size))
            remaining_size -= buf_size
            lines = buffer.split('\n')
            # the first line of the buffer is probably not a complete line so
            # we'll save it and append it to the last line of the next buffer
            # we read
            if segment is not None:
                # if the previous chunk starts right from the beginning of line
                # do not concact the segment to the last line of new chunk
                # instead, yield the segment first
                if buffer[-1] != '\n':
                    lines[-1] += segment
                else:
                    yield segment
            segment = lines[0]
            for index in range(len(lines) - 1, 0, -1):
                yield lines[index]
        yield segment


class Teradata(object):
    _conn_pool = {}

    def __init__(self, host, user_name, password, database=None, table=None):
        super(Teradata, self).__init__()
        self.host = host
        self.user_name = user_name
        self.password = password
        self.database = database
        self.table = table

    def _get_conn(self):
        conn = self._conn_pool.get((self.host, self.user_name))
        if conn is None:
            conn = pyodbc.connect('DRIVER={{Teradata}};DBCNAME={};UID={};PWD={};'
                                  .format(self.host, self.user_name, self.password),
                                  ansi=True, unicode_results=False)
            # self._conn_pool[(self.host, self.user_name)] = conn
        return conn

    def query(self, query_string=None,
              select=None, distinct=False, where=None, order_by=None, ascend=True, limit=None,
              database=None, table=None):
        """
        Only for specific use. Run arbitary query and return a pandas table
        """
        if query_string is None:
            if database is None: database = self.database
            if table is None: table = self.table
            clause_select = 'SELECT {} {} {}'.format('DISTINCT' if distinct else '',
                                                     '' if limit is None else 'TOP {}'.format(limit),
                                                     '*' if select is None else select)
            clause_from = 'FROM {}.{}'.format(database, table)
            clause_where = '' if where is None else 'WHERE {}'.format(where)
            clause_order_by = '' if order_by is None else 'ORDER BY {} {}'.format(order_by, 'ASC' if ascend else 'DESC')
            query_string = ' '.join((clause_select, clause_from, clause_where, clause_order_by)) + ';'
        result = pd.read_sql(query_string, self._get_conn())
        return result.rename(columns=str.upper)

    def upsert(self, dataframe, on=(), database=None, table=None):
        """
        Only for specific use.
        """
        if dataframe.shape[0] == 0:
            return
        database = database or self.database
        table = table or self.table
        query_insert_table_schema = ', '.join(dataframe.columns)
        query_insert_value_param = ', '.join(['?'] * dataframe.columns.size)
        if on:
            if isinstance(on, str):
                on = (on,)
            query_update_where_clause = ' AND '.join(col + ' = ?' for col in on)
            query_update_set_columns = list(dataframe.columns)
            for col in on:
                query_update_set_columns.remove(col)
            query_update_set_clause = ', '.join(col + ' = ?' for col in query_update_set_columns)
            query = \
                """
                UPDATE {database}.{table}
                    SET {query_update_set_clause}
                    WHERE {query_update_where_clause}
                ELSE
                    INSERT INTO {database}.{table} ({query_insert_table_schema})
                    VALUES ({query_insert_value_param});
                """.format(database=database, table=table,
                           query_update_set_clause=query_update_set_clause,
                           query_update_where_clause=query_update_where_clause,
                           query_insert_table_schema=query_insert_table_schema,
                           query_insert_value_param=query_insert_value_param)
        else:
            query = \
                """
                INSERT INTO {database}.{table} ({query_insert_table_schema})
                VALUES ({query_insert_value_param});
                """.format(database=database, table=table,
                           query_insert_table_schema=query_insert_table_schema,
                           query_insert_value_param=query_insert_value_param)
        conn = self._get_conn()
        cursor = conn.cursor()
        for i_row in range(dataframe.shape[0]):
            row = dataframe.iloc[i_row]
            query_params = []
            if on:
                query_params.extend(row[col] for col in query_update_set_columns)
                query_params.extend(row[col] for col in on)
            query_params.extend(row)
            cursor.execute(query, query_params)
        conn.commit()

    def delete(self, where=None, database=None, table=None):
        """
        Only for specific use. Run DELETE command.
        """
        database = database or self.database
        table = table or self.table
        conn = self._get_conn()
        cursor = conn.cursor()
        if where:
            query = \
                """
                DELETE FROM {database}.{table} WHERE {where};
                """.format(database=database, table=table, where=where)
        else:
            query = \
                """
                DELETE FROM {database}.{table};
                """.format(database=database, table=table)
        cursor.execute(query)
        conn.commit()

    def commit(self, query_string):
        """
        Only for specific use. Run arbitary command that needs commit.
        """
        conn = self._get_conn()
        cursor = conn.cursor()
        cursor.execute(query_string)
        conn.commit()
