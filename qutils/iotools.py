import json
import os

import pandas as pd
import teradata
import yaml

from qutils import VERSION


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

    pooling = True

    config = {
        "appName": __name__ + '.Teradata',
        "version": VERSION,
        "runNumber": "0",
        "configureLogging": False
    }

    _pool = {}

    def __init__(self, host, user_name, password, database=None, table=None, **connect_kwargs):
        super(Teradata, self).__init__()
        self.host = host
        self.user_name = user_name
        self.password = password
        self.database = database
        self.table = table

        self.connect_kwargs = connect_kwargs.copy()
        self.connect_kwargs['method'] = self.connect_kwargs.get('method', 'odbc')

    @property
    def session(self):
        session = None
        if self.pooling:
            session = self._pool.get((self.host, self.user_name))
        if session is None:
            session = self._new_session()
            if self.pooling:
                self._pool[(self.host, self.user_name)] = session
        return session

    def query(self, query_string=None,
              select=None, distinct=False, where=None, order_by=None, ascend=True, limit=None,
              database=None, table=None,
              **kwargs):
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
        result = self._query(query_string, **kwargs)
        if result.shape == (1, 1) and result.columns[0] in ('Request Text', 'RequestText') and result.index[0] == 0:
            return result.iat[0, 0]
        else:
            return result

    def upsert(self, data_frame, on=(), database=None, table=None, chunk_size=None, **kwargs):  # frequent used kwargs: batch=True
        if data_frame.shape[0] == 0:
            return
        database = database or self.database
        table = table or self.table
        query_insert_table_schema = ', '.join(data_frame.columns)
        query_insert_value_param = ', '.join(['?'] * data_frame.columns.size)
        if on:
            if isinstance(on, str):
                on = (on,)
            query_update_where_clause = ' AND '.join(col + ' = ?' for col in on)
            query_update_set_columns = list(data_frame.columns)
            for col in on:
                query_update_set_columns.remove(col)
            query_update_set_clause = ', '.join(col + ' = ?' for col in query_update_set_columns)
            query = \
                "UPDATE {database}.{table} " \
                "  SET {query_update_set_clause} " \
                "  WHERE {query_update_where_clause} " \
                "ELSE " \
                "  INSERT INTO {database}.{table} ({query_insert_table_schema}) " \
                "  VALUES ({query_insert_value_param}); ".format(database=database, table=table,
                                                                 query_update_set_clause=query_update_set_clause,
                                                                 query_update_where_clause=query_update_where_clause,
                                                                 query_insert_table_schema=query_insert_table_schema,
                                                                 query_insert_value_param=query_insert_value_param)
        else:
            query = "INSERT INTO {database}.{table} ({query_insert_table_schema}) " \
                    "VALUES ({query_insert_value_param});".format(database=database, table=table,
                                                                  query_insert_table_schema=query_insert_table_schema,
                                                                  query_insert_value_param=query_insert_value_param)

        def query_params(row):
            params = []
            if on:
                params.extend(row[col] for col in query_update_set_columns)
                params.extend(row[col] for col in on)
            params.extend(row)
            return [None if pd.isnull(v) or isinstance(v, float) and pd.np.isinf(v) else v for v in params]

        if chunk_size is None:
            chunk_size = data_frame.shape[0]

        chunk_pos = 0
        while chunk_pos < data_frame.shape[0]:
            data_chunk = data_frame.iloc[chunk_pos:chunk_pos + chunk_size]
            all_query_params = [query_params(row) for index, row in data_chunk.iterrows()]
            self.session.executemany(query, all_query_params, **kwargs)
            chunk_pos += chunk_size

    def delete(self, where=None, database=None, table=None):
        database = database or self.database
        table = table or self.table
        if where:
            query = "DELETE FROM {database}.{table} WHERE {where};".format(database=database, table=table, where=where)
        else:
            query = "DELETE FROM {database}.{table};".format(database=database, table=table)
        self.session.execute(query)

    def execute(self, *args, **kwargs):
        return self.session.execute(*args, **kwargs)

    def _new_session(self):
        uda = teradata.UdaExec(**self.config)
        return uda.connect(system=self.host, username=self.user_name, password=self.password,
                           **self.connect_kwargs)

    def _query(self, query_string, **kwargs):
        try:
            return pd.read_sql(query_string, self.session, **kwargs)
        except Exception as err:
            if self.pooling and \
                    "(32, '[08S01] [Teradata][Unix system error]  32 Socket error - Connection reset by peer')" in str(err):
                self._pool[(self.host, self.user_name)] = self._new_session()
                return pd.read_sql(query_string, self.session, **kwargs)
            raise err
