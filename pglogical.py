#!/usr/bin/python3

import psycopg2
from config import config


# function for try except check output
def status_param(query, psql_query, status, var_confs):
    try:
        print(f'{query}: {psql_query[0]} - {status} [{var_confs[query]}]')
    except:
        print(f'{query}: {psql_query} - {status} [{var_confs[query]}]')


# function for query sql
def query_sql(cur, var_confs, show=''):
    # check params
    for query in var_confs:
        cur.execute(f'{show} {query};')
        psql_query = cur.fetchone()
        # if var_confs[query] == 'pglogical':
        #     query_print = 'extension'
        # elif var_confs[query] == 'logical_replication':
        #     query_print = 'replication_role'
        # else:
        #     query_print = query
        if psql_query == var_confs[query]:
            status_param(query, psql_query, 'OK', var_confs)
        else:
            status_param(query, psql_query, 'FAILED', var_confs)


def connect(server):
    """ Connect """
    conn = None
    pglogical_user = {'SELECT usename FROM pg_user where usename=\'logical_replication\'': 'logical_replication'}
    pglogical_extension = {'select true as result from pg_extension where extname = \'pglogical\'': 'pglogical'}
    check_confs = {'server_version': '12',
                   'wal_level': 'logical',
                   'max_worker_processes': 10,
                   'max_replication_slots': 10,
                   'max_wal_senders': 10,
                   'track_commit_timestamp': 'on'}
    check_tables_pks = 'select tab.table_schema, ' \
                       'tab.table_name ' \
                       'from information_schema.tables tab ' \
                       'left join information_schema.table_constraints tco ' \
                       'on tab.table_schema = tco.table_schema ' \
                       'and tab.table_name = tco.table_name ' \
                       'and tco.constraint_type = \'PRIMARY KEY\' ' \
                       'where tab.table_type = \'BASE TABLE\' ' \
                       'and tab.table_schema not in (\'pg_catalog\', \'information_schema\') ' \
                       'and tco.constraint_name is null ' \
                       'order by table_schema, ' \
                       'table_name;'

    try:
        params = config(server)

        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params, connect_timeout=1)

        cur = conn.cursor()

        # get params from config
        print('---------------------')
        print(f'PostgreSQL get params for {server}:')

        # check confs param
        query_sql(cur, check_confs, 'SHOW')

        # check user pglogical
        query_sql(cur, pglogical_user)

        # check extension pglogical
        query_sql(cur, pglogical_extension)


        # check tables only master
        print(server)
        if server == 'master':
            cur.execute(check_tables_pks)
            psql_query = cur.fetchall()
            for table in psql_query:
                if table[0] == 'public':
                    print(f'{table[1]} - OK')
        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print('Check your configs - FAILED')
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


if __name__ == '__main__':
    connect('master')
    connect('slave')
