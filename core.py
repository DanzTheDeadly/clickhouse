import pandahouse as ph
import pandas as p
import urllib
import requests
import datetime as d
import config


class connection ():
    def __init__ (self, connector):
        if type(connector) == dict:
            self.connector = connector
            self.connector_str = f'http://{self.connector["host"]}/?user={self.connector["user"]}&password={self.connector["password"]}&database={self.connector["database"]}'
 
        elif type(connector) == str:
            parsed = urllib.parse.urlparse(connector)
            self.connector = dict(
                host=parsed.scheme + '://' + parsed.netloc,
                user=urllib.parse.parse_qs(parsed.query)['user'][0],
                password=urllib.parse.parse_qs(parsed.query)['password'][0],
                database=urllib.parse.parse_qs(parsed.query)['database'][0]
            )
            self.connector_str = connector

    def select (self, query, output='dict', file='data.csv'):
        data = ph.read_clickhouse(query, connection=self.connector)
        if output == 'table':
            pass
        elif output == 'csv':
            data.to_csv(file, index=0)
            data = file
        elif output == 'dict':
            data = data.to_dict(orient='records')
        return data

    def insert (self, data, table, event):
        if type(data) == p.core.frame.DataFrame:
            data.columns = ['json']
            data['insert_time'] = d.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data['event'] = event
            data = data.set_index('insert_time')
            ph.to_clickhouse(data, table=table, connection=self.connector)
        
        elif type(data) == list:
            def convert_dict_to_string (d):
                for key in d:
                    d[key] = str(d[key])
                return str(d).replace('\'','"').replace(': ',':').replace(', ',',')
            
            data = list(map(convert_dict_to_string, data))
            df = p.DataFrame.from_dict({'json': data})
            df['insert_time'] = d.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['event'] = event
            df = df.set_index('insert_time')
            ph.to_clickhouse(df, table=table, connection=self.connector)
        
        else:
            print('Input DataFrame or list of dicts')
    
    def delete (self, table, event):
        query = f"alter table {table} delete where event='{event}'"
        url = self.connector_str + f'&query={query}'
        requests.post(url=url)

    def raw (self, query):
        if type(query) == str:
            url = self.connector_str + f'&query={query}'
            r = requests.post(url=url)

    def clear (self, table):
        query = f'alter table {table} delete where 1'
        url = self.connector_str + f'&query={query}'
        r = requests.post(url=url)

def log (f):
    def wrap (*pargs, **kargs):
        pargs_names = f.__code__.co_varnames[:f.__code__.co_argcount]
        args_dict = {**dict(zip(pargs_names, pargs)), **kargs}
        if 'data' in args_dict.keys():
            args_dict['rows_inserted'] = len(args_dict['data'])
            del args_dict['data']
        cluster.insert([args_dict], table=f'{cluster.connector['database']}_logs', event=f.__name__)
        return f(*pargs, **kargs)
    return wrap


def select (query, output='dict', file='data.csv'):
    return cluster.select(query=query, output=output, file=file)

@log
def insert (data, table, event):
    cluster.insert(data=data, table=table, event=event)

@log
def delete (table, event):
    if nodes:
        for node in nodes:
            node.delete(table=table+'_node', event=event)
    else:
        cluster.delete(table=table, event=event)

@log
def replace (data, table, event):
    if nodes:
        for node in nodes:
            node.delete(table=table+'_node', event=event)
    else:
        cluster.delete(table=table, event=event)
    cluster.insert(data=data, table=table)

@log
def clear (table):


@log
def create (table):
	if '.' in table:
		table=table.split('.')[1]
	if nodes:
		cluster_info = select('select cluster, shard_num, host_name from system.clusters')
		cluster_name = cluster_info[0]['cluster']
		for node in nodes:
			host = cluster_info[nodes.index(node)]['host_name']
			shard = cluster_info[nodes.index(node)]['shard_num']
			query = f"CREATE TABLE IF NOT EXISTS {self.connector['database']}.{table}_node on cluster {cluster_name} (insert_time DateTime, event String, json String) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{self.connector['database']}.{table}', '{host}', insert_time, insert_time, 8192)"
			cluster.raw(query)
		query_distr = f"CREATE TABLE IF NOT EXISTS {self.connector['database']}.{table} on cluster {cluster_name} (insert_time DateTime, event String, json String) ENGINE = Distributed('{cluster_name}', '{self.connector['database']}', '{table}_node', rand())"
		cluster.raw(query_distr)
	else:
		query = f"CREATE TABLE IF NOT EXISTS {self.connector['database']}.{table} (insert_time DateTime, event String, json String) ENGINE = MergeTree()"
		cluster.raw(query)
@log
def clear (table):
	if nodes:
		for node in nodes:
			node.clear(table=table+'_node')
	else:
		cluster.clear(table=table)

cluster = connection(config.cluster_connector)
nodes = [connection(node) for node in config.nodes_connectors]