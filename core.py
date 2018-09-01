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

    def select (self, query, output='table', file='data.csv'):
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
            data['insert_time'] = d.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            data['event'] = event
            data = data.set_index('insert_time')
            data.columns = ['json']
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
    
    def replace (self, data, table, event):
        self.delete(table=table, event=event)
        self.insert(data=data, table=table)

def select (query, output='table', file='data.csv'):
    return cluster.select(query=query, output=output, file=file)

def insert (data, table, event):
    cluster.insert(data=data, table=table, event=event)

def delete (table, event):
    if nodes:
        for node in nodes:
            node.delete(table=table+'_node', event=event)
    else:
        cluster.delete(table=table, event=event)

def update (data, table, event):
    if nodes:
        for node in nodes:
            node.delete(table=table+'_node', event=event)
    else:
        cluster.delete(table=table, event=event)
    cluster.insert(data=data, table=table)


cluster = connection(config.cluster_connector)
nodes = [connection(node) for node in config.nodes_connectors]