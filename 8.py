import tarantool
import pandas as pd
from clickhouse_driver import Client
import pandahouse as ph

connection = tarantool.connect("localhost", 3301, user='admin', password='pass')

tester = connection.space('queue')


data = tester.select()
df = pd.DataFrame(data, columns=['Day', 'TickTime', 'Speed'])

connection = dict(database='db', user='default', password='ubuntu')
ph.to_clickhouse(df, 'table', connection=connection, index=False) #Переносим данные в ClickHouse
