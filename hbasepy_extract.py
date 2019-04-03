__author__ = "José Luis Barrera Trancoso"
__license__ = "GPL"
__version__ = "1.0.0"
__email__ = "jlbartra@alu.upo.es"


import happybase
import datetime
import csv

# Script configuration
hbase_master = 'nodea'
bootstrapping_row = 2
bootstrapping_column = 3
table_name = 'measurements{}'.format(bootstrapping_row).encode()

# Connection to HBase
connection = happybase.Connection(hbase_master)
connection.open()

# Header of output
td = datetime.timedelta(hours=0, minutes=0)

header = ['Sensor', 'Date']
for i in range(144):
    hour = td.seconds//3600
    minutes = (td.seconds//60) % 60
    header.append('{}:{}'.format(hour, minutes))
    td = td + datetime.timedelta(minutes=10)

row_prefix = str(bootstrapping_column).encode()
table = connection.table(table_name)

with open('consumo.csv', 'w') as outfile:
    wr = csv.writer(outfile, quoting=csv.QUOTE_NONE)
    wr.writerow(header)

for key, data in table.scan(row_prefix=row_prefix):
    row_name = key.decode()[1:].split('-')[0]
    row_date = data[b'cfd:col0'].decode().split(' ')[0]
    sensor_data = [row_name, row_date]
    h = 0
    period = 0
    while h < 24:

        cf = 'cf{}'.format(h)
        col = 'col{}'.format(period)

        sensor_data.append(
            data['{}:{}'.format(cf, col).encode()].decode()
        )

        if period == 50:
            h += 1
            period = 0
        else:
            period += 10

    with open('consumo.csv', 'a') as outfile:
        wr = csv.writer(outfile, quoting=csv.QUOTE_NONE)
        wr.writerow(sensor_data)

