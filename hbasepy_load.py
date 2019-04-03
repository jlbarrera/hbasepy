__author__ = "José Luis Barrera Trancoso"
__license__ = "GPL"
__version__ = "1.0.0"
__email__ = "jlbartra@alu.upo.es"

import happybase
import csv

# Script configuration
hbase_master = 'nodea'
file_name = 'SET-dec-2013.csv'
bootstrapping_row = 2
bootstrapping_column = 3

# Connection to HBase
connection = happybase.Connection(hbase_master)
connection.open()

# We create a table for each bootstrapping row defined
for table_number in range(bootstrapping_row):

    table_name = 'measurements{}'.format(table_number+1).encode()

    # Delete the table if exists
    for table in connection.tables():
        if table == table_name:
            connection.disable_table(table_name)
            connection.delete_table(table_name)

    # Create the column families: 1 for date and 24 for the hours of the day
    families = {'cf{}'.format(i): dict() for i in range(24)}
    families['cfd'] = dict()

    # Create the table
    connection.create_table(table_name, families)
    table = connection.table(table_name)

    # Read the file and insert the data in the table
    with open(file_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        hours = 0  # From 1 to 24
        period = 0  # 10 minutes
        columns = {}  #
        for row in csv_reader:
            columns[b'cfd:col0'] = row[1]  # We save the date
            measure = 'cf{}:col{}'.format(hours, period).encode()  # We build the key: cf for hours and col por period
            columns[measure] = row[2]
            period += 10  # +10 minutes
            if period == 60:
                period = 0  # we restart the period for the next hour
                hours += 1

            # We create one row for day
            if hours == 24:
                # we create different rows in base in them bootstrapping_column param
                for i in range(bootstrapping_column):
                    row_name = '{}{}-{}'.format(i+1, row[0], row[1])
                    table.put(row_name, columns)

                hours = 1
