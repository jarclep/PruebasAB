#Esto sólo correrá si es llamada la función desde shell
import apache_beam as beam
from apache_beam.io.jdbc import ReadFromJdbc, WriteToJdbc
#from apache_beam import typehints, coders, Row
from apache_beam.options.pipeline_options import PipelineOptions
import typing

query_top10 = "SELECT TOP(10) NumeroTienda FROM tiendas"

# Opciones del pipeline
options = PipelineOptions()

p2 = beam.Pipeline()
test_sqlserver = (
    p2
    | 'Leer desde JDBC' >> ReadFromJdbc(
                table_name='tiendas',
                driver_class_name='com.microsoft.sqlserver.jdbc.SQLServerDriver',
                #driver_jars = 'C:\\Program Files\\sqljdbc_12.8\\enu\\jars\\mssql-jdbc-12.8.1.jre8',
                jdbc_url='jdbc:sqlserver://localhost:1433;databaseName=ControlTiendas;encrypt=false',
                username='sqlserver',
                password='P4$$w0rd',
                # Puedes agregar otras opciones si es necesario, como el esquema
                query=query_top10
            )
    | 'Map para ver' >> beam.Map(lambda e: e)
    | 'Ver en txt' >> beam.io.WriteToText('SQLtotext.txt')
)
p2.run().wait_until_finish()