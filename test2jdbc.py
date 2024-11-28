import apache_beam as beam
from apache_beam.io.jdbc import ReadFromJdbc
from apache_beam.options.pipeline_options import PipelineOptions

# Configuración de opciones
pipeline_options = PipelineOptions()

# Pipeline
with beam.Pipeline(options=pipeline_options) as pipeline:
    result = (
        pipeline
        | 'Leer desde JDBC' >> ReadFromJdbc(
            table_name='tiendas',  # Usar table_name para leer toda la tabla
            driver_class_name='com.microsoft.sqlserver.jdbc.SQLServerDriver',
            jdbc_url = 'jdbc:sqlserver://localhost:1433;databaseName=ControlTiendas;encrypt=false;trustServerCertificate=true',  # Cambiar por tu host y base
            username='sqlserver',
            password='P4$$w0rd'
        )
        | 'Imprimir resultados' >> beam.Map(print)
    )
