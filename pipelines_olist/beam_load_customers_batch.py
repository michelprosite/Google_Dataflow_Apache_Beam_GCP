import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'olist-brasil-project',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://olist_brasil_project/temp',
    'temp_location': 'gs://olist_brasil_project/temp',
    'template_location': 'gs://olist_brasil_project/template/batch_job_df_olist_customers_full',
    'save_main_session': True
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
pipeline = beam.Pipeline(options=pipeline_options)
#pipeline = beam.Pipeline()

serviceAccount = r'Chave/olist-brasil-project-1646e2167a8f.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount


class RemoveDuplicates(beam.DoFn):
    def process(self, record):
        first_column = record[0]
        return [(first_column, record)]


class RemoveColumns(beam.DoFn):
    def process(self, record):
        indices_para_remover = [3, 4]
        for indice in sorted(indices_para_remover, reverse=True):
            del record[indice]
        return [record]

customers = (
    pipeline
    | "Importar Dados customers" >> beam.io.ReadFromText(r'gs://olist_brasil_project/import_csv/olist_customers_dataset.csv', skip_header_lines=1)
    | "Separar por Vírgulas" >> beam.Map(lambda record: record.split(','))
    | "Remove aspas duplas" >> beam.Map(lambda record: [column.replace('"', '') for column in record])
    | "Remover Colunas" >> beam.ParDo(RemoveColumns())
    | "Remove Duplicadas" >> beam.ParDo(RemoveDuplicates())
    | "Convert to Tuple" >> beam.Map(lambda record: (record[0], record[1], record[2]))  
    | "Distinct" >> beam.Distinct()
    | beam.io.WriteToText(r"gs://olist_brasil_project/raw/batch_job_df_olist_customers_full.csv")
#    | "Mostrar Resultados" >> beam.Map(print)
)

pipeline.run()
