import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'olist-brasil-389423' ,
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://olist-brasil/temp',
    'temp_location': 'gs://olist-brasil/temp',
    'template_location': 'gs://olist-brasil/templates/batch_job_df_olist_customers_full' }
    
pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
pipeline = beam.Pipeline(options=pipeline_options)

serviceAccount = r'/home/michel/Documentos/Engenharia de Dados com Google Dataflow e Apache Beam na GCP/Chave/olist-brasil-389423-8839a1d34886.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]= serviceAccount


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
    | "Importar Dados" >> beam.io.ReadFromText(r'gs://olist-brasil/data-olist-csv/olist_customers_dataset.csv', skip_header_lines=1)
    | "Eliminar Duplicatas" >> beam.ParDo(RemoveDuplicates())
    | "Remapear para Valor" >> beam.Map(lambda x: x[1])
    | "Filtra valor" >> beam.Filter(lambda x: int(x[2]) > 0)
    | "Remover Colunas" >> beam.ParDo(RemoveColumns())
    | "Salvando no GCP" >> beam.io.WriteToText(r'gs://olist-brasil/raw/olist_customers.csv')
#    | "Salvar em novo" >> beam.io.WriteToText("pipelines_olist/olist_customers_dataset.csv")
)

pipeline.run()
