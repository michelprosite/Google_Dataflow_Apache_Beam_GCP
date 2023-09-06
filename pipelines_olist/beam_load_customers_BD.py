import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = {
    'project': 'olist-brasil-project',
    'runner': 'DataflowRunner',
    'region': 'southamerica-east1',
    'staging_location': 'gs://olist_brasil_project/temp',
    'temp_location': 'gs://olist_brasil_project/temp',
    'template_location': 'gs://olist_brasil_project/template/batch_job_BQ_olist_customers_full',
    'save_main_session': True
}

pipeline_options = PipelineOptions.from_dictionary(pipeline_options)
#pipeline = beam.Pipeline(options=pipeline_options)
pipeline = beam.Pipeline()

serviceAccount = r'/home/michel/Documentos/Chave/olist-brasil-project-1646e2167a8f.json'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount


class RemoveColumns(beam.DoFn):
    def process(self, element):
        indices_para_remover = [3, 4]
        for indice in sorted(indices_para_remover, reverse=True):
            del element[indice]
        return [element]
        
# Desempacotando a tupla e criando um dicionário simples linha a linha    
class MontaDict(beam.DoFn):
    def process(self, element):
        dict_ = {} 
        dict_['custumers_id'] = str(element[0]) # Forçando a tipagem STR
        dict_['customers_unique_id'] = str(element[1]) # Forçando a tipagem STR
        dict_['zip_code'] = 'NULL' if element[2] is None else int(element[2]) #Tratando os dados nulos e forçando a tipagem INT
        return [dict_]
    

class VerificarZipCode(beam.DoFn):
    def process(self, element):
        zip_code = element.get('zip_code')
        try:
            element['zip_code'] = int(zip_code)
            yield element
        except ValueError:
            pass  


class RemoveDuplicates(beam.DoFn):
    def process(self, element):
        first_value = list(element.values())[0]
        return [element] if first_value not in self.seen_values else []

    def setup(self):
        self.seen_values = set()


table_schema = 'customers_id:STRING, customers_unique_id:STRING, zip_code:INTEGER'
tabela = 'olist-brasil-project.olist_db_full.customers'

customers_write_BG = (
    pipeline
    | "Importar dados customers" >> beam.io.ReadFromText(r'gs://olist_brasil_project/import_csv/olist_customers_dataset.csv', skip_header_lines=1)
#>>> IMPORTANDO DA ORIGEM E SALVANDO OS DADOS NA TRANSIENT
    | "Salvar no GCP TRANSIENT" >> beam.io.WriteToText(r"gs://olist_brasil_project/transient/batch_transient_olist_customers_full.csv")
#>>> REALOCANDO OS DADOS DA TRANSIENT PARA A RAW EM UM NOVO FORMATO
    | "Insert/Update no GCP RAW" >> beam.io.WriteToText(r"gs://olist_brasil_project/raw/batch_raw_olist_customers_full.csv", append_trailing_newlines=True)
#>>> TRATANDO OS DADOS APLICANDO AS TRANSFORMAÇÕES E REGRAS DE NEGÓCIO, EM SEGUIDA, SALVANDO NA TRUSTED
    | "Separar por vírgulas" >> beam.Map(lambda element: element.split(','))
    | "Remover aspas duplas" >> beam.Map(lambda element: [column.replace('"', '') for column in element])
    | "Remover colunas" >> beam.ParDo(RemoveColumns())
    | "Insert/Update no GCP TRUSTED" >> beam.io.WriteToText(r"gs://olist_brasil_project/trusted/batch_trusted_olist_customers_full.csv", append_trailing_newlines=True)
#>>> INICIANDO A PREPARAÇÃO DOS DADOS PARA CARGA NA REFINED
    | "Montar dicionário" >> beam.ParDo(MontaDict())
#    | "Salvar no GCP" >> beam.io.WriteToText(r"gs://olist_brasil_project/raw/batch_job_BQ_olist_customers_full.csv")    
#    | "Verificar Zip Code" >> beam.ParDo(VerificarZipCode())
    | "Remove duplicadas coluna 0" >> beam.ParDo(RemoveDuplicates())
    | "Mostrar Resultados" >> beam.Map(print)
#>>> CARREGANDO A REFINED NO BIGQUERY
#    | "Salva no BigQuery" >> beam.io.WriteToBigQuery(
#        table=tabela,
#        schema=table_schema,
#        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
#        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
#        custom_gcs_temp_location='gs://olist_brasil_project/temp'
#    )
)

pipeline.run()
