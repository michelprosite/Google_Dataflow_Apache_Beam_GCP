import apache_beam as beam

class toolsMerge(beam.DoFn):
    def process(self,row):
        col1 = row['data1']
        col2 = row['data2']
        concatenated = col1 + col2
        row['concatenated'] = concatenated
        del row['data1']
        del row['data2']
        return row

    @staticmethod
    def compare_data(element):
        key, values = element

        data1 = values['data1']
        data2 = values['data2']

        # Comparar data1 com data2
        if data1 == data2:
            comparison_result = "suprime"
        elif not data1:
            comparison_result = "delete"
        elif not data2:
            comparison_result = "insert"
        else:
            comparison_result = "update"

        updated_values = dict(values)
        updated_values[comparison_result] = True

        return (key, updated_values)


# Dados de exemplo
data = [
    (11, {'data1': [('Sam', 1000, 'ind', 'IT', '02/11/19')], 'data2': [('Sam', 1000, 'ind', 'IT', '02/11/19')]}),
    (22, {'data1': [('Tom', 2000, 'usa', 'HR', '02/11/19')], 'data2': [('Tom', 2000, 'usa', 'HR', '12/11/19')]}),
    (33, {'data1': [('Can', 3500, 'uk', 'IT', '02/11/19')], 'data2': [('Can', 3500, 'uk', 'IT', '02/11/19')]}),
    (77, {'data1': [], 'data2': [('samy', 1000, 'ind', 'PM', '02/11/19')]}),
    (66, {'data1': [('samy', 1000, 'ind', 'PM', '02/11/19')], 'data2': []}),
]

with beam.Pipeline() as pipeline:
    results = (
        pipeline
        | beam.Create(data)
        | beam.ParDo(toolsMerge.compare_data)
        | "Remapear para Valor" >> beam.Filter(lambda x: isinstance(x, tuple))
        | "Concatenate columns" >> beam.Map(toolsMerge().process)
        | beam.Map(print)
    )





