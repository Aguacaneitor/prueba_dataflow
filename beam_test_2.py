import apache_beam as beam
from apache_beam.transforms.sql import SqlTransform
import sys
import schema
import csv
import argparse 


def define_schema():
    beam.coders.registry.register_coder(schema.schema, beam.coders.RowCoder)

def print_row(element):
  print(element)
  return element

def parse_file(element):
  for line in csv.reader([element], quotechar='"', delimiter=',', quoting=csv.QUOTE_ALL, skipinitialspace=True):
    return line
    


if __name__ == '__main__':    
  
    p = beam.Pipeline(argv=sys.argv)
   
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--fileInput", help="Nombre del archivo de entrada")
    parser.add_argument("-o", "--outputFile", help="Nombre del archivo de salida")
    args = parser.parse_args()
     
    # Aquí procesamos lo que se tiene que hacer con cada argumento
    if args.fileInput:
        input_filename=args.fileInput
    else:
        print('no argumento de entrada ingresado')
        input_filename= 'prueba_schema.txt'
    if args.outputFile:
        prefijoSalida=args.outputFile
    else:
        print('no argumento de salida ingresado')
        prefijoSalida= 'salida'
    parser.parse_args()
    
    #with beam.Pipeline(argv=sys.argv) as p:
    parsed_csv = (
                    p 
                    | 'Readfile' >> beam.io.ReadFromText(input_filename)
                    | 'Parsefile' >> beam.Map(parse_file)
                    | 'DifinirSchema' >> beam.Map(lambda x: beam.Row(fecha=str(x[0].strip()), fruta=str(x[1].strip()), cantidad=int(x[2].strip())))                
                    #| 'SQLTransform' >> SqlTransform("""
                    #SELECT 
                    #  fruta, 
                    #  COUNT(fruta) AS Cuenta
                    #FROM PCOLLECTION
                    #GROUP BY fruta""")
                    | 'Groupby' >> beam.GroupBy('fruta').aggregate_field(lambda x: 1 if x.fruta else 0, sum, 'Cuenta')
                    | 'print' >> beam.FlatMap(print_row)
                    | 'write' >> beam.io.WriteToText(prefijoSalida, file_name_suffix='.txt', header='fecha, fruta, cantidad')
                 )
    p.run().wait_until_finish()