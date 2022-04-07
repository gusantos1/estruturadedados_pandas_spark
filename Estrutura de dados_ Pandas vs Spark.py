# Databricks notebook source
# MAGIC %md
# MAGIC # Tipos de dados no Spark: Simples e Complexo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estrutura Simples
# MAGIC 
# MAGIC | Tipo do Dado  | Equivalência em Python  | Instância no Spark                           |
# MAGIC | ------------- | ----------------------- | -------------------------------------------- |
# MAGIC | ByteType    | int               | ByteType()                                 |
# MAGIC | ShortType | int       | ShortType()                              |
# MAGIC | IntegerType      | int          | IntegerType()                                   |
# MAGIC | LongType     | int   | LongType()              |
# MAGIC | FloatType       | float                    | FloatType() |
# MAGIC | StringType    | str         | StringType()                         |
# MAGIC | BooleanType   | Bool | BooleanType() |
# MAGIC | DecimalType   | decimal.Decimal | DecimalType() |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estrutura Complexa
# MAGIC 
# MAGIC | Tipo do Dado  | Equivalência em Python  | Instância no Spark                           |
# MAGIC | ------------- | ----------------------- | -------------------------------------------- |
# MAGIC | BinaryType    | bytearray               | BinaryType()                                 |
# MAGIC | TimestampType | datetime.datetime       | TimestampType()                              |
# MAGIC | DateType      | datetime.date           | DateType()                                   |
# MAGIC | ArrayType     | List, tuple, or array   | ArrayType(dataType, [nullable])              |
# MAGIC | MapType       | dict                    | MapType(keyType, valueType, [nul<br/>lable]) |
# MAGIC | StructType    | List or tuple           | StructType([fields])                         |
# MAGIC | StructField   | Um objeto do tipo campo | StructField(name, dataType, [nul<br/>lable]) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importando tipos

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Exemplo de estrutura complexa

# COMMAND ----------

data = [
    (
        'Guilherme', [
            {
                'Telefones':
                {
                    'Fixo': '2137706213',
                    'Celular': '2196857513'
                }
            }
        ]
    ),
    (
        'Jéssica', [
            {
                'Telefones':
                {
                    'Fixo': '1122565427',
                    'Celular': '11984529368'
                }
            }
        ]
    )
]

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Schema Spark
# MAGIC 
# MAGIC > (Nome, [{Telefones: {Fixo: numero, Celular: numero}}])  
# MAGIC > tuple(string, list[dict[string: dict[string, string]]])

# COMMAND ----------

SCHEMA_TELEFONES = StructType(
    [
        StructField('Nome', StringType()),
        StructField('Contato',ArrayType(MapType(StringType(), StringType())))
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe Spark

# COMMAND ----------

df = spark.createDataFrame(data, SCHEMA_TELEFONES)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## O Pandas aceita a mesma estrutura?

# COMMAND ----------

df.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Convertendo para estrutura simples do Pandas

# COMMAND ----------

from pyspark.sql.functions import to_json

# COMMAND ----------

pd = df.select(df.Nome,to_json(df.Contato).alias('Contato'))

# COMMAND ----------

pandinha = pd.toPandas()
display(pandinha)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Convertendo para estrutura original dos dados

# COMMAND ----------

pandinha_dict = pandinha.to_dict('records')
pandinha_dict

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Toda a estrutura em 'Contatos' está como string é preciso converter para uma estrutura complexa e aninhada.

# COMMAND ----------

def converte_estrutura(texto: str):
    index_celular = texto.find('Celular') + 8
    index_fixo = texto.find('Fixo') + 5
    celular = texto[index_celular: index_celular + 10]
    fixo = texto[index_fixo: index_fixo + 10]
    
    return {'Celular': celular, 'Fixo': fixo}

# COMMAND ----------

original = [(pessoa['Nome'], [{'Telefones': converte_estrutura(pessoa['Contato'])}]) for pessoa in pandinha_dict]

for pessoas in original:
    print(pessoas)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Recriando o dataframe com o Spark

# COMMAND ----------

df2 = spark.createDataFrame(original, SCHEMA_TELEFONES)

# COMMAND ----------

display(df2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC 
# MAGIC O pandas não suporta estruturas complexas de dados, como solução ele converte toda a estrutura como uma string, deixando com que o desenvolvedor converta esses dados. Em contrapartida, o Spark mantém a estrutura dos dados oferecendo uma lista de tipagem mais ampla e estruturada.
