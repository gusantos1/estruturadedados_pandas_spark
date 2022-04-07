# Tipos de dados no Spark: Simples e Complexo

### Estrutura Simples

| Tipo do Dado  | Equivalência em Python  | Instância no Spark                           |
| ------------- | ----------------------- | -------------------------------------------- |
| ByteType    | int               | ByteType()                                 |
| ShortType | int       | ShortType()                              |
| IntegerType      | int          | IntegerType()                                   |
| LongType     | int   | LongType()              |
| FloatType       | float                    | FloatType() |
| StringType    | str         | StringType()                         |
| BooleanType   | Bool | BooleanType() |
| DecimalType   | decimal.Decimal | DecimalType() |

### Estrutura Complexa

| Tipo do Dado  | Equivalência em Python  | Instância no Spark                           |
| ------------- | ----------------------- | -------------------------------------------- |
| BinaryType    | bytearray               | BinaryType()                                 |
| TimestampType | datetime.datetime       | TimestampType()                              |
| DateType      | datetime.date           | DateType()                                   |
| ArrayType     | List, tuple, or array   | ArrayType(dataType, [nullable])              |
| MapType       | dict                    | MapType(keyType, valueType, [nul<br/>lable]) |
| StructType    | List or tuple           | StructType([fields])                         |
| StructField   | Um objeto do tipo campo | StructField(name, dataType, [nul<br/>lable]) |

## Importando tipos

```Python
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType
```

## Exemplo de estrutura complexa

```Python
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
```
## Schema Spark

> (Nome, [{Telefones: {Fixo: numero, Celular: numero}}])  
> tuple(string, list[dict[string: dict[string, string]]])

```Python
SCHEMA_TELEFONES = StructType(
    [
        StructField('Nome', StringType()),
        StructField('Contato',ArrayType(MapType(StringType(), StringType())))
    ]
)
```

## Dataframe Spark

```Python
df = spark.createDataFrame(data, SCHEMA_TELEFONES)
```

## O Pandas aceita a mesma estrutura?
```Python
df.toPandas()
```

ArrowNotImplementedError: Not implemented type for Arrow list to pandas: map<string, string>

## Convertendo para estrutura simples do Pandas

```Python
from pyspark.sql.functions import to_json
pd = df.select(df.Nome,to_json(df.Contato).alias('Contato'))

pandinha = pd.toPandas()
```

## Convertendo para estrutura original dos dados
```Python
pandinha_dict = pandinha.to_dict('records')
pandinha_dict
```

##### Toda a estrutura em 'Contatos' está como string é preciso converter para uma estrutura complexa e aninhada.
```Python
def converte_estrutura(texto: str):
    index_celular = texto.find('Celular') + 8
    index_fixo = texto.find('Fixo') + 5
    celular = texto[index_celular: index_celular + 10]
    fixo = texto[index_fixo: index_fixo + 10]
    
    return {'Celular': celular, 'Fixo': fixo}

original = [(pessoa['Nome'], [{'Telefones': converte_estrutura(pessoa['Contato'])}]) for pessoa in pandinha_dict]

for pessoas in original:
    print(pessoas)
```
```bash
('Guilherme', [{'Telefones': {'Celular': '2196857513', 'Fixo': '2137706213'}}])
('Jéssica', [{'Telefones': {'Celular': '1198452936', 'Fixo': '1122565427'}}])
```

## Recriando o dataframe com o Spark
```Python
df2 = spark.createDataFrame(original, SCHEMA_TELEFONES)
```

## Conclusão

O pandas não suporta estruturas complexas de dados, como solução ele converte toda a estrutura como uma string, deixando com que o desenvolvedor converta esses dados. Em contrapartida, o Spark mantém a estrutura dos dados oferecendo uma lista de tipagem mais ampla e estruturada.
