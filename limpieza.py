import pandas as pd
import numpy as np
import pyspark.sql.functions as func
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

"""
    df.drop('MODO')
    df.drop('Pagar Web')
    df.drop('Servicio Financiero')
    df.drop('Pagar Presencial')
    df.drop('Cajero Express y Redes de 3ros')
"""

df = [("Finance",10, None, "", "Fake", 0),
        ("Marketing",20, False, "Yes", "Fake", 0),
        ("Sales",0, True, "Yes", "Fake", 0),
        ("IT",20, True, "No", "Fake", 90),
      ]

deptColumns = ["name","id","hasMODO", "Test", "acc Type", "test 2"]
df = spark.createDataFrame(data=df, schema = deptColumns)
df.printSchema()
df.show(truncate=False)

# Remove colls with NaNs and Nulls
def drop_nulls(df):
    null_counts = df.select([func.count(func.when(func.col(c).isNull(), c)).alias(c) for c in df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in zeros_count.items() if v > (df.count() * 0.80)]
    for i in to_drop:
        df = df.drop(*i)
    return df

df = drop_nulls(df)
df.show()

# Remove colls with
def drop_empy(df):
    empty_counts = df.select([func.count(func.when(df[c] == '', c)).alias(c) for c in df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in zeros_count.items() if v > (df.count() * 0.80)]
    for i in to_drop:
        if i != "test 2": # Add condition
            df = df.drop(*i)
    return df

df = drop_empy(df)
df.show()

# Remove colls with 0s (Todavia no esta claro que columnas utilizan 0) pero se agrega un filter a esas especificas
def drop_zero(df):
    zeros_count = df.select([func.count(func.when(df[c] == 0, c)).alias(c) for c in df.columns]).collect()[0].asDict()
    to_drop = [k for k, v in zeros_count.items() if v > (df.count() * 0.25)]
    for i in to_drop:
        if i != "test 2": # Add condition
            df = df.drop(*i)
    return df

df = drop_zero(df)
df.show()
