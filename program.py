from pyspark.sql import SparkSession

def load_csv_data_set(spark, csv_path):
    df = spark.read.format("csv").option("header", True).option("path", csv_path).load()
    return df

def load_data(spark):

    colegios_df = load_csv_data_set(spark, 'data/colegios.csv')
    escuelas_df = load_csv_data_set(spark, 'data/escuelas.csv')
    extranjeros_colegios_df = load_csv_data_set(spark, 'data/extranjeros_colegios.csv')
    extranjeros_escuelas_df = load_csv_data_set(spark, 'data/extranjeros_escuelas.csv')
    distritos_df = load_csv_data_set(spark, 'data/distritos.csv')
    crimenes_df = load_csv_data_set(spark, 'data/crimenes.csv')

    return colegios_df, escuelas_df, extranjeros_colegios_df, extranjeros_escuelas_df, distritos_df, crimenes_df

spark = SparkSession.builder.appName('database').master('local').getOrCreate()
colegios_df, escuelas_df, extranjeros_colegios_df, extranjeros_escuelas_df, distritos_df, crimenes_df = load_data(spark)

colegios_grouped_df = colegios_df.groupby('ZIPCODE').count()

colegios_grouped_df.show()