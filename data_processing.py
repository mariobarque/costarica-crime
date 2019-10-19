from pyspark.sql import SparkSession
from merge_district_data import load_csv, process_escuelas, process_colegios, \
    process_extranjeros_escuelas, process_extranjeros_colegios, process_crimenes, merge_distritos_data

database_name = 'distritos'
database_user_name = 'postgres'
database_password = '123'
database_table = 'distritos'


def add_calculated_columns(df):
    df = df.withColumn('TCP', (df['POBLACION_2016'] - df['POBLACION_2011']) / df['POBLACION_2016'])

    df = df.withColumn('EPEXM', df['EEXM'] / df['ETM'])
    df = df.withColumn('CPEXM', df['CEXM'] / df['CTM'])

    df = df.withColumn('ET', df['EPU'] + df['EPR'] + df['ESV'])
    df = df.withColumn('EPPR', df['EPR'] / df['ET'])
    df = df.withColumn('EPRU', df['ERU'] / df['ET'])
    df = df.withColumn('EPHM', df['EHM'] / df['EMM'])

    df = df.withColumn('CT', df['CPU'] + df['CPR'] + df['CSV'])
    df = df.withColumn('CPPR', df['CPR'] / df['CT'])
    df = df.withColumn('CPRU', df['CRU'] / df['CT'])
    df = df.withColumn('CPHM', df['CHM'] / df['CMM'])

    df = df.withColumn('TASA_ASALTO', df['ASALTO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_HOMICIDIO', df['HOMICIDIO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_HURTO', df['HURTO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_ROBO', df['ROBO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_ROBO_VEHICULO', df['ROBO_VEHICULO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_TACHA_VEHICULO', df['TACHA_VEHICULO'] / df['POBLACION_2016'])

    return df


def get_processed_data(spark, distritos_path, crimenes_path, escuelas_path, colegios_path,
                    extranjerons_escuelas_path, extranjeros_colegios_path, save_path):

    distritos_df = load_csv(spark, distritos_path)
    crimenes_df = load_csv(spark, crimenes_path)
    escuelas_df = load_csv(spark, escuelas_path)
    colegios_df = load_csv(spark, colegios_path)
    escuelas_extranjeros_df = load_csv(spark, extranjerons_escuelas_path)
    colegios_extranjeros_df = load_csv(spark, extranjeros_colegios_path)

    escuelas_grouped = process_escuelas(escuelas_df)
    colegios_grouped = process_colegios(colegios_df)
    escuelas_extranjeros_grouped = process_extranjeros_escuelas(escuelas_extranjeros_df)
    colegios_extranjeros_grouped = process_extranjeros_colegios(colegios_extranjeros_df)
    crimenes_grouped = process_crimenes(crimenes_df)

    distritos_df = merge_distritos_data(distritos_df, escuelas_grouped, colegios_grouped, escuelas_extranjeros_grouped,
                                        colegios_extranjeros_grouped, crimenes_grouped)
    distritos_df = add_calculated_columns(distritos_df)

    distritos_df.toPandas().to_csv(save_path, encoding='utf-8-sig', index=False)

    return distritos_df


def save_dataset_to_db(df, database_name, database_user_name, database_password, database_table):
    df \
        .write \
        .format("jdbc") \
        .mode('overwrite') \
        .option("url", "jdbc:postgresql://localhost/" + database_name) \
        .option("user", database_user_name) \
        .option("password", database_password) \
        .option("dbtable", database_table) \
        .save()


def get_data_set_from_db(database_name, database_user_name, database_password, database_table):
    df = spark \
        .read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost/" + database_name) \
        .option("user", database_user_name) \
        .option("password", database_password) \
        .option("dbtable", database_table) \
        .load()

    return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName('database').master('local')\
                                .appName("Basic JDBC pipeline") \
                                .config("spark.driver.extraClassPath", "postgresql-42.1.4.jar") \
                                .config("spark.executor.extraClassPath", "postgresql-42.1.4.jar") \
                                .getOrCreate()

    #distritos_df = get_merged_data(spark, 'data/distritos.csv', 'data/crimenes.csv',
    #                               'data/escuelas.csv', 'data/colegios.csv',
    #                               'data/extranjeros_escuelas.csv', 'data/extranjeros_colegios.csv',
    #                               'data/processed/dataset.csv')


    df = get_data_set_from_db(database_name, database_user_name, database_password, database_table)
    df.show()

    #save_to_database(distritos_df, database_name, database_user_name, database_password, database_table)

