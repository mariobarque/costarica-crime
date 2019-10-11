import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as funcs


def load_csv(spark, path):
    df = spark \
        .read \
        .format('csv') \
        .option('path', path) \
        .option("header", "true") \
        .load()

    return df


def process_extranjeros_escuelas(escuelas_extranjeros_df):
    group = escuelas_extranjeros_df.groupby(['ZIPCODE'])
    df = group.agg({'TOTT': 'sum'}).withColumnRenamed("SUM(TOTT)", "EEXM")
    return df


def process_extranjeros_colegios(colegios_extranjeros_df):
    group = colegios_extranjeros_df.groupby(['ZIPCODE'])
    df = group.agg({'TOTT': 'sum'}).withColumnRenamed("SUM(TOTT)", "CEXM")
    return df


def process_escuelas(escuelas_csv):
    sector_cat = escuelas_csv.select('SECTOR').distinct().rdd.flatMap(lambda x: x).collect()
    zona_cat = escuelas_csv.select('ZONA').distinct().rdd.flatMap(lambda x: x).collect()
    sector_exprs = [funcs.when(funcs.col('SECTOR') == cat, 1).otherwise(0).alias('SECTOR_' + str(cat)) for cat in sector_cat]
    zona_exprs = [funcs.when(funcs.col('ZONA') == cat, 1).otherwise(0).alias('ZONA_' + str(cat)) for cat in zona_cat]

    colegios_csv = escuelas_csv.select(escuelas_csv.columns + sector_exprs + zona_exprs)

    group = colegios_csv.groupby('ZIPCODE')
    df = group.agg({'MFT': 'sum', 'MFH': 'sum', 'MFM': 'sum', 'RET': 'sum', 'REH': 'sum', 'REM': 'sum', 'APT': 'sum',
                    'APH': 'sum', 'APM': 'sum', 'EPU': 'sum', 'EPR': 'sum', 'ESV': 'sum', 'EUR': 'sum', 'ERU': 'sum'})\
        .withColumnRenamed("SUM(MFT)", "ETM")\
        .withColumnRenamed("SUM(MFH)", "EHM") \
        .withColumnRenamed("SUM(MFM)", "EMM") \
        .withColumnRenamed("SUM(RET)", "ETR")\
        .withColumnRenamed("SUM(REH)", "EHR")\
        .withColumnRenamed("SUM(REM)", "EMR")\
        .withColumnRenamed("SUM(APT)", "ETA")\
        .withColumnRenamed("SUM(APH)", "EHA")\
        .withColumnRenamed("SUM(APM)", "EMA")\
        .withColumnRenamed("SUM(SECTOR_1)", "EPU")\
        .withColumnRenamed("SUM(SECTOR_2)", "EPR")\
        .withColumnRenamed("SUM(SECTOR_3)", "ESV")\
        .withColumnRenamed("SUM(ZONA_1)", "EUR")\
        .withColumnRenamed("SUM(ZONA_2)", "ERU")

    df = df.withColumn('ETAA', df['ETA'] / df['ETM'])
    df = df.withColumn('ETAAH', df['EHA'] / df['EHM'])
    df = df.withColumn('ETAAM', df['EMA'] / df['EMM'])

    return df


def process_colegios(colegios_csv):
    sector_cat = colegios_csv.select('SECTOR').distinct().rdd.flatMap(lambda x: x).collect()
    zona_cat = colegios_csv.select('ZONA').distinct().rdd.flatMap(lambda x: x).collect()
    sector_exprs = [funcs.when(funcs.col('SECTOR') == cat, 1).otherwise(0).alias('SECTOR_' + str(cat)) for cat in sector_cat]
    zona_exprs = [funcs.when(funcs.col('ZONA') == cat, 1).otherwise(0).alias('ZONA_' + str(cat)) for cat in zona_cat]

    colegios_csv = colegios_csv.select(colegios_csv.columns + sector_exprs + zona_exprs)

    group = colegios_csv.groupby('ZIPCODE')
    df = group.agg({'MFT': 'sum', 'MFH': 'sum', 'MFM': 'sum', 'RET': 'sum', 'REH': 'sum', 'REM': 'sum', 'APT': 'sum',
                    'APH': 'sum', 'APM': 'sum', 'CPU': 'sum', 'CPR': 'sum', 'CSV': 'sum', 'CUR': 'sum', 'CRU': 'sum'})\
        .withColumnRenamed("SUM(MFT)", "CTM")\
        .withColumnRenamed("SUM(MFH)", "CHM") \
        .withColumnRenamed("SUM(MFM)", "CMM") \
        .withColumnRenamed("SUM(RET)", "CTR")\
        .withColumnRenamed("SUM(REH)", "CHR")\
        .withColumnRenamed("SUM(REM)", "CMR")\
        .withColumnRenamed("SUM(APT)", "CTA")\
        .withColumnRenamed("SUM(APH)", "CHA")\
        .withColumnRenamed("SUM(APM)", "CMA")\
        .withColumnRenamed("SUM(SECTOR_1)", "CPU")\
        .withColumnRenamed("SUM(SECTOR_2)", "CPR")\
        .withColumnRenamed("SUM(SECTOR_3)", "CSV")\
        .withColumnRenamed("SUM(ZONA_1)", "CUR")\
        .withColumnRenamed("SUM(ZONA_2)", "CRU")

    df = df.withColumn('CTAA', df['CTA'] / df['CTM'])
    df = df.withColumn('CTAAH', df['CHA'] / df['CHM'])
    df = df.withColumn('CTAAM', df['CMA'] / df['CMM'])

    return df


def process_crimenes(crimenes_df):
    category = crimenes_df.select('CATEGORY').distinct().rdd.flatMap(lambda x: x).collect()
    cat_exprs = [funcs.when(funcs.col('CATEGORY') == cat, 1).otherwise(0).alias(str(cat)) for cat in category]

    crimenes_df = crimenes_df.select(crimenes_df.columns + cat_exprs)
    group = crimenes_df.groupby('ZIPCODE')
    df = group.agg({'HOMICIDIO': 'sum', 'HURTO': 'sum', 'ROBO': 'sum', 'ROBO DE VEHICULO': 'sum',
                    'TACHA DE VEHICULO': 'sum'})\
        .withColumnRenamed("SUM(HOMICIDIO)", "HOMICIDIO")\
        .withColumnRenamed("SUM(HURTO)", "HURTO")\
        .withColumnRenamed("SUM(ROBO)", "ROBO")\
        .withColumnRenamed("SUM(ROBO DE VEHICULO)", "HROBO_VEHICULO")\
        .withColumnRenamed("SUM(TACHA DE VEHICULO)", "TACHA_VEHICULO")

    return df


def merge_distritos_data(distritos_df, escuelas_grouped, colegios_grouped, escuelas_extranjeros_grouped,
                         colegios_extranjeros_grouped, crimenes_grouped):

    distritos_df = distritos_df.join(escuelas_grouped, on='ZIPCODE')
    distritos_df = distritos_df.join(colegios_grouped, on='ZIPCODE')
    distritos_df = distritos_df.join(escuelas_extranjeros_grouped, on='ZIPCODE')
    distritos_df = distritos_df.join(colegios_extranjeros_grouped, on='ZIPCODE')
    distritos_df = distritos_df.join(crimenes_grouped, on='ZIPCODE')

    return distritos_df


def add_calculated_columns(df):
    df = df.withColumn('TCP', (df['POBLACION_2016'] - df['POBLACION_2011']) / df['POBLACION_2016'])

    df = df.withColumn('EPEXM', df['EEXM'] / df['ETM'])
    df = df.withColumn('CPEXM', df['CEXM'] / df['CTM'])

    df = df.withColumn('ET', df['EPU'] + df['EPR'] + df['ESV'])
    df = df.withColumn('EPPR', df['EPR'] / df['ET'])
    df = df.withColumn('EPRU', df['ERU'] / df['ET'])

    df = df.withColumn('CT', df['CPU'] + df['CPR'] + df['CSV'])
    df = df.withColumn('CPPR', df['CPR'] / df['CT'])
    df = df.withColumn('CPRU', df['CRU'] / df['CT'])

    df = df.withColumn('TASA_ASALTO', df['ASALTO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_HOMICIDIO', df['HOMICIDIO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_HURTO', df['HURTO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_ROBO', df['ROBO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_ROBO_VEHICULO', df['ROBO_VEHICULO'] / df['POBLACION_2016'])
    df = df.withColumn('TASA_TACHA_VEHICULO', df['TACHA_VEHICULO'] / df['POBLACION_2016'])

    return df


if __name__ == "__main__":
    spark = SparkSession.builder.appName('database').master('local').getOrCreate()

    escuelas_df = load_csv(spark, 'data/escuelas.csv')
    colegios_df = load_csv(spark, 'data/colegios.csv')
    escuelas_extranjeros_df = load_csv(spark, 'data/extranjeros_escuelas.csv')
    colegios_extranjeros_df = load_csv(spark, 'data/extranjeros_colegios.csv')
    crimenes_df = load_csv(spark, 'data/crimenes.csv')
    distritos_df = load_csv(spark, 'data/distritos.csv')

    escuelas_grouped = process_escuelas(escuelas_df)
    colegios_grouped = process_colegios(colegios_df)
    escuelas_extranjeros_grouped = process_extranjeros_escuelas(escuelas_extranjeros_df)
    colegios_extranjeros_grouped = process_extranjeros_colegios(colegios_extranjeros_df)
    crimenes_grouped = process_crimenes(crimenes_df)

    distritos_df = merge_distritos_data(distritos_df, escuelas_grouped, colegios_grouped, escuelas_extranjeros_grouped,
                                        colegios_extranjeros_grouped, crimenes_grouped)
    distritos_df = add_calculated_columns(distritos_df)

    distritos_df.show()
