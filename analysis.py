from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType

import data_processing
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as funcs
from pyspark.ml.feature import StandardScaler


def perform_cross_validation(df, linear_model):
    model_evaluator = RegressionEvaluator()
    param_grid = ParamGridBuilder().addGrid(linear_model.solver, ["l-bfgs", "normal"]).build()

    crossval = CrossValidator(estimator=linear_model,
                              estimatorParamMaps=param_grid,
                              evaluator=model_evaluator,
                              numFolds=3)

    cv_model = crossval.fit(df)

    return cv_model


def stardize_data(df, features_standard):
    standard_scaler = StandardScaler(inputCol='features', outputCol=features_standard)
    scale_model = standard_scaler.fit(df)
    df = scale_model.transform(df)

    return df


def vectorize_data(df, features):
    assembler = VectorAssembler(
        inputCols=features,
        outputCol='features')
    assembler.setHandleInvalid("skip")

    vector_df = assembler.transform(df)
    vector_df = vector_df.select(['features', 'label'])

    return vector_df


def process_df(df, cols, label):
    df = df.filter(funcs.col(label).isNotNull()).select(cols)
    df = df.fillna(0, subset=cols)

    for feature in cols:
        df = df.withColumn(feature, df[feature].cast(FloatType()))

    df = df.withColumnRenamed(label, 'label')

    return df


def get_data_set(spark):
    # df = data_processing.get_merged_data(spark, 'data/distritos.csv', 'data/crimenes.csv', 'data/escuelas.csv',
    #                                     'data/colegios.csv', 'data/extranjeros_escuelas.csv',
    #                                     'data/extranjeros_colegios.csv', 'data/processed/dataset.csv')

    df = spark.read.format('csv').option('path', 'data/processed/dataset.csv').option("header", "true").load()
    return df



features = ['POBLACION_2016', 'IDS', 'POSICION', 'EXTENSION', 'DENSIDAD', 'TCP',
            'CTAAM', 'CTM', 'CPPR', 'CPRU', 'CPHM', 'CPEXM',
            'TASA_ASALTO', 'TASA_HOMICIDIO', 'TASA_HURTO', 'TASA_ROBO', 'TASA_ROBO_VEHICULO', 'TASA_TACHA_VEHICULO']
label = 'CTAA'
features_standard = 'features_starndard'

spark = SparkSession.builder.appName('local').master('local').getOrCreate()
df = get_data_set(spark)
df = process_df(df, features + [label], label)
df = vectorize_data(df, features)
df = stardize_data(df, features_standard)
df.show()

lr = LinearRegression(featuresCol=features_standard, labelCol='label')
cv_model = perform_cross_validation(df, lr)

print(cv_model.avgMetrics)


