from pyspark.sql.types import FloatType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import functions as funcs
from pyspark.ml.feature import StandardScaler


def perform_cross_validation(df, linear_model):
    model_evaluator = RegressionEvaluator()
    #params = ParamGridBuilder().addGrid(linear_model.solver, ["l-bfgs", "normal"]).build()
    params = ParamGridBuilder().build()

    crossval = CrossValidator(estimator=linear_model,
                              estimatorParamMaps=params,
                              evaluator=model_evaluator,
                              numFolds=4,
                              parallelism=4)

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
        df = df.withColumn(feature, funcs.round(df[feature].cast(FloatType()), 2))

    df = df.withColumnRenamed(label, 'label')

    return df