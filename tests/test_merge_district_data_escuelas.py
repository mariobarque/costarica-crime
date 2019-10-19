from merge_district_data import process_escuelas


def test_process_escuelas_check_no_errors(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    escuelas_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    escuelas_df = spark_session.createDataFrame(escuelas_data, cols)

    target_df = process_escuelas(escuelas_df)
    assert target_df is not None


def test_process_escuelas_check_tasa_aprobacion(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    escuelas_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    escuelas_df = spark_session.createDataFrame(escuelas_data, cols)

    target_df = process_escuelas(escuelas_df)

    target = target_df.filter('ZIPCODE == 1').select('ETAAM').first().ETAAM

    assert target == 0.8


def test_process_escuelas_check_total_matricula(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    escuelas_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    escuelas_df = spark_session.createDataFrame(escuelas_data, cols)

    target_df = process_escuelas(escuelas_df)

    target = target_df.filter('ZIPCODE == 1').select('ETM').first().ETM

    assert target == 30


def test_process_escuelas_check_matricula_hombre(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    escuelas_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    escuelas_df = spark_session.createDataFrame(escuelas_data, cols)

    target_df = process_escuelas(escuelas_df)

    target = target_df.filter('ZIPCODE == 1').select('EHM').first().EHM

    assert target == 15


def test_process_escuelas_check_matricula_mujer(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    escuelas_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    escuelas_df = spark_session.createDataFrame(escuelas_data, cols)

    target_df = process_escuelas(escuelas_df)

    target = target_df.filter('ZIPCODE == 1').select('EMM').first().EMM

    assert target == 15


def test_process_escuelas_check_total_aprobados(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    escuelas_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    escuelas_df = spark_session.createDataFrame(escuelas_data, cols)

    target_df = process_escuelas(escuelas_df)

    target = target_df.filter('ZIPCODE == 1').select('ETA').first().ETA

    assert target == 24

