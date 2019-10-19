from merge_district_data import process_colegios


def test_process_colegios_check_no_errors(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    colegios_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    colegios_df = spark_session.createDataFrame(colegios_data, cols)

    target_df = process_colegios(colegios_df)
    assert target_df is not None


def test_process_colegios_check_tasa_aprobacion(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    colegios_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    colegios_df = spark_session.createDataFrame(colegios_data, cols)

    target_df = process_colegios(colegios_df)

    target = target_df.filter('ZIPCODE == 1').select('CTAAM').first().CTAAM

    assert target == 0.8


def test_process_colegios_check_total_matricula(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    colegios_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    colegios_df = spark_session.createDataFrame(colegios_data, cols)

    target_df = process_colegios(colegios_df)

    target = target_df.filter('ZIPCODE == 1').select('CTM').first().CTM

    assert target == 30


def test_process_colegios_check_matricula_hombre(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    colegios_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    colegios_df = spark_session.createDataFrame(colegios_data, cols)

    target_df = process_colegios(colegios_df)

    target = target_df.filter('ZIPCODE == 1').select('CHM').first().CHM

    assert target == 15


def test_process_colegios_check_matricula_mujer(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    colegios_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    colegios_df = spark_session.createDataFrame(colegios_data, cols)

    target_df = process_colegios(colegios_df)

    target = target_df.filter('ZIPCODE == 1').select('CMM').first().CMM

    assert target == 15


def test_process_colegios_check_total_aprobados(spark_session):
    cols = ['SECTOR', 'ZONA', 'ZIPCODE', 'MFT', 'MFH', 'MFM', 'RET', 'REH', 'REM', 'APT', 'APH', 'APM']
    colegios_data = [(1, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 2, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (3, 1, 1, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4),
                     (2, 1, 2, 10, 5, 5, 2, 1, 1, 8, 4, 4)]
    colegios_df = spark_session.createDataFrame(colegios_data, cols)

    target_df = process_colegios(colegios_df)

    target = target_df.filter('ZIPCODE == 1').select('CTA').first().CTA

    assert target == 24

