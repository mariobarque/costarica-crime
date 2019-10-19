from merge_district_data import process_crimenes


def test_process_crimenes_check_no_errors(spark_session):
    #cols = ['CATEGORY', 'ZIPCODE', 'HOMICIDIO', 'ASALTO', 'HURTO', 'ROBO', 'ROBO DE VEHICULO', 'TACHA DE VEHICULO']
    cols = ['CATEGORY', 'ZIPCODE']
    crimenes_data = [('HOMICIDIO', 1),
                     ('ASALTO', 1),
                     ('ASALTO', 1),
                     ('ROBO', 1),
                     ('ROBO', 1),
                     ('ROBO', 1),
                     ('ROBO DE VEHICULO', 2),
                     ('TACHA DE VEHICULO', 2),
                     ('ASALTO', 2),
                     ('ASALTO', 2),
                     ('HURTO', 2),
                     ('HURTO', 2)]
    crimenes_df = spark_session.createDataFrame(crimenes_data, cols)

    target_df = process_crimenes(crimenes_df)
    assert target_df is not None


def test_process_crimenes_check_total_asaltos(spark_session):
    cols = ['CATEGORY', 'ZIPCODE']
    crimenes_data = [('HOMICIDIO', 1),
                     ('ASALTO', 1),
                     ('ASALTO', 1),
                     ('ROBO', 1),
                     ('ROBO', 1),
                     ('ROBO', 1),
                     ('ROBO DE VEHICULO', 2),
                     ('TACHA DE VEHICULO', 2),
                     ('ASALTO', 2),
                     ('ASALTO', 2),
                     ('HURTO', 2),
                     ('HURTO', 2)]
    crimenes_df = spark_session.createDataFrame(crimenes_data, cols)

    target_df = process_crimenes(crimenes_df)

    target = target_df.filter('ZIPCODE == 1').select('ASALTO').first().ASALTO

    assert target == 2


def test_process_crimenes_check_total_robos(spark_session):
    cols = ['CATEGORY', 'ZIPCODE']
    crimenes_data = [('HOMICIDIO', 1),
                     ('ASALTO', 1),
                     ('ASALTO', 1),
                     ('ROBO', 1),
                     ('ROBO', 1),
                     ('ROBO', 1),
                     ('ROBO DE VEHICULO', 2),
                     ('TACHA DE VEHICULO', 2),
                     ('ASALTO', 2),
                     ('ASALTO', 2),
                     ('HURTO', 2),
                     ('HURTO', 2)]
    crimenes_df = spark_session.createDataFrame(crimenes_data, cols)

    target_df = process_crimenes(crimenes_df)

    target = target_df.filter('ZIPCODE == 1').select('ROBO').first().ROBO

    assert target == 3


def test_process_crimenes_check_total_homicidio(spark_session):
    cols = ['CATEGORY', 'ZIPCODE']
    crimenes_data = [('HOMICIDIO', 1),
                     ('ASALTO', 1),
                     ('ASALTO', 1),
                     ('ROBO', 1),
                     ('ROBO', 1),
                     ('ROBO', 1),
                     ('ROBO DE VEHICULO', 2),
                     ('TACHA DE VEHICULO', 2),
                     ('ASALTO', 2),
                     ('ASALTO', 2),
                     ('HURTO', 2),
                     ('HURTO', 2)]
    crimenes_df = spark_session.createDataFrame(crimenes_data, cols)

    target_df = process_crimenes(crimenes_df)

    target = target_df.filter('ZIPCODE == 1').select('HOMICIDIO').first().HOMICIDIO

    assert target == 1