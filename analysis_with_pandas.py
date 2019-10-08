import pandas as pd

##############################################
#MFT: Matrícula Total.
#MFH: Matrícula Total Hombres
#MFM: Matrícula Total Mujeres
#RET: Total Reprobados
#REH: Total Hombres Reprobados
#REM: Total Mujeres Reprobadas
#APT: Total Aprobados
#APH: Total Hombres Aprobados
#APM: Total Mujeres Aprobadas

#TOTT: Total Extranjeros
#TOTH: Total Hombres Extranjeros
#TOTM: Total Mujeres Extranjeros
##############################################


def group_escuelas(escuelas_df):
    # expand the categorical information
    escuelas_df = pd.concat([escuelas_df, pd.get_dummies(escuelas_df['SECTOR'], prefix='SECTOR')], axis=1)
    escuelas_df = pd.concat([escuelas_df, pd.get_dummies(escuelas_df['ZONA'], prefix='ZONA')], axis=1)
    group = escuelas_df.groupby(['ZIPCODE'])
    df = group.agg(ESCUELAS_MATRICULADOS=('MFT', 'sum'),
                   ESCUELAS_HOMBRES_MATRICULADOS=('MFH', 'sum'),
                   ESCUELAS_MUJERES_MATRICULADAS=('MFM', 'sum'),
                   ESCUELAS_REPROBADOS=('RET', 'sum'),
                   ESCUELAS_HOMBRES_REPROBADOS=('REH', 'sum'),
                   ESCUELAS_MUJERES_REPROBADAS=('REM', 'sum'),
                   ESCUELAS_APROBADOS=('APT', 'sum'),
                   ESCUELAS_HOMBRES_APROBADOS=('APH', 'sum'),
                   ESCUELAS_MUJERES_APROBADAS=('APM', 'sum'),
                   ESCUELAS_PUBLICAS=('SECTOR_1', 'sum'),
                   ESCUELAS_PRIVADAS=('SECTOR_2', 'sum'),
                   ESCUELAS_SUBVENCIONADAS=('SECTOR_3', 'sum'),
                   ESCUELAS_RURALES=('ZONA_1', 'sum'),
                   ESCUELAS_URBANAS=('ZONA_2', 'sum'))

    df['ESCUELAS_TAZA_APROBACION'] = df['ESCUELAS_APROBADOS'] / df['ESCUELAS_MATRICULADOS']
    df['ESCUELAS_TAZA_APROBACION_HOMBRES'] = df['ESCUELAS_HOMBRES_APROBADOS'] / df['ESCUELAS_HOMBRES_MATRICULADOS']
    df['ESCUELAS_TAZA_APROBACION_MUJERES'] = df['ESCUELAS_MUJERES_APROBADAS'] / df['ESCUELAS_MUJERES_MATRICULADAS']
    return df

def group_colegios(colegios_df):
    # expand the categorical information
    colegios_df = pd.concat([colegios_df, pd.get_dummies(colegios_df['SECTOR'], prefix='SECTOR')], axis=1)
    colegios_df = pd.concat([colegios_df, pd.get_dummies(colegios_df['ZONA'], prefix='ZONA')], axis=1)
    group = colegios_df.groupby(['ZIPCODE'])
    df = group.agg(COLEGIOS_MATRICULADOS=('MFT', 'sum'),
                   COLEGIOS_HOMBRES_MATRICULADOS=('MFH', 'sum'),
                   COLEGIOS_MUJERES_MATRICULADAS=('MFM', 'sum'),
                   COLEGIOS_REPROBADOS=('RET', 'sum'),
                   COLEGIOS_HOMBRES_REPROBADOS=('REH', 'sum'),
                   COLEGIOS_MUJERES_REPROBADAS=('REM', 'sum'),
                   COLEGIOS_APROBADOS=('APT', 'sum'),
                   COLEGIOS_HOMBRES_APROBADOS=('APH', 'sum'),
                   COLEGIOS_MUJERES_APROBADAS=('APM', 'sum'),
                   COLEGIOS_PUBLICOS=('SECTOR_1', 'sum'),
                   COLEGIOS_PRIVADOS=('SECTOR_2', 'sum'),
                   COLEGIOS_SUBVENCIONADOS=('SECTOR_3', 'sum'),
                   COLEGIOS_RURALES=('ZONA_1', 'sum'),
                   COLEGIOS_URBANOS=('ZONA_2', 'sum'))

    df['COLEGIOS_TAZA_APROBACION'] = df['COLEGIOS_APROBADOS'] / df['COLEGIOS_MATRICULADOS']
    df['COLEGIOS_TAZA_APROBACION_HOMBRES'] = df['COLEGIOS_HOMBRES_APROBADOS'] / df['COLEGIOS_HOMBRES_MATRICULADOS']
    df['COLEGIOS_TAZA_APROBACION_MUJERES'] = df['COLEGIOS_MUJERES_APROBADAS'] / df['COLEGIOS_MUJERES_MATRICULADAS']
    return df

def group_extranjeros_escuelas(escuelas_df):
    group = escuelas_df.groupby(['ZIPCODE'])
    df = group.agg(ESCUELAS_EXTRANJEROS_MATRICULADOS=('TOTT', 'sum'))
    return df

def group_extranjeros_colegios(colegios_df):
    group = colegios_df.groupby(['ZIPCODE'])
    df = group.agg(COLEGIOS_EXTRANJEROS_MATRICULADOS=('TOTT', 'sum'))
    return df

def group_crimenes(crimenes_df):
    crimenes_df = pd.concat([crimenes_df, pd.get_dummies(crimenes_df['CATEGORY'])], axis=1)
    group = crimenes_df.groupby(['ZIPCODE'])

    df = group.agg(ASALTO=('ASALTO', 'count'),
                   HOMICIDIO=('HOMICIDIO', 'count'),
                   HURTO=('HURTO', 'count'),
                   ROBO=('ROBO', 'count'),
                   ROBO_VEHICULO=('ROBO DE VEHICULO', 'sum'),
                   TACHA_VEHICULO=('TACHA DE VEHICULO', 'sum'))

    return df


distritos_df = pd.read_csv('data/distritos.csv')
colegios_df = pd.read_csv('data/colegios.csv')
escuelas_df = pd.read_csv('data/escuelas.csv')
extranjeros_colegios_df = pd.read_csv('data/extranjeros_colegios.csv')
extranjeros_escuelas_df = pd.read_csv('data/extranjeros_escuelas.csv')
crimenes_df = pd.read_csv('data/crimenes.csv')



grouped_escuelas = group_escuelas(escuelas_df)
grouped_colegios = group_colegios(escuelas_df)
grouped_estranjeros_escuelas = group_extranjeros_escuelas(extranjeros_escuelas_df)
grouped_estranjeros_colegios = group_extranjeros_colegios(extranjeros_escuelas_df)
grouped_crimenes_df = group_crimenes(crimenes_df)

df = pd.merge(distritos_df, grouped_escuelas, on='ZIPCODE')
df = pd.merge(df, grouped_colegios, on='ZIPCODE')
df = pd.merge(df, grouped_estranjeros_escuelas, on='ZIPCODE')
df = pd.merge(df, grouped_estranjeros_colegios, on='ZIPCODE')
df = pd.merge(df, grouped_crimenes_df, on='ZIPCODE')

df['TAZA_CRECIMIENTO_POBLACION'] = (df['POBLACION_2016'] - df['POBLACION_2011'])/df['POBLACION_2016']

df['TAZA_EXTRANJEROS_ESCUELAS'] = df['ESCUELAS_EXTRANJEROS_MATRICULADOS'] / df['ESCUELAS_MATRICULADOS']
df['TAZA_EXTRANJEROS_COLEGIOS'] = df['COLEGIOS_EXTRANJEROS_MATRICULADOS'] / df['COLEGIOS_MATRICULADOS']

df['TOTAL_ESCUELAS'] = df['ESCUELAS_PUBLICAS'] + df['ESCUELAS_PRIVADAS'] + df['ESCUELAS_SUBVENCIONADAS']
df['PROPORCION_ESCUELAS_PRIVADAS'] = df['ESCUELAS_PRIVADAS'] / df['TOTAL_ESCUELAS']
df['PROPORCION_ESCUELAS_RURALES'] = df['ESCUELAS_RURALES'] / df['TOTAL_ESCUELAS']

df['TOTAL_COLEGIOS'] = df['COLEGIOS_PUBLICOS'] + df['COLEGIOS_PRIVADOS'] + df['COLEGIOS_SUBVENCIONADOS']
df['PROPORCION_COLEGIOS_PRIVADOS'] = df['COLEGIOS_PRIVADOS'] / df['TOTAL_COLEGIOS']
df['PROPORCION_COLEGIOS_RURALES'] = df['COLEGIOS_RURALES'] / df['TOTAL_COLEGIOS']

df['TAZA_ASALTO'] = df['ASALTO'] / df['POBLACION_2016']
df['TAZA_HOMICIDIO'] = df['HOMICIDIO'] / df['POBLACION_2016']
df['TAZA_HURTO'] = df['HURTO'] / df['POBLACION_2016']
df['TAZA_ROBO'] = df['ROBO'] / df['POBLACION_2016']
df['TAZA_ROBO_VEHICULO'] = df['ROBO_VEHICULO'] / df['POBLACION_2016']
df['TAZA_TACHA_VEHICULO'] = df['TACHA_VEHICULO'] / df['POBLACION_2016']


df.to_csv('data/processed/processed.csv', encoding='utf-8-sig')

