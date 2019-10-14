import pandas as pd

"""
Nomenclatura Fuente:
    MFT: Matrícula Total.
    MFH: Matrícula Total Hombres
    MFM: Matrícula Total Mujeres
    RET: Total Reprobados
    REH: Total Hombres Reprobados
    REM: Total Mujeres Reprobadas
    APT: Total Aprobados
    APH: Total Hombres Aprobados
    APM: Total Mujeres Aprobadas
    
    TOTT: Total Extranjeros
    TOTH: Total Hombres Extranjeros
    TOTM: Total Mujeres Extranjeros
    
Nomenclatura datos procesados:
    ETM = Escuelas Total Matricualados
    EHM = Escuelas Hombres Matricualados
    EMM = Escuelas Mujeres Matricualados
    ETR = Escuelas Total Reprobados
    EHR = Escuelas Hombres Reprobados
    EMR = Escuelas Mujeres Reprobadas
    ETA = Escuelas Total Aprobados
    EHA = Escuelas Hombres Aprobados
    EMA = Escuelas Mujeres Aprobadas
    EPU = Escuela Publica
    EPR = Escuela Privada
    ESV = Escuela Subvencionada
    EUR = Escuela Urbana
    ERU = Escuela Rural 
    ETAA = Escuela Tasa de Aprobacion
    ETAAH = Escuela Tasa de Aprobacion Hombres
    ETAAM = Escuela Tasa de Aprobacion Mujeres
    EEXM = Escuela Extranjeros Matriculads
    EPEXM = Escuela Proporcion Extranjeros Matriculads
    ET = Escuelas en Total
    EPPR = Escuelas Proporcion de Privadas
    EPRU = Escuelas Proporcion de Rurales
    EPHM = Escuelas Proporcion Hombres Mujeres
    
    CTM = Colegios Total Matricualados
    CHM = Colegios Hombres Matricualados
    CMM = Colegios Mujeres Matricualados
    CTR = Colegios Total Reprobados
    CHR = Colegios Hombres Reprobados
    CMR = Colegios Mujeres Reprobadas
    CTA = Colegios Total Aprobados
    CHA = Colegios Hombres Aprobados
    CMA = Colegios Mujeres Aprobadas
    CPU = Colegio Publica
    CPR = Colegio Privada
    CSV = Colegio Subvencionada
    CUR = Colegio Urbana
    CRU = Colegio Rural 
    CTAA = Colegio Tasa de Aprobacion
    CTAAH = Colegio Tasa de Aprobacion Hombres
    CTAAM = Colegio Tasa de Aprobacion Mujeres 
    CEXM = Colegio Extranjeros Matriculads
    CPEXM = Colegio Proporcion Extranjeros Matriculads
    CT = Colegios en Total
    CPPR = Colegios Proporcion de Privados
    CPRU = Colegios Proporcion de Rurales
    CPHM = Colegios Proporcion Hombres Mujeres
    
    TCP = Tasa Crecimiento de Población (2011 - 2016)  
"""




def group_escuelas(escuelas_df):
    # expand the categorical information
    escuelas_df = pd.concat([escuelas_df, pd.get_dummies(escuelas_df['SECTOR'], prefix='SECTOR')], axis=1)
    escuelas_df = pd.concat([escuelas_df, pd.get_dummies(escuelas_df['ZONA'], prefix='ZONA')], axis=1)
    group = escuelas_df.groupby(['ZIPCODE'])
    df = group.agg(ETM=('MFT', 'sum'),
                   EHM=('MFH', 'sum'),
                   EMM=('MFM', 'sum'),
                   ETR=('RET', 'sum'),
                   EHR=('REH', 'sum'),
                   EMR=('REM', 'sum'),
                   ETA=('APT', 'sum'),
                   EHA=('APH', 'sum'),
                   EMA=('APM', 'sum'),
                   EPU=('SECTOR_1', 'sum'),
                   EPR=('SECTOR_2', 'sum'),
                   ESV=('SECTOR_3', 'sum'),
                   EUR=('ZONA_1', 'sum'),
                   ERU=('ZONA_2', 'sum'))

    df['ETAA'] = df['ETA'] / df['ETM']
    df['ETAAH'] = df['EHA'] / df['EHM']
    df['ETAAM'] = df['EMA'] / df['EMM']
    return df


def group_colegios(colegios_df):
    # expand the categorical information
    colegios_df = pd.concat([colegios_df, pd.get_dummies(colegios_df['SECTOR'], prefix='SECTOR')], axis=1)
    colegios_df = pd.concat([colegios_df, pd.get_dummies(colegios_df['ZONA'], prefix='ZONA')], axis=1)
    group = colegios_df.groupby(['ZIPCODE'])
    df = group.agg(CTM=('MFT', 'sum'),
                   CHM=('MFH', 'sum'),
                   CMM=('MFM', 'sum'),
                   CTR=('RET', 'sum'),
                   CHR=('REH', 'sum'),
                   CMR=('REM', 'sum'),
                   CTA=('APT', 'sum'),
                   CHA=('APH', 'sum'),
                   CMA=('APM', 'sum'),
                   CPU=('SECTOR_1', 'sum'),
                   CPR=('SECTOR_2', 'sum'),
                   CSV=('SECTOR_3', 'sum'),
                   CUR=('ZONA_1', 'sum'),
                   CRU=('ZONA_2', 'sum'))

    df['CTAA'] = df['CTA'] / df['CTM']
    df['CTAAH'] = df['CHA'] / df['CHM']
    df['CTAAM'] = df['CMA'] / df['CMM']
    return df


def group_extranjeros_escuelas(escuelas_df):
    group = escuelas_df.groupby(['ZIPCODE'])
    df = group.agg(EEXM=('TOTT', 'sum'))
    return df


def group_extranjeros_colegios(colegios_df):
    group = colegios_df.groupby(['ZIPCODE'])
    df = group.agg(CEXM=('TOTT', 'sum'))
    return df


def group_crimenes(crimenes_df):
    crimenes_df = pd.concat([crimenes_df, pd.get_dummies(crimenes_df['CATEGORY'])], axis=1)
    group = crimenes_df.groupby(['ZIPCODE'])

    df = group.agg(ASALTO=('ASALTO', 'sum'),
                   HOMICIDIO=('HOMICIDIO', 'sum'),
                   HURTO=('HURTO', 'sum'),
                   ROBO=('ROBO', 'sum'),
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
grouped_colegios = group_colegios(colegios_df)
grouped_estranjeros_escuelas = group_extranjeros_escuelas(extranjeros_escuelas_df)
grouped_estranjeros_colegios = group_extranjeros_colegios(extranjeros_escuelas_df)
grouped_crimenes_df = group_crimenes(crimenes_df)

df = pd.merge(distritos_df, grouped_escuelas, on='ZIPCODE')
df = pd.merge(df, grouped_colegios, on='ZIPCODE')
df = pd.merge(df, grouped_estranjeros_escuelas, on='ZIPCODE')
df = pd.merge(df, grouped_estranjeros_colegios, on='ZIPCODE')
df = pd.merge(df, grouped_crimenes_df, on='ZIPCODE')

df['TCP'] = (df['POBLACION_2016'] - df['POBLACION_2011'])/df['POBLACION_2016']

df['EPEXM'] = df['EEXM'] / df['ETM']
df['CPEXM'] = df['CEXM'] / df['CTM']

df['ET'] = df['EPU'] + df['EPR'] + df['ESV']
df['EPPR'] = df['EPR'] / df['ET']
df['EPRU'] = df['ERU'] / df['ET']
df['EPHM'] = df['EHM'] / df['EMM']

df['CT'] = df['CPU'] + df['CPR'] + df['CSV']
df['CPPR'] = df['CPR'] / df['CT']
df['CPRU'] = df['CRU'] / df['CT']
df['CPHM'] = df['CHM'] / df['CMM']

df['TASA_ASALTO'] = df['ASALTO'] / df['POBLACION_2016']
df['TASA_HOMICIDIO'] = df['HOMICIDIO'] / df['POBLACION_2016']
df['TASA_HURTO'] = df['HURTO'] / df['POBLACION_2016']
df['TASA_ROBO'] = df['ROBO'] / df['POBLACION_2016']
df['TASA_ROBO_VEHICULO'] = df['ROBO_VEHICULO'] / df['POBLACION_2016']
df['TASA_TACHA_VEHICULO'] = df['TACHA_VEHICULO'] / df['POBLACION_2016']


df.to_csv('data/processed/processed.csv', encoding='utf-8-sig', index=False)

