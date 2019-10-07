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


colegios_df = pd.read_csv('data/colegios.csv')
escuelas_df = pd.read_csv('data/escuelas.csv')
#extranjeros_colegios_df = pd.read_csv('data/extranjeros_colegios.csv')
#extranjeros_escuelas_df = pd.read_csv('data/extranjeros_escuelas.csv')
#distritos_df = pd.read_csv('data/distritos.csv')
#crimenes_df = pd.read_csv('data/crimenes.csv')

group = escuelas_df.groupby(['ZIPCODE'])
group.apply(lambda g: g[g['SECTOR'] == 1])
df = group.agg({'MFT': 'sum', 'MFH': 'sum', 'MFM': 'sum', 'RET': 'sum',
                'REH': 'sum', 'REM': 'sum', 'APT': 'sum', 'APH': 'sum', 'APM': 'sum'})


df['APROVE_RATE'] = df['APT'] / df['MFT']
df['APROVE_RATE_H'] = df['APH'] / df['MFH']
df['APROVE_RATE_M'] = df['APM'] / df['MFM']

#a = escuelas_df[escuelas_df['SECTOR'] == 2].groupby(['ZIPCODE']).count()
#print(a)

#df.to_csv('data/c.csv', encoding='utf-8-sig')
