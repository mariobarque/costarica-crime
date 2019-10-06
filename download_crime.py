import pandas as pd
import requests

url = 'https://pjenlinea3.poder-judicial.go.cr/estadisticasoij/Home/obtenerDatosDescargas'
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
json_template = '{"TN_FechaInicio":20180101,"TN_FechaFinal":20181231,' \
                '"TC_Provincias":"%s","TC_Cantones":"%s","TC_Distritos":"%s",' \
                '"TC_Delito":"1,2,3,4,5,6","TC_Victima":"1,2,3,4,5","TC_Modalidades":"0"}'
crime_cols = ['category', 'sub_category', 'date', 'hour', 'victim_category', 'victim_sub_category', 'age_category',
              'sex', 'nationality', 'province', 'canton', 'district', 'zipcode']

def download_csv(zipcode):
    zipcode = str(zipcode)
    json_object = json_template % (zipcode[0:1], zipcode[0:3], zipcode)
    json_data = {
        'pJson': json_object,
        'pExtension': 'csv'
    }
    response = requests.post(url, headers=headers, json=json_data)
    if response.status_code == 200:
        return response.content
    else:
        print('There was an error: ', response.status_code)
        return None

def parse_csv(csv, row):
    csv = csv.decode("utf-8")
    lines = csv.splitlines()
    crimes = []
    index = 0
    for line in lines:
        index += 1
        if index == 1:
            continue

        cols = line.split(',')
        category = cols[0].strip()
        sub_category = cols[1].strip()
        date = cols[2].strip()
        hour = cols[3].strip()
        victim_category = cols[4]
        victim_sub_category = cols[5]
        age_category = cols[6]
        sex = cols[7]
        nationality = cols[8]
        province = str(row['Province']).upper()
        canton = str(row['Canton']).upper()
        district = str(row['District']).upper()
        zipcode = row['Code']

        crime = [category, sub_category, date, hour, victim_category, victim_sub_category, age_category, sex, nationality, province, canton, district, zipcode]
        crimes.append(crime)

    return crimes


df = pd.read_csv('data/distritos.csv')
all_crimes = pd.DataFrame(columns=crime_cols)
for index, row in df.iterrows():
    print(row['Province'] + ' ' + row['Canton'])
    code = row['Code']
    csv = download_csv(code)
    crimes = parse_csv(csv, row)
    df = pd.DataFrame(crimes, columns=crime_cols)
    all_crimes = all_crimes.append(df, ignore_index=True)

all_crimes.to_csv('data/all_crimes.csv', encoding='utf-8-sig')

