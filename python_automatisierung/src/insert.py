import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import chardet

class Insert():

    server = 'server-sago.database.windows.net'
    database = 'db-sago' 
    username = 'adminsago'
    password = 'wdhDpXWK09PQ9BAKcirv'
    driver = 'ODBC Driver 17 for SQL Server'
    
    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver}'

    def stromsensoren_data(self, src_path):
        
        table_name = 'Stromsensoren'

        with open(src_path, 'rb') as f:
            rawdata = f.read(10000)
        result = chardet.detect(rawdata)
        encoding = result['encoding']
        print(f'-----------> Detected encoding: {encoding}')

        df = pd.read_csv(src_path, encoding=encoding, delimiter= ',')
        print("-----------> Original column names:", df.columns.tolist())

        df.rename(columns={
            'power (Watt)': 'power_watt',
            'energy (Ws)': 'energy_ws'
        }, inplace=True)

        engine = create_engine(self.connection_string)

        try:
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print("-----------> Daten erfolgreich in die Datenbank eingefügt")
        except SQLAlchemyError as e:
            print(f"-----------> Fehler beim Einfügen der Daten: {e}")
    

    def wettersensoren_data(self, src_path): 

        table_name = 'Wettersensoren_Data'
     
        with open(src_path, 'rb') as f:
            rawdata = f.read(10000)
        result = chardet.detect(rawdata)
        encoding = result['encoding']
        print(f'Detected encoding: {encoding}')

        df = pd.read_csv(src_path, encoding=encoding, delimiter=';')
        print("Original column names:", df.columns.tolist())
        print(df.columns.values)

        df.rename(columns={
            'Datum (Europe/Berlin)': 'Datum', 
            'Temperatur Innen (°C)': 'Temp_Innen_C',
            'Temperatur (°C)': 'Temp_C',
            'Wind Chill (°C)': 'Wind_Chill_C',
            'Taupunkt Innen (°C)': 'Taupunkt_Innen_C',
            'Taupunkt (°C)': 'Taupunkt_C',
            'Gef. Temperatur Innen (°C)': 'Gef_Temp_Innen_C',
            'Gefühlte Temperatur (°C)': 'Gef_Temp_C',
            'Luftfeuchtigkeit innen (%)': 'Luftfeucht_Innen_Prozent',
            'Luftfeuchtigkeit (%)': 'Luftfeucht_Prozent',
            'Böen (m/s)': 'Boen_ms',
            'Durch. Windgeschwindigkeit (m/s)': 'Wind_ms',
            'Durch. Windrichtung (°)': 'Wind_Richtung_Grad',
            'Luftdruck (hPa)': 'Luftdruck_hPa',
            'Regen (mm)': 'Regen_mm',
            'Verdunstung (mm)': 'Verdunstung_mm',
            'Regenrate (mm/h)': 'Regenrate_mmh',
            'Solarstrahlung (W/m²)': 'Solarstrahlung_wm2',
            'UV Index': 'UV_Index',
            'Unnamed: 19': 'Zusatzspalte'
        }, inplace=True)

        engine = create_engine(self.connection_string)

        try:
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print("Daten erfolgreich in die Datenbank eingefügt")
        except SQLAlchemyError as e:
            print(f"Fehler beim Einfügen der Daten: {e}")

    def raspberry_data(self, src_path):
        
        table_name = 'SensorData'
     
        with open(src_path, 'rb') as f:
            rawdata = f.read(10000)
        result = chardet.detect(rawdata)
        encoding = result['encoding']
        print(f'Detected encoding: {encoding}')

        df = pd.read_csv(src_path, encoding=encoding, delimiter=';')
        print("Original column names:", df.columns.tolist())
        print(df.columns.values)

        engine = create_engine(self.connection_string)

        try:
            df.to_sql(table_name, engine, if_exists='append', index=False)
            print("Daten erfolgreich in die Datenbank eingefügt")
        except SQLAlchemyError as e:
            print(f"Fehler beim Einfügen der Daten: {e}")







