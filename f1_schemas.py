from f1_entities import F1_Entities
from pyspark.sql import DataFrame
from pyspark.sql.functions import *

class F1_Schemas(F1_Entities):
    def __init__(self):
        super().__init__()
        self.schemas = { # v_k: v_v 
            'circuits':{
                'circuitId': "INT",
                'circuitRef': "STRING",
                'name': "STRING",
                'location': "STRING",
                'country': "STRING",
                'lat': "DOUBLE",
                'lng': "DOUBLE",
                'alt': "DOUBLE",
                'url': "STRING" # reemplazar el http:// por https:// 
            },
            'seasons':{
                'year': "INT",
                'url': "STRING"
            },
            'races':{
                'raceId': 'INT',
                'year': 'INT',
                'round': 'INT',
                'circuitId': 'INT', # Hay circuitos con solo una carrera
                'name': 'STRING', # AUSTRALIAN GRAND PRIX, BARHAIN GRAND PRIX
                'date': 'DATE', # Desde 1950-05-13 hasta Nov-2023
                'time': 'TIMESTAMP', # Idea imputar promedios remplazando del pais o global \N 
                'url': 'STRING', 
                'fp1_date': 'DATE',
                'fp1_time': 'TIMESTAMP', # Idea imputar promedios remplazando del pais o global \N 
                'fp2_date': 'DATE',
                'fp2_time': 'TIMESTAMP',# Idea imputar promedios remplazando del pais o global \N 
                'fp3_date': 'DATE',
                'fp3_time': 'TIMESTAMP',# Idea imputar promedios remplazando del pais o global \N 
                'quali_date': 'DATE',
                'quali_time': 'TIMESTAMP',# Idea imputar promedios remplazando del pais o global \N 
                'sprint_date': 'DATE', 
                'sprint_time': 'TIMESTAMP'# Idea imputar promedios remplazando del pais o global \N 
            },
            'constructors':{
                'constructorId': 'INT', 
                'constructorRef': 'STRING',
                'name': 'STRING',
                'nationality': 'STRING',
                'url': 'STRING'
            },
            'status':{ 
                'statusId': 'INT',
                'status': 'STRING'
            },
            'drivers':{
                'driverId': 'INT',
                'driverRef': 'STRING',
                'number': 'STRING', # replace \N with null
                'code': 'STRING', # replace n with null
                'forename': 'STRING',
                'surname': 'STRING',
                'dob': 'DATE', 
                'nationality': 'STRING',
                'url': 'STRING'
            },
            'constructor_results':{
                'constructorResultsId': 'INT',
                'raceId': 'INT',
                'constructorId': 'INT',
                'points': 'DOUBLE',
                'status': 'STRING' # replace \N with null
            },
            'driver_standings':{
                'driverStandingsId': 'INT',
                'raceId': 'INT',
                'driverId': 'INT',
                'points': 'DOUBLE',
                'position': 'INT',
                'positionText': 'STRING', # Debe de venir con la conversion con base en position
                'wins': 'INT',
            },
            'constructor_standings':{
                'constructorStandingsId': 'INT',
                'raceId': 'INT',
                'constructorId': 'INT',
                'points': 'DOUBLE', 
                'position': 'INT',
                'positionText': 'STRING', # Debe de venir con la conversion con base en position
                'wins': 'INT'
            },
            'pit_stops':{
                'pitStopsId': 'INT', ### SURROGATE ### revisar con funcion de encriptación crc32 - se usó zipwithindex
                'raceId': 'INT', # están desde el raceId 841?? 
                'driverId': 'INT', # hay muchos conductores sin pits
                'stop': 'INT',
                ### SURROGATE ###
                'lap': 'INT', 
                'time': 'STRING', # Cambiar para representar solo la hora sin la fecha
                'duration': 'DOUBLE', # Convertir a double, llenar automaticamente usando la columna de milliseconds
                'milliseconds': 'BIGINT'
            },
            'lap_times':{
                'lapTimesId': 'BIGINT', ### SURROGATE ###
                'raceId': 'INT',
                'driverId': 'INT',
                'lap': 'INT',
                'position': 'INT',
                'time': 'STRING', # Debe de venir con la conversion automatica con base en los milisegundos
                'milliseconds': 'BIGINT'
            },
            'results':{
                'resultId': 'INT',
                'raceId': 'INT', #hay ciertas raceId que no están registradas en results, se habrán cancelado esas carreras?
                'driverId': 'INT',
                'constructorId': 'INT',
                'number': 'INT', # CAR Number
                'grid': 'INT', # POSITION DE INICIO
                'position': 'INT', # POSICION FINAL
                'positionText': 'STRING', # Debe de venir con la conversion automatica con base en la posicion
                'positionOrder': 'INT',
                'points': 'DOUBLE',
                'laps': 'INT',
                'time': 'STRING', # Para estandarizar se pueden crear dos columnas una con el tiempo limpio y otra con el valor de tiempo a sumar partiendo de los milliseconds, será util hacerlo así o será mejor dejarlo así como está?
                'milliseconds': 'BIGINT', # Primero reemplazar \N por null y después convertir a: 'BIGINT',
                'fastestLap': 'INT', # Primero reemplazar \N por null y después convertir a: 'INT',
                'rank': 'INT', # Primero reemplazar \N por null y después convertir a: 'INT',
                'fastestLapTime': 'STRING', # para capa oro agregar fastestLapTimeMilisegundos
                ### para capa oro agregar fastestLapTimeMilisegundos
                'fastestLapSpeed': 'DOUBLE', # Primero reemplazar \N por null y después convertir a DOUBLE
                'statusId': 'INT'
            },
            'sprint_results':{ # solamente hay 180 REGISTROS
                'sprintResultsId': 'INT', # cambiarle el nombre a sprintResultsId
                'raceId': 'INT', #hay ciertas raceId que no están registradas en results, se habrán cancelado esas carreras, o será que se implementaron recientemente?
                'driverId': 'INT',
                'constructorId': 'INT',
                'number': 'INT', # CAR Number
                'grid': 'INT', # POSITION DE INICIO
                'position': 'INT', # POSICION FINAL
                'positionText': 'STRING', # Debe de venir con la conversion automatica con base en la posicion
                'positionOrder': 'INT',
                'points': 'INT',
                'laps': 'INT',
                'time': 'STRING', # Misma estrategia que entity results: Para estandarizar se pueden crear dos columnas una con el tiempo limpio y otra con el valor de tiempo basado en milisegundos
                'milliseconds': 'BIGINT', # Primero reemplazar \N por null y después convertir a: 'BIGINT',
                'fastestLap': 'INT', # Primero reemplazar \N por null y después convertir a: 'INT',
                'fastestLapTime': 'STRING', # para capa oro agregar fastestLapTimeMilisegundos
                ### para capa oro agregar fastestLapTimeMilisegundos
                # se puede crear otro fastes lap speed???
                'statusId': 'INT'
            }, 
            'qualifying':{ # Q1, Q2, Q3: TIEMPO EN RONDAS CLASIFICATORIAS
                'qualifyId': 'INT',
                'raceId': 'INT',
                'driverId': 'INT', 
                'constructorId': 'INT',
                'number': 'INT',
                'position': 'INT',
                'q1': 'STRING',
                'q2': 'STRING',
                'q3': 'STRING'
            }


        }
        # The following list of lines are dict comprehension where **v returns the current iteration value of v
        self.entities = {k: {**v, 'schema': self.schemas.get(k, '')} for k, v in self.entities.items()} # key 'schema': class schema definition 
        self.entities = {k: {**v, 'silver_name': 'silver.' + k} for k, v in self.entities.items()} # key 'silver_name': concat silver and entity name 
        self.entities = {k: {**v, 'gold_name': ('gold.Dim' if v.get('is_dim') else 'gold.Fact') + "".join([word.capitalize() for word in k.split("_")])} for k, v in self.entities.items()} # key 'gold_name': concat Dim or Fact depending on 'is_dim' key and entity name 
        self.entities = {k: {**v, 'dw_name': ('Dim' if v.get('is_dim') else 'Fact') + "".join([word.capitalize() for word in k.split("_")])} for k, v in self.entities.items()} # key 'dw_name': concat Dim or Fact depending on 'is_dim' key and entity name 
        self.entities = {k: {**v, 'hive_metastore_silver_path': (v.get('hive_metastore_silver_path') + '/' + k)} for k, v in self.entities.items()} # return hive_metastore_silve_path defined on f1_entities concatenating /entity
        self.entities = {k: {**v, 'hive_metastore_gold_path': (v.get('hive_metastore_gold_path') + '/' + v.get('gold_name'))} for k, v in self.entities.items()} # return hive_metastore_gold_path defined on f1_entities concatenating /entity
        self.entities = {k: {**v, 'blob_bronce_path': (v.get('blob_bronce_path') + '/' + k + '.csv')} for k, v in self.entities.items()} # return blob_bronce_path defined on f1_entities concatenating /entity
        self.entities = {k: {**v, 'blob_silver_path': (v.get('blob_silver_path') + '/' + k + '.parquet')} for k, v in self.entities.items()} # return blob_silver_path defined on f1_entities concatenating /entity
        self.entities = {k: {**v, 'blob_gold_path': (v.get('blob_gold_path') + '/' + v["dw_name"] + '.parquet')} for k, v in self.entities.items()} # return blob_gold_path defined on f1_entities concatenating /entity

def join_date_and_time_cols(
    dataframe: DataFrame, 
    date_column_name: str, 
    time_column_name: str, 
    date_format: str = "yyyy-MM-dd",
    time_format: str = "HH:mm:ss"
    ) -> DataFrame:
    """
    This functions replaces :ref:`\N` in :ref:`time_column_name` and concatenates :ref:` 00:00:00` otherwise just concatenates :ref:`date_column_name` date and :ref:`time_column_name` and converts to :ref:`pd.Timestamp`
    """
    dataframe = dataframe.withColumn( 
        time_column_name, 
        when(
            col(time_column_name) == "\\N", 
            concat(
                col(date_column_name), 
                lit(" 00:00:00")
            )
        ).otherwise(
            concat(
                col(date_column_name), 
                lit(" "), 
                col(time_column_name)
            )
        )
    )
    dataframe = dataframe.withColumn(time_column_name, unix_timestamp(col(time_column_name), f"{date_format} {time_format}").cast('timestamp'))
    return dataframe

def col_str_to_date(
    dataframe: DataFrame, 
    date_column_name: str, 
    date_format: str = "yyyy-MM-dd",
) -> DataFrame:
    """
    Takes the :ref:`date_column_name` from :ref:`dataframe` and convert it to date type
    """
    dataframe = dataframe.withColumn(date_column_name, when(col(date_column_name) == "\\N", lit(None)).otherwise(col(date_column_name)))
    dataframe = dataframe.withColumn(date_column_name, to_timestamp(col(date_column_name), date_format).cast('date'))
    return dataframe


def df_str_replace(
    dataframe: DataFrame,
    column_list: list[str],
    old_text: str,
    new_text: None,
    output_type: str = 'string'
    ) -> DataFrame:
    """
    Replaces :ref:`old_text` is the current iteration row with :ref:`new_text` in the specified :ref:`List(column_list)` in the given :ref:`dataframe`.
    """
    for column in column_list:
        dataframe = dataframe.withColumn(column, replace(col(column), lit(old_text), lit(new_text)).cast(output_type))
    return dataframe
        
        