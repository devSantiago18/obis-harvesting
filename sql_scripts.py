import cx_Oracle
from datetime import datetime
import func


IP_DIR = '10.0.1.101'
PORT = '1521'
SID ='SCI'


def n():
    dsn_connection = cx_Oracle.makedsn('192.168.3.70', port='1521', sid='SCI')
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()
    sql = """SELECT NOMBRE FROM CMDWC_VARIABLES"""



    cursor.execute('select nombre from cmdwc_variables')

    list_db_vars = []
    for row in cursor:
        list_db_vars.append(row[0].upper())
        
    #print(list_db_vars)
    keys_obis = ['country', 'date_year', 'habitat', 'institutionID', 'scientificNameID', 'year', 'scientificName', 'county', 'dropped', 'hab', 'aphiaID', 'language', 'decimalLatitude', 'type', 'locationAccordingTo', 'occurrenceStatus', 'basisOfRecord', 'terrestrial', 'fieldNotes', 'nameAccordingToID', 'maximumDepthInMeters', 'id', 'day', 'verbatimEventDate', 'sampleSizeUnit', 'dataset_id', 'locality', 'stateProvince', 'decimalLongitude', 'verbatimLocality', 'date_end', 'occurrenceID', 'verbatimLatitude', 'date_start', 'month', 'samplingProtocol', 'parentEventID', 'eventDate', 'continent', 'eventID', 'brackish', 'scientificNameAuthorship', 'islandGroup', 'absence', 'samplingEffort', 'taxonRank', 'eventRemarks', 'originalScientificName', 'marine', 'minimumDepthInMeters', 'institutionCode', 'countryCode', 'verbatimLongitude', 'date_mid', 'nameAccordingTo', 'datasetName', 'geodeticDatum', 'taxonomicStatus', 'kingdom', 'waterBody', 'previousIdentifications', 'locationID', 'datasetID', 'kingdomid', 'sampleSizeValue', 'node_id', 'sss', 'depth', 'shoredistance', 'sst', 'bathymetry']
    in_db = []
    out_db = []
    for x in keys_obis:
        if x.upper() not in list_db_vars:
            out_db.append(x)
        else:
            in_db.append(x)

    print("In db\n")
    for x in in_db:
        print(x)
    print('---------------')
    print("Out db\n")
    for x in out_db:
        print(x)
    # print("#######")

    # print(cursor)
    connection.close()

######

def create_dic_var():
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()

    connection.close()




def insert_data(dataset, doi):
    """ el parametro data debe corresponder a una lista de ocurrencias 
        [
            {occurrenceId : val1, dataset_id : val2, datasetId : val3}
            {occurrenceId : val1, dataset_id : val2, datasetId : val3}
        ]
    """
    now = datetime.now()
    timestamp = round(datetime.timestamp(now)* 1000)
    
    #dsn_connection = cx_Oracle.makedsn('192.168.3.70', port='1521', sid='SCI')
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()
    
    # creamos el registro de la fila con su id unico y su nombre que es el doi
    sql_insert_fila = 'INSERT INTO cmdwc_filas (id_fila, data_name) VALUES (:1, :2)'
    try:
        cursor.execute(sql_insert_fila, (timestamp, doi))
        connection.commit()
    except :
        print("Error insertando fila")
        
    for occ in dataset:
        pass
    
    connection.close()