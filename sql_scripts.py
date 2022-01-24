import cx_Oracle
from datetime import datetime

vars_obis = [
'bathymetry',
'tribeid',
'classid',
'infrakingdomid',
'originalNameUsage',
'scientificName',
'parentNameUsage',
'identificationRemarks',
'superfamily',
'subfamily',
'infraspecificEpithet',
'taxonRank',
'reproductiveCondition',
'wrims',
'associatedTaxa',
'organismQuantity',
'dataset_id',
'georeferencedDate',
'previousIdentifications',
'georeferencedBy',
'subkingdomid',
'infraorder',
'collectionID',
'marine',
'basisOfRecord',
'startDayOfYear',
'speciesid',
'taxonomicStatus',
'higherGeography',
'maximumDepthInMeters',
'subterclass',
'eventDate',
'minimumDepthInMeters',
'identifiedByID',
'subkingdom',
'decimalLatitude',
'orderid',
'maximumElevationInMeters',
'superfamilyid',
'verbatimLatitude',
'sampleSizeValue',
'recordNumber',
'subphylumid',
'forma',
'superclass',
'lifeStage',
'shoredistance',
'originalScientificName',
'datasetID',
'varietyid',
'superorder',
'language',
'flags',
'occurrenceRemarks',
'nameAccordingToID',
'verbatimCoordinateSystem',
'namePublishedIn',
'recordedBy',
'verbatimDepth',
'section',
'municipality',
'subterclassid',
'geodeticDatum',
'sex',
'associatedSequences',
'infraclassid',
'family',
'node_id',
'tribe',
'fieldNotes',
'disposition',
'otherCatalogNumbers',
'class',
'locationID',
'countryCode',
'collectionCode',
'parvphylumid',
'higherGeographyID',
'phylumid',
'institutionID',
'terrestrial',
'county',
'catalogNumber',
'subtribeid',
'identificationVerificationStatus',
'verbatimTaxonRank',
'endDayOfYear',
'footprintSRS',
'verbatimLongitude',
'suborder',
'subspeciesid',
'sectionid',
'category',
'subphylum',
'locationAccordingTo',
'dynamicProperties',
'behavior',
'license',
'maximumDistanceAboveSurfaceInMeters',
'typeStatus',
'coordinatePrecision',
'associatedOccurrences',
'id',
'sst',
'sampleSizeUnit',
'associatedMedia',
'nameAccordingTo',
'preparations',
'accessRights',
'genusid',
'eventTime',
'fieldNumber',
'rightsHolder',
'individualCount',
'subgenusid',
'institutionCode',
'habitat',
'kingdom',
'subgenus',
'verbatimSRS',
'samplingProtocol',
'eventID',
'depth',
'georeferenceRemarks',
'phylum',
'scientificNameAuthorship',
'megaclassid',
'subsection',
'infraphylumid',
'locationRemarks',
'taxonRemarks',
'recordedByID',
'date_end',
'subclass',
'georeferenceVerificationStatus',
'day',
'megaclass',
'acceptedNameUsage',
'aphiaID',
'establishmentMeans',
'sss',
'species',
'infraphylum',
'acceptedNameUsageID',
'modified',
'nomenclaturalStatus',
'datasetName',
'eventRemarks',
'month',
'specificEpithet',
'associatedOrganisms',
'stateProvince',
'vernacularName',
'taxonID',
'georeferenceSources',
'suborderid',
'parvphylum',
'samplingEffort',
'namePublishedInID',
'locality',
'date_mid',
'identificationReferences',
'organismID',
'infrakingdom',
'absence',
'genus',
'dataGeneralizations',
'footprintWKT',
'infraclass',
'parvorder',
'dateIdentified',
'type',
'year',
'namePublishedInYear',
'formaid',
'superclassid',
'informationWithheld',
'higherClassification',
'identifiedBy',
'subclassid',
'decimalLongitude',
'gigaclass',
'associatedReferences',
'organismQuantityType',
'nomenclaturalCode',
'familyid',
'kingdomid',
'subfamilyid',
'waterBody',
'georeferenceProtocol',
'minimumElevationInMeters',
'materialSampleID',
'parentEventID',
'organismRemarks',
'variety',
'continent',
'subtribe',
'scientificNameID',
'occurrenceID',
'taxonConceptID',
'organismScope',
'date_start',
'parvorderid',
'verbatimEventDate',
'verbatimCoordinates',
'order',
'verbatimElevation',
'date_year',
'subspecies',
'ownerInstitutionCode',
'hab',
'islandGroup',
'coordinateUncertaintyInMeters',
'gigaclassid',
'superorderid',
'island',
'country',
'identificationQualifier',
'minimumDistanceAboveSurfaceInMeters',
'verbatimLocality',
'subsectionid',
'dropped',
'bibliographicCitation',
'occurrenceStatus',
'references',
'infraorderid',
'brackish'
]

IP_DIR = '192.168.3.70' # prod
#IP_DIR = '10.0.1.101' # dev
PORT = '1521'
SID ='SCI'


def n():
    """
        Funcion usada una sola vez para ingresar las variables de obis en la base dedatos
        tabla: CMDWC_VARIABLES
    """
    dsn_connection = cx_Oracle.makedsn('192.168.3.70', port='1521', sid='SCI')
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()
    #TABLA_VARIABLES = "SD_DWC_VARIABLES_AUX" #dev
    TABLA_VARIABLES = "CMDWC_VARIABLES" # prod

    cursor.execute(f'select nombre from {TABLA_VARIABLES}')

    list_db_vars = []
    for row in cursor:
        print('row query: ', row)
        list_db_vars.append(row[0])
        
    
    print('\n===========================')
    in_db = []
    out_db = []
    for x in vars_obis:
        if x not in list_db_vars:
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
    """
        Funcion que retorna un diccionario con el id y el nombre de las variables en la base de datos
    """
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()

    var_dict = {}
    vars_list_query =  cursor.execute('SELECT NOMBRE, ID_VARIABLE FROM CMDWC_VARIABLES')
    for row in vars_list_query:
        var_dict[row[0]] = row[1]
    
    connection.close()
    return var_dict



def datasets_id_inv():
    """Funcion que extrae los datasets id de la base de datos """
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()
    datasets = [x[0] for x in cursor.execute("SELECT DATASET_ID FROM CMDWC_DATASETS WHERE UPPER(FUENTE_DATASET) = UPPER('OBIS')")]
    return datasets
    
    
def delete_dataset(dataset_id):
    """ Funcion para eliminar un dataset id lo que eliminara en cascada las ocurrencias de este """
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()
    cursor.execute("DELETE FROM cmdwc_datasets WHERE dataset_id = :1", [dataset_id])
    connection.commit()
    
    
def insertar_dataset(dataset_id,title,  doi):
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()
    
    sql_insert_fila = "INSERT INTO cmdwc_datasets (dataset_id, data_url, ext_name, fuente_dataset) VALUES (:1, :2, :3, :4)"
    dataset_flag = False
    try:
        #print("to insert: {}, {}, {}".format(dataset_id, doi, title) )
        cursor.execute(sql_insert_fila, [dataset_id, doi, title, 'OBIS'])
        connection.commit()
        print("inserto 1 fila {}".format(dataset_id))
        return True
    except :
        print("Error insertando fila ", dataset_id)
        return False
    
    
def insert_occurrence(occurrencias, dataset_id,title,  doi, var_dict):
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()
    sql_insert_detalle = """INSERT INTO cmdwc_occurrences (occurrence_id, dataset_id, variable, valor) VALUES (:1,:2,:3,:4)"""
    insert_occ_flag = True # si hay algun error en 1 occurencia del datasert, esta bandera hace que no se haga el commint al db por lo tanto no se insetan datos en la base de datos
    i = 1
    occ_count = 1
    
    for occ in occurrencias:
        occurrence_id = occ['id']
        for key in occ:
            variable = var_dict[key]
            valor = str(occ[key]) if len(str(occ[key])) < 500 else str(occ[key])[:500]
            try:
                
                cursor.execute(sql_insert_detalle, (occurrence_id, dataset_id, variable, valor))
                #print("se inserto esta vaina {}:{}:{}:{}".format(occurrence_id, dataset_id, variable, occ[key]))
            except cx_Oracle.Error as error:
                print("Error insertando 1 {} : {} : {} : {}".format(occurrence_id, dataset_id, variable, valor))
                print(error)
                insert_occ_flag = False
                return 0
            i+=1
            
    connection.commit()
    
        
    
def insert_data(occurrencias, dataset_id,title,  doi, var_dict):
    """ el parametro data debe corresponder a una lista de ocurrencias 
        [
            {occurrenceId : val1, dataset_id : val2, datasetId : val3}
            {occurrenceId : val1, dataset_id : val2, datasetId : val3}
        ]
    """
    string_error = ""
    print(len(occurrencias))
    print(doi)
    print(title)
    print(title)
    print(dataset_id)
    
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()
    
    #creamos el registro de la fila con su id unico y su nombre que es el doi
    sql_insert_fila = "INSERT INTO cmdwc_datasets (dataset_id, data_url, ext_name, fuente_dataset) VALUES (:1, :2, :3, :4)"
    dataset_flag = False
    try:
        #print("to insert: {}, {}, {}".format(dataset_id, doi, title) )
        cursor.execute(sql_insert_fila, [dataset_id, doi, title, 'OBIS'])
        connection.commit()
        print("inserto 1 dataset nuevo:  {}".format(dataset_id))
        dataset_flag = True
    except :
        print("Error insertando fila")
        string_error += '\nError insertando fila {},{},{}'.format(dataset_id, doi, title)
    
    
    if dataset_flag:
        sql_insert_detalle = """INSERT INTO cmdwc_occurrences (occurrence_id, dataset_id, variable, valor) VALUES (:1,:2,:3,:4)"""
        insert_occ_flag = True # si hay algun error en 1 occurencia del datasert, esta bandera hace que no se haga el commint al db por lo tanto no se insetan datos en la base de datos
        i = 1
        occ_count = 1
        print("insertando las occurrencias del dataset ", dataset_id)     
        
        for occ in occurrencias:
            occurrence_id = occ['id']
            for key in occ:
                variable = var_dict[key]
                valor = str(occ[key]) if len(str(occ[key])) < 490 else str(occ[key])[:490]
                try:
                    
                    cursor.execute(sql_insert_detalle, (occurrence_id, dataset_id, variable, valor))
                    #print("se inserto esta vaina {}:{}:{}:{}".format(occurrence_id, dataset_id, variable, occ[key]))
                except cx_Oracle.Error as error:
                    print("Error insertando una variable: {} : {} : {} : {}".format(occurrence_id, dataset_id, variable, valor))
                    print(error)
                    string_error += "\nError insertando {}, {}, {}, {}".format(occurrence_id, dataset_id, int(variable), valor)
                    insert_occ_flag = False
                    return 0
                i+=1
            
        if insert_occ_flag:
            try:
                #print("Insertando occ {}".format(occ_count))
                occ_count += 1
                connection.commit()
                
            except:
                print("error en el commint con las ocurrencias del dataset {}".format(dataset_id))
                return 0
        else:
            print("ERROR: No se pudo insertar")
            print("occcurrencias del dataset: {}".format(dataset_id))
            
    with open('./data/errores_harvesting.txt', 'a', encoding='utf-8') as file:
        file.write(string_error)
        
    connection.close()
        
    


if __name__ == '__main__':
    #create_dic_var()
    #create_dic_var()
    datasets_id_inv()
    #n()