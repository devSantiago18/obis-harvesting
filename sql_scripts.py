import cx_Oracle
from datetime import datetime
import func

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
'brackish']

IP_DIR = '192.168.3.70'
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
        list_db_vars.append(row[0])
        
    #print(list_db_vars)
    
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
    dsn_connection = cx_Oracle.makedsn(IP_DIR, port=PORT, sid=SID)
    connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")
    cursor = connection.cursor()

    var_dict = {}
    vars_list_query =  cursor.execute('SELECT NOMBRE, ID_VARIABLE FROM CMDWC_VARIABLES')
    for row in vars_list_query:
        var_dict[row[0]] = row[1]
    print(var_dict)
    connection.close()
    return var_dict




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
    # sql_insert_fila = 'INSERT INTO cmdwc_filas (id_fila, data_name) VALUES (:1, :2)'
    # try:
    #     cursor.execute(sql_insert_fila, (timestamp, doi))
    #     connection.commit()
    # except :
    #     print("Error insertando fila")
    #print(dataset)
    var_dict = create_dic_var()
    for occ in dataset:
        for key in occ:
            print('{} : {} : {}'.format(var_dict[key], key, occ[key]))
        
    
    connection.close()
    


if __name__ == '__main__':
    #create_dic_var()
    n()