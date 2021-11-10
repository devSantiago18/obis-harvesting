import requests
import re
import time
import json
from flask import jsonify
import csv
# variables que tienen en comun todas las occurrencias
# l = [
    # 'scientificName',
    # 'dropped',
    # 'aphiaID',
    # 'decimalLatitude',
    # 'basisOfRecord',
    # 'id',
    # 'dataset_id',
    # 'decimalLongitude',
    # 'absence',
    # 'originalScientificName',
    # 'kingdom',
    # 'kingdomid',
    # 'node_id',
    # 'shoredistance',
    # 'bathymetry'
    # ]

datasets_id_inv = [
'da9b067b-7fa2-44a7-a1e9-075dc639f6ce',
'537ef2a9-b403-41fe-93da-eba61bc9d5d8',
'f4870a35-b4b5-4cdd-b665-8b7b2d7fc245',
'61c5e93d-1bcf-4d90-9c38-f7ff654eb364',
'8b5b8fd5-331a-4296-9a53-86929789046b',
'948851ff-3387-45dc-a97b-87f381dbacbe',
'eb4c9fce-7d3f-43e4-a00e-9404866472c9',
'1738da7c-c098-4a6a-9d04-3814fa7613fe',
'5e30b591-d94b-4d98-ae84-4dc6c93541f1',
'6b5c0712-d9c7-4e2f-b3a8-355ad2539081',
'c683d802-a110-4043-95b3-3a20a7c57608',
'08124763-5f33-485d-a3df-b6d502989ba4',
'8a361a70-efcc-4eef-969e-d5049c026861',
'2c8cc767-4ec9-49bd-814f-0e275f09b3c4',
'848027ba-3900-4ebd-b7d8-4c71e0cdf0ba',
'714266aa-123b-43b7-98a8-0529ac7f7c4c',
'483211f0-f210-44a1-85ea-deb16948baae',
'23ba1623-a8c1-4bde-b3b0-912aa5cca885',
'cef48fb7-f74c-45a6-9871-7c5f346b7729',
'6651cf9e-b40f-48dc-a91f-e2e13f7b0891',
'7a673089-50f3-4e17-9ad2-8493d261d84c',
'a0c77508-4641-43f2-b194-cab3386d9e02',
'0b1ec148-693f-4517-b112-386e17956314',
'22e4d9d4-48bd-439d-a1f4-5056e1e55a5b',
'f260b9a8-72a8-44fc-9ddc-383b7d7e54ff',
'0569339f-a376-42f4-8453-49ab9932713d',
'6142952e-7bbc-434f-a055-6bf140e6463d',
'48e988d2-f5f2-4b69-a1a1-6fe06e0fbd29',
'd79dbddb-aee1-4e33-82ef-58004f6603d7',
'0abb8cc1-8651-4213-aff3-2153d14ac322',
'58480117-eefa-49e7-abee-bad994d9d91b',
'b1cb8e5f-17c4-4bee-bead-8cfbad831dae',
'4f3ea0e0-3b75-4062-8199-318df5164d4c',
'dc590428-7a0e-4272-985e-b5ad279a245a',
'6e18e05f-a198-49e2-a59d-edd721f5a677',
'9673fbc4-5be5-468d-ac2f-d575dcfb6d8b',
'1d94f371-a3e5-4e9a-b48c-f26e864c7621',
'754347ff-b193-4665-8fc1-9792e3c1da55',
'573ef83d-6014-405c-b421-7c09505b179a',
'c4e73593-607d-4688-9bd7-9234928082b8',
'20c5a1df-f8a1-4963-8b98-e8000de0f8bd',
'9e3dea3d-6b47-4564-8616-14d54ffc153f',
'be7a16ea-3e22-4365-b066-a0e96c52d2a6',
'f504ff3b-0cdc-4145-ac73-2fc9ae2c4784',
'0d0dd9e9-5d93-442b-a820-51d2027976ed',
'01222525-5c04-4735-8eaa-d31533a48c4f',
'e869e06b-df12-491c-b49f-c77c4daa857c',
'570b4537-7517-4543-bf45-94740b6c5af3',
'448765be-6c4c-4caa-a1ab-013cb20499f3',
'7009824b-b1e4-4a67-9fe7-7ed4cf63cbef',
'80773cc7-44a8-42b8-a3ad-cc30c5b8f866',
'ae2fcd3f-15f7-4879-940a-20ab6e865bde',
'7eb44e64-8515-4c9e-aa7a-03aadbd546b1',
'd070cf6e-287a-4fcd-83b2-92324fbced1c',
'4b85acca-a155-4935-b679-9ee4ad4a8f7f',
'8e3cd1db-c8ce-44b1-bb26-8c06421304ca',
'51a6caf1-9c75-4dba-9ee5-f4f6226a78f3',
'499dfc7b-a567-42b5-8c26-a017c87298e6',
'de803590-9afc-4340-8fdc-49d5b2454657',
'80cfbe92-d715-4309-9f13-0c12fa0abcb4',
'63f00054-c8a2-44eb-b0f1-44b3dbe6e571',
'574a6f59-d0d6-432d-bb04-bf52c14e3ccd',
'3b06674d-555f-4a67-8bd2-ca9c9f0755a6',
'1a42b0c7-e99c-4510-8cbb-25a82dac09e4',
'2dffebfc-8132-445e-a127-3a360208a7b3',
'0f17131c-6dad-43da-a559-fa009fdc3c54',
'e3a1422d-c3f3-41bc-8ff9-fd293d4fbbaa',
'5c9d8a11-f632-49ba-a467-449ef93b23dc',
'02be4d6c-76fa-410c-8968-49e5110d38b1',
'b2b6d762-5281-4841-81df-5514d0dd293d',
'0b7920b4-c0f5-402a-9c6c-b00acf540483',
'f0f0e6fd-5bdc-45cd-a3b0-1b65bb225b35',
'2e2b587e-6425-4a75-9dd5-c64c43a77dd9',
'a671cf89-2e48-4b2c-ba3a-78107caff583',
'e39be6ef-3c91-4e97-baee-2b23ea59808e',
'7b3bf64f-e4c1-48e3-b136-70177ee07fa8',
'049a805c-16f4-4224-b6a5-58382b0f2089',
'98ac8997-ba0b-4bfa-ab8e-8b8f56145689',
'e0b19500-a6b0-44dd-ba09-f492c4fe4fa4',
'a2446547-4c94-4df3-a6dc-ccfcdcbb4596',
'e99a0e34-b015-4047-a44f-66c40cbaff03',
'736a3672-ee50-403d-8e8c-26f615f127fa',
'4a595d2a-3c4e-48a3-aca0-8bf64aa368ad',
'fad57650-e793-4b0c-bb34-b47832c5cc90',
'f325a99c-f6a6-4ba7-96ae-7463a0002982',
'be8b6264-6dcf-42d1-95bc-cb048ca1fe0f',
'2e64d936-f31a-4dab-a40b-41422870e37c',
'fd2f7640-4095-40d2-92a8-7b4f09006d65',
'39566040-b33b-44f5-ad5a-d0bb8e4e9235',
'8cc6429c-0226-45bd-88df-ffe6a3047bcd',
'07ee2a54-17c9-4888-a180-2250fbc3d501',
'7c3596a7-6709-48db-aeb8-111ab2ea6c6c',
'adaebaa5-073a-4902-b723-b7446195c310',
'ecb2f86b-2a27-490e-9ebc-f35284e76095',
'21a5ac4d-0df1-4e15-93ad-5fe7c65439b4',
'b77019ec-e812-461d-9f32-2de64940dbc5',
'6ea5869c-cb2f-495e-a99b-d100f1779109',
'd7b10a4b-e588-4cf6-a5da-3064cab00af4',
'157729b7-1053-486a-aa79-1173ed01a895',
'f5ff2b6d-a217-47ea-bb74-12ed0c75f1b8',
'0ff0ae1e-ebc2-4e11-9f3b-2a08a6ffb377',
'9588aff0-ef50-4ef1-8819-bd0e8aaaca06',
'f7389969-2a16-484d-8dfe-ecf1b6f36d4b',
'ecffd606-cbdd-45d6-b89c-be339537c825',
'e80bbf0a-9142-4ee8-8a2b-5a0ffebe06bc',
'e56bca5d-49eb-4f0a-9585-7aa2854d4e25',
'85fc9de0-2ac2-43e3-bda9-64ef66044bfb',
'29271d9a-c768-4928-accc-7d61a52185d7',
'3f3a3f44-5734-4fa6-a62d-7f1c998a3259',
'fc9cc04c-d9e6-4f5d-ba39-be3113f9f9ce',
'95dcd908-a6b9-4422-a441-42caa46724f5',
'4bbfb75e-7b41-4f8f-b475-051d8c21860a',
'3410f695-4bbb-40c3-82e9-1991c30d4d6d',
'b60d18ca-25a0-4be2-b3c6-e42bf58140e5',
'9cdcfc4a-791f-45b4-8923-ed2011602e6c',
# added
# '0e1d55f3-c7dc-4355-a81f-e48a96795329',
# '36fbc01b-72bd-42f0-af0a-e1701cddcf94',
# 'b58d5ca0-1af8-48b3-993d-6c89742bb0d2',
# '89e23fc8-3f61-4480-9de3-358fe6eefe0b',
# '4d6cd31a-4559-4fd3-beff-4d47f5e2ee86',
# 'd6d6fe4c-425f-4ce7-bf28-7a6befaeb413'
]

# Esta funcion busca en OBIS [siempre que el parametro onlyInv sea False] todos los datasetsID que NO hagan parte de los que ya estan en el sibm
# en caso de que se le pase como argumento un verdadero, traera todos los datasetsID que tenba OBIS en el areadID 41, incluyendo los que tenemos
# ya en el SIBM
# en conclusion retorna la lista de datasetsID dependiendo del parametro pasado
#
# NOTA: Se debe crear una tabla en la base de datos o una vista, pero debe haber una forma de consultar los datasetsID que pertenecen al SIBM
#
def discard_datasets(onlyInv = False):
    #areas_harvesting = [41, 127]
    areas_harvesting = [41]
    CODIGOS_INV_DATASETS = [17925, 18300]
    total = 0
    datasets_validos = []
    url = 'https://api.obis.org/v3/dataset?areaid=' + str(41)
    #print('\n\n\n')
    #print(url)
    response = requests.get( url )
    #print('total datasets ', response.json()['total'])
    
    dic_resp = {x['id'] : (x['title'], x['url'] ) for x in response.json()['results']}
    v = {}
    nv = {}
    
    
    if onlyInv:
        for datasetid in dic_resp:
            datasets_validos.append(( datasetid ,  dic_resp[datasetid][0],  dic_resp[datasetid][1]))
    else:
        for datasetid in dic_resp:
            if datasetid not in datasets_id_inv:
                #print('no inv : ', datasetid)
                #print(datasetid ,  dic_resp[datasetid][0],  dic_resp[datasetid][1])
                datasets_validos.append(( datasetid ,  dic_resp[datasetid][0],  dic_resp[datasetid][1] ))
    return datasets_validos








def get_datasets_ids():
    """ Funcion que retorna una lista de id de datasets que no tienen como instituo a invemar """
    # codigos de datasets de  invemar
    CODIGOS_INV_DATASETS = [17925, 18300]

    url = 'https://api.obis.org/v3/dataset?areaid=41'
    response = requests.get( url )

    #return response.json()
    dic_resp = response.json()['results']
    datasets = [{x['id'] : x['institutes'] } for x in response.json()['results']]

    titulos_inv_datasets = []
    titulos_noinv_datasets = []

    datasets_nullos = []
    datasets_inv = []
    datasets_nov_inv = []
    datasets_errors = []

    for data_set in dic_resp:
        title = data_set['title']
        id_dataset = data_set['id']
        institutes = data_set['institutes']
        if institutes == None:
            datasets_nullos.append([id_dataset, title])
        elif type(institutes) == list:
            flag = False
            for instituto in institutes:
                if instituto['oceanexpert_id'] not in CODIGOS_INV_DATASETS:
                    flag = True
            if flag:
                datasets_nov_inv.append([id_dataset, title])
            else:
                datasets_inv.append([id_dataset, title])
        else:
            print('este, nada')
            print([id_dataset, title])
            datasets_errors([id_dataset, title])

    with open('nulos.csv', 'w', encoding='utf-8') as file:
        for x in datasets_nullos:
            file.write("{},{}\n".format(x[0], x[1]))
    with open('inv.csv', 'w', encoding='utf-8') as file:
        for x in datasets_inv:
            file.write("{},{}\n".format(x[0], x[1]))
    with open('noinv.csv', 'w', encoding='utf-8') as file:
        for x in datasets_nov_inv:
            file.write("{},{}\n".format(x[0], x[1]))

    return datasets_nov_inv
    return jsonify({
        'len no inv' : len(datasets_nov_inv),
        'datasets no inv' : datasets_nov_inv,
        'len inv' : len(datasets_inv),
        'datasets inv' : datasets_inv,
        'len null' : len(datasets_nullos),
        'datasets null' : datasets_nullos,
        'len errors' : len(datasets_errors),
        'datasets errors' : datasets_errors,
        })



# funcion que recorre todos las occurrencias del area 41 y al final graba un archivo csv con todas las variables posibless en los objetos dwc
# implementar para que ademas de el scrapping tambien traiga las variables desde la BD
def consult_obis_vars(size, count):
    print('in')
    count = int(count)
    url = 'https://api.obis.org/occurrence?areaid=41'
    ini_request = time.time()
    print('inicio de la peticion 1')
    response = requests.get( url + f'&size={size}')
    fin_request = time.time()
    print('fin de la peticion 1 {:.2f}'.format(fin_request - ini_request ))
    dic_resp = response.json()['results']
    flag_next = False
    i = 1
    all_vars = []
    while count > 0:
        try:
            last_id = dic_resp[-1]['id'] # ultimo id
        except:
            with open('data/variables_obis.csv', 'w', encoding='utf-8', newline='') as file:
                writer = csv.writer(file)
                writer.writerows(all_vars)
            break
        if flag_next:
            ini_time = time.time()
            print('inicio de la peticion {} '.format(i))
            response = requests.get( url + f'&size={str(size)}' + f'&after={last_id}' )
            print('fin de la peticion {} {:.2f}'.format(i , time.time() - ini_time))
            dic_resp = response.json()['results']
        else:
            flag_next = True
        for occ in dic_resp:
            for k in occ:
                if [k] not in all_vars:
                    all_vars.append([k])
        i += 1
        count -= 1

    with open('data/variables_obis.csv', 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(all_vars)
    print(all_vars)


def var_to_discriminator(size, count):
    """ Funcion que retorna una lista de variables que se repiten en todas las ocurrencias  """
    url = 'https://api.obis.org/occurrence?areaid=41'
    response = requests.get( url + f'&size={size}')
    dic_resp = response.json()['results']
    final_vars = []
    keys_obis = ['country', 'date_year', 'habitat', 'institutionID', 'scientificNameID', 'year', 'scientificName', 'county', 'dropped', 'hab', 'aphiaID', 'language', 'decimalLatitude', 'type', 'locationAccordingTo', 'occurrenceStatus', 'basisOfRecord', 'terrestrial', 'fieldNotes', 'nameAccordingToID', 'maximumDepthInMeters', 'id', 'day', 'verbatimEventDate', 'sampleSizeUnit', 'dataset_id', 'locality', 'stateProvince', 'decimalLongitude', 'verbatimLocality', 'date_end', 'occurrenceID', 'verbatimLatitude', 'date_start', 'month', 'samplingProtocol', 'parentEventID', 'eventDate', 'continent', 'eventID', 'brackish', 'scientificNameAuthorship', 'islandGroup', 'absence', 'samplingEffort', 'taxonRank', 'eventRemarks', 'originalScientificName', 'marine', 'minimumDepthInMeters', 'institutionCode', 'countryCode', 'verbatimLongitude', 'date_mid', 'nameAccordingTo', 'datasetName', 'geodeticDatum', 'taxonomicStatus', 'kingdom', 'waterBody', 'previousIdentifications', 'locationID', 'datasetID', 'kingdomid', 'sampleSizeValue', 'node_id', 'sss', 'depth', 'shoredistance', 'sst', 'bathymetry']
    flag_next = False
    i = 1
    while count > 0:
        print(f'Peticion {i}')
        i += 1
        last_id = dic_resp[-1]['id'] # ultimo id
        if flag_next:
            response = requests.get( url + f'&size={str(size)}' + f'&after={last_id}' )
            dic_resp = response.json()['results']
        else:
            flag_next = True
        for occ in dic_resp:
            li = []
            for var in occ:
                if var in keys_obis:
                    li.append(var)
            keys_obis = li
        count -= 1

    print(keys_obis)
    print('len ',len(keys_obis))

#var_to_discriminator(10000,30)

def inspect_dataset():
    """

    """
    # codigos de datasets de  invemar
    CODIGOS_INV_DATASETS = [17925, 18300]

    url = 'https://api.obis.org/v3/dataset?areaid=41'
    response = requests.get( url )

    #return response.json()

    datasets_nov_inv = [] # lista de ids de datasets que no son de invemar
    datasets_in = []
    dic_response = response.json()['results']
    datasets = [{x['id'] : x['institutes'] } for x in response.json()['results']]
    datasets2 = {}
    for x in dic_response:
        datasets2[x['id']] = x['title']
        print( '{} {} '.format(x['id'], x['title']))
    for data_obj in datasets:
        for data_id in data_obj:
            flag = False # bandera para saber si un dataset es de inv
            # print(data_obj)
            # break

            if data_obj[data_id] == None:
                datasets_nov_inv.append([data_id, datasets2[data_id]])
            elif type(data_obj[data_id]) == list:

                for institue in data_obj[data_id]:
                    if institue['oceanexpert_id'] in CODIGOS_INV_DATASETS:
                        flag = True
                if not flag:
                    print('to insert ', datasets2[data_id])
                    datasets_nov_inv.append([data_id, datasets2[data_id]])
                else:
                    datasets_in.append([data_id, datasets2[data_id]])
    print(' no invemar')
    for noIn in datasets_nov_inv:
        print( '{},{},'.format(noIn[0], noIn[1]) )
    print('invemar')
    for noIn in datasets_in:
        print( '{},{},'.format(noIn[0], noIn[1]) )

    print("""
          de invemar {}
          fuera de invemar {}
          """.format(len(datasets_in), len(datasets_nov_inv)))
    return datasets_nov_inv





def var(size, count):
    url = 'https://api.obis.org/occurrence?areaid=41'
    response = requests.get( url + f'&size={size}')
    dic_resp = response.json()['results']
    final_vars = []
    flag_next = False
    others_occ = []
    especials =[]
    i = 1
    total = 0
    #datasets_id que no son de invemar
    datasets_id_valid = inspect_dataset()

    # list de

    #print(len(keys_obis))
    while count > 0:
        print('peticion no: ', i)
        i +=1
        last_id = dic_resp[-1]['id'] # ultimo id
        if flag_next:
            # inicio_request = time.time()
            # print('antes de la peticion')
            response = requests.get( url + f'&size={str(size)}' + f'&after={last_id}' )
            # fin_request = time.time()
            # print('Tiempo de la request ', fin_request - inicio_request)
            dic_resp = response.json()['results']

        if dic_resp == []: # cuando no se encuentran occurencias se devuelbe una [] vacia
            print('Ultima peticion : {}'.format(url + f'&size={str(size)}' + f'&after={last_id}'))
            print('Ultimo result {}'.format(dic_resp))
            print('Ultimo id {}'.format(last_id))
            print('Fin de las peticiones')
            break


        for occ in dic_resp:
            total += 1
            if occ['dataset_id'] in datasets_id_valid:
                others_occ.append(occ['id'])
        count -= 1
    print(others_occ)
    print('Cantida ', len(others_occ))
    print('Total : ', total)


def datasets_with_title():
    url = 'https://api.obis.org/v3/dataset'
    datasets_names = {}
    for datasetid in datasets_id_inv:
        print('Peticion a : ' + url + '/' + datasetid)
        response = requests.get( url + '/' + datasetid)
        #return response.json()
        dts_id = response.json()['results'][0]['id']
        title = response.json()['results'][0]['title']
        institutes = []
        for institute in response.json()['results'][0]['institutes']:
            institutes.append(institute['name'])
        if datasetid != dts_id:
            datasets_names[datasetid] = {'otherID' : dts_id, 'title' : title, 'institutes': "|".join(institutes)}
        else:
            datasets_names[datasetid] = {'title' : title, 'institutes' : "|".join(institutes)}
    return datasets_names



def get_all_vars():
    vars_obis = set()
    first_ = True
    last_id = ""
    i = 1
    for x in range(30):
        if first_:
            url = 'https://api.obis.org/occurrence?areaid=41'
            print(f'peticion {i}')
            response = requests.get( url + f'&size={10000}')
            dic_resp = response.json()['results']    
            first_ = False
            for occ in dic_resp:
                for key in occ:
                    vars_obis.add(key)
                last_id = occ['id'] 
        else:
            url = 'https://api.obis.org/occurrence?areaid=41'
            response = requests.get( url + f'&size={10000}' + f'&after={last_id}')
            dic_resp = response.json()['results']    
            print(f'peticion {i}')
            for occ in dic_resp:
                for key in occ:
                    vars_obis.add(key)
                last_id = occ['id'] 
        i += 1
    print(vars_obis)

#var(10000,30)
if __name__ == '__main__':
    get_all_vars()
    #datasets_with_title()
    #consult_obis_vars(1000, 300)
    #discard_datasets()