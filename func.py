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
'6b5c0712-d9c7-4e2f-b3a8-355ad2539081',
'c683d802-a110-4043-95b3-3a20a7c57608',
'08124763-5f33-485d-a3df-b6d502989ba4',
'0b1ec148-693f-4517-b112-386e17956314',
'f260b9a8-72a8-44fc-9ddc-383b7d7e54ff',
'0569339f-a376-42f4-8453-49ab9932713d',
'6142952e-7bbc-434f-a055-6bf140e6463d',
'48e988d2-f5f2-4b69-a1a1-6fe06e0fbd29',
'd79dbddb-aee1-4e33-82ef-58004f6603d7',
'58480117-eefa-49e7-abee-bad994d9d91b',
'dc590428-7a0e-4272-985e-b5ad279a245a',
'6e18e05f-a198-49e2-a59d-edd721f5a677',
'1d94f371-a3e5-4e9a-b48c-f26e864c7621',
'754347ff-b193-4665-8fc1-9792e3c1da55',
'573ef83d-6014-405c-b421-7c09505b179a',
'20c5a1df-f8a1-4963-8b98-e8000de0f8bd',
'9e3dea3d-6b47-4564-8616-14d54ffc153f',
'be7a16ea-3e22-4365-b066-a0e96c52d2a6',
'd070cf6e-287a-4fcd-83b2-92324fbced1c',
'4b85acca-a155-4935-b679-9ee4ad4a8f7f',
'51a6caf1-9c75-4dba-9ee5-f4f6226a78f3',
'499dfc7b-a567-42b5-8c26-a017c87298e6',
'3b06674d-555f-4a67-8bd2-ca9c9f0755a6',
'0f17131c-6dad-43da-a559-fa009fdc3c54',
'0b7920b4-c0f5-402a-9c6c-b00acf540483',
'39566040-b33b-44f5-ad5a-d0bb8e4e9235',
'7c3596a7-6709-48db-aeb8-111ab2ea6c6c',
'6ea5869c-cb2f-495e-a99b-d100f1779109',
'd7b10a4b-e588-4cf6-a5da-3064cab00af4',
'e80bbf0a-9142-4ee8-8a2b-5a0ffebe06bc',
'85fc9de0-2ac2-43e3-bda9-64ef66044bfb',
'3f3a3f44-5734-4fa6-a62d-7f1c998a3259',
'fc9cc04c-d9e6-4f5d-ba39-be3113f9f9ce',
'95dcd908-a6b9-4422-a441-42caa46724f5',
'4bbfb75e-7b41-4f8f-b475-051d8c21860a',
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
    for area in areas_harvesting:
        url = 'https://api.obis.org/v3/dataset?areaid=' + str(area)
        print('\n\n\n')
        print(url)
        response = requests.get( url )
        print('total datasets ', response.json()['total'])
        
        dic_resp = {x['id'] : x['citation_id'] for x in response.json()['results']}
        v = {}
        nv = {}
        
        if onlyInv:
            for datasetid in dic_resp:
                #v[datasetid] = dic_resp[datasetid]
                if dic_resp[datasetid] is not None:
                    datasets_validos.append(( datasetid ,  dic_resp[datasetid] ))
        else:
            for datasetid in dic_resp:
                if datasetid not in datasets_id_inv:
                    if dic_resp[datasetid] is not None:
                        datasets_validos.append(( datasetid ,  dic_resp[datasetid] ))
                # else:
                #     datasets_id_inv.remove( datasetid )
                #     nv[datasetid] = dic_resp[datasetid]
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