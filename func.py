import requests
import re
import time
import json
from flask import jsonify
import csv
import sql_scripts
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


# Esta funcion busca en OBIS [siempre que el parametro onlyInv sea False] todos los datasetsID que NO hagan parte de los que ya estan en la base de datos
# en caso de que se le pase como argumento un verdadero, traera todos los datasetsID que tenga OBIS en el areadID 41, incluyendo los que tenemos
# ya en la base de datos (esto sirve para hacer un full update de los datasets)

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
    datasets_id_inv = sql_scripts.datasets_id_inv() # traigo los datasets registrados en la base de datos y descartarlos del harvesting
    
    if onlyInv:
        for datasetid in dic_resp:
            datasets_validos.append(( datasetid ,  dic_resp[datasetid][0],  dic_resp[datasetid][1]))
    else:
        for datasetid in dic_resp:
            if datasetid not in datasets_id_inv:
                datasets_validos.append(( datasetid ,  dic_resp[datasetid][0],  dic_resp[datasetid][1] ))
    return datasets_validos



## funcion que trae los datastes en la base de datos de inv con fuente obis
def invemar_datasets():
    return sql_scripts.datasets_id_inv() 

# funcion para eliminar un dataset por su id
def delete_datasets(dataset_id):
    sql_scripts.delete_dataset(dataset_id)


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



# funcion que recorre todos las occurrencias del area 41 y al final graba un archivo csv con todas las variables posibles en los objetos dwc
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