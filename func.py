import requests
import re
import time
import json
from flask import jsonify

# variables que tienen en comun todas las occurrencias
# l = ['scientificName', 'dropped', 'aphiaID', 'decimalLatitude', 'basisOfRecord', 'id', 'dataset_id', 'decimalLongitude', 'absence', 'originalScientificName', 'kingdom', 'kingdomid', 'node_id', 'shoredistance', 'bathymetry']

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

inspect_dataset()




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
    



#var(10000,30)
