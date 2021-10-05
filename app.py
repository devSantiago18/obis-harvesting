from flask import Flask, request, jsonify
import json
from flask.scaffold import F
import requests
import re
import time
# instituteid=17925 

app = Flask(__name__)

URL_OBI  = 'https://api.obis.org/occurrence?areaid=41&'
size_url = '&size='

INV_CODES = [
    "Instituto de Investigaciones Marinas y Costeras",
    "Instituto de Investigaciones Marinas y Costeras “José Benito Vives de Andréis” (Invemar)",
    "INVEMAR",
    "NIT:800250062"
]
   
pattern = '(INVEMAR|NIT:800250062|MHNMC)'

#@app.route('/bydataset')
def by_dataset():
    """ 
        Funcion que retorna una lista de id de los datasets 
        que pertenecen al area de colombia, 
        pero que no son del instituto 
    """
    # codigos de datasets de  invemar
    CODIGOS_INV_DATASETS = [17925, 18300]

    url = 'https://api.obis.org/v3/dataset?areaid=41'
    response = requests.get( url )
    datasets_nov_inv = [] # lista de ids de datasets que no son de invemar
    datasets = [{x['id'] : x['institutes'] } for x in response.json()['results']]
    for obj in datasets:
        for key in obj:
            if obj[key] == None:
                datasets_nov_inv.append(key)
                #print(key, obj[key])
            if type(obj[key]) == list:
                flag_nov = False # bandera para saber si ninguno de los elemenos del elemento contiene los codigos de inv
                for element in obj[key]:
                    if element['oceanexpert_id'] in CODIGOS_INV_DATASETS:
                        flag_nov = True #
                        #print(key, obj[key])
                        print()  
                if not flag_nov:
                    #print(key, obj[key])
                    datasets_nov_inv.append(key)
    #return jsonify( len(datasets_nov_inv ))
    #return response.json()
    return datasets_nov_inv
    datasets_inv = []
    datasets_no_inv = []
    errors = []
    for data in datasets:
        value = 0
        try:
            i = len(data['institutes'])
            values = []
            for x in range(i):
                #print('pos: ' , x, i)
                #print(data['institutes'])
                try:
                    val = int(data['institutes'][i - 1]['oceanexpert_id'])
                except TypeError:
                    val = 0
                values.append(val)
            if [x for x in values if x not in CODIGOS_INV_DATASETS] != []:
                #datasets_no_inv.append( {data['id'] : data['institutes']} )
                datasets_no_inv.append( data['id'] )
            else:
                datasets_inv.append( data['id'] )
                #datasets_inv.append( {data['id'] : data['institutes']} )
        except TypeError:
            #datasets_no_inv.append( {data['id'] : data['institutes']} )   
            datasets_no_inv.append( data['id'])   
    return datasets_no_inv


@app.route('/get/v2/<size>/<count>')
def get_v2(size, count):
    no_inv_datasets = by_dataset()


@app.route('/test')
def test():
    no_inv_datasets = by_dataset()
    print(len(no_inv_datasets))
    occ_no_inv = []
    i = 1
    for dataset_id in no_inv_datasets:
        print('Peticion no: {} para el datasetId {}'.format(i, dataset_id))
        t_i = time.time()
        
        response = requests.get(URL_OBI  + f'&datasetid={dataset_id}') 
        
        print('timepo de la peticion {}'.format(time.time() - t_i))
        occ_no_inv += response.json()['results']
        i +=1
    return jsonify({
        'total' : len(occ_no_inv),
        'occ' : occ_no_inv
        })
    return response.json()

@app.route('/get/<size>/<count>')
def get_occurrences(size, count):
    
    total_inicio = time.time()
    inicio_request = time.time()
    print('antes de la peticion')

    response = requests.get(URL_OBI  + f'&size={size}') 

    fin_request = time.time()
    print('Tiempo de la request ', fin_request - inicio_request)

    dic_resp =  response.json()['results']
    inv_occ = []
    counter_inv  = 0 # cuantas occurencias son de inv
    counter_other  = 0 # cuantas occurencias no son de inv
    count = int(count) # cuantas vueltas da el codigo haciendo [size] peticiones
    flag_next = False
    datasets_no_inv = by_dataset() # obtengo la lista de id de datasets que no son de inv
    others_occ = []

    while count > 0:
        
        if flag_next:
            inicio_request = time.time()
            print('antes de la peticion')
            response = requests.get( URL_OBI + f'&size={str(size)}' + f'&after={last_id}' )
            fin_request = time.time()
            print('Tiempo de la request ', fin_request - inicio_request)
            dic_resp = response.json()['results']
        
        if dic_resp == []: # cuando no se encuentran occurencias se devuelbe una [] vacia
            print('Ultima peticion : {}'.format(URL_OBI + f'&size={str(size)}' + f'&after={last_id}'))
            print('Ultimo result {}'.format(dic_resp))
            print('Ultimo id {}'.format(last_id))
            print('Fin de las peticiones')
            break

        last_id = ''

        print('antes del proceso')
        proceso_ini = time.time()
        for occ in dic_resp:
            #flag = False
            #return jsonify({'occurrencia_de_mierda' : occ, 'occurencias' : datasets_no_inv })
            if occ['dataset_id'] in datasets_no_inv:
                counter_other += 1
                others_occ.append(occ)
            else:
                counter_inv += 1
        if not flag_next : 
            flag_next = True
        
        proceso_fin = time.time()
        print('tiempo del proceso', proceso_fin - proceso_ini)
        print('Ultima peticion : {}'.format(URL_OBI + f'&size={str(size)}' + f'&after={last_id}'))
        count -= 1

        with open('data.json', 'a+') as file:
            file.write(json.dumps(others_occ))

    total_fin = time.time()
    print("Ya se acabo primo  ", total_fin - total_inicio)

    return jsonify({
        'len_others' : counter_other,
        #'inv'    : inv_occ,
        'len_inv' : counter_inv,
        #'occurrences_others' : others_occ
    })
        



@app.route('/byid/<id>')
def get_occurrence_by_id(id):
    res = requests.get(URL_OBI + '&id=' + id)
    return res.json()




if __name__ == '__main__':
    app.run(debug=True)
