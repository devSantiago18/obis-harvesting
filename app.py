from flask import Flask, request, jsonify
import json
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

    # codigos de datasets de  invemar
    CODIGOS_INV_DATASETS = [17925, 18300]

    url = 'https://api.obis.org/v3/dataset?areaid=41'
    response = requests.get( url )
    datasets = [x for x in response.json()['results']]
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

    #return jsonify({
    #    'inv' : datasets_inv,
    #    'noInv' : datasets_no_inv,
    #    'errors' : errors
    #})


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
    counter_inv  = 0
    counter_other  = 0
    count = int(count)
    flag_next = False
    datasets_no_inv = by_dataset()
    while count > 0:
        
        others_occ = []
        if flag_next:
            inicio_request = time.time()
            print('antes de la peticion')
            response = requests.get( URL_OBI + f'&size={str(size)}' + f'&after={last_id}' )
            fin_request = time.time()
            print('Tiempo de la request ', fin_request - inicio_request)
            dic_resp = response.json()['results']
        
        if dic_resp == []:
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
        print('escribiendo archivo')
        with open('data.json', 'a') as file:
            file.write(json.dumps(others_occ))

        count -= 1
    total_fin = time.time()
    print("Ya se acabo primo  ", total_fin - total_inicio)

    return jsonify({
        'len_others' : counter_other,
        #'inv'    : inv_occ,
        'len_inv' : counter_inv
    })
        



@app.route('/byid/<id>')
def get_occurrence_by_id(id):
    res = requests.get(URL_OBI + '&id=' + id)
    return res.json()




if __name__ == '__main__':
    app.run(debug=True)
