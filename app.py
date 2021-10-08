from flask import Flask, request, jsonify
import json
from flask.scaffold import F
import requests
import re
import time
# instituteid=17925 
import func
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

@app.route('/bydataset')  
def inspect_dataset():
    """ 
        Funcion que retorna una lista de id de los datasets 
        que pertenecen al area de colombia, 
        pero que no son del instituto 
    """
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
    
    

@app.route('/get/v3/<size>/<count>')
def var(size, count):
    url = 'https://api.obis.org/occurrence?areaid=41'
    response = requests.get( url + f'&size={size}')
    dic_resp = response.json()['results']
    flag_next = False
    news_occ = []
    olders_occ = []
    i = 1
    total = 0
    #datasets_id que no son de invemar
    datasets_id_valid = func.discard_datasets()
    numeros_de_catalago = {}

    count = int(count)
    while count > 0:
        to_write_json = {}
        print('peticion no: ', i)
        i +=1
        last_id = dic_resp[-1]['id'] # ultimo id
        if flag_next:
            # inicio_request = time.time()
            # print('antes de la peticion')
            response = requests.get( url + f'&size={size}' + f'&after={last_id}' )
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
            try:
                numeros_de_catalago[occ['id']] = occ['catalogNumber']
            except:
                print('No tiene numero de catalog ', occ['id'] )
            total += 1
            if occ['dataset_id'] in datasets_id_valid:
                news_occ.append(occ)
                to_write_json[occ['id']] = occ
            else:
                olders_occ.append(occ)
        count -= 1
        flag_next = True #  flag to next request 
    
    with open(f'./data/numerosCatalogo.json', 'w') as file:
        file.write(json.dumps(numeros_de_catalago))
        
    # with open(f'./data/ocurrencias.json', 'w') as file:
    #     file.write(json.dumps(news_occ))
    
    print('Cantida ', len(news_occ), '')
    print('Total : ', total)
    return jsonify({
        'inv' : olders_occ,
        'news' : news_occ
    })





## vistas de pruebas
@app.route('/byid/<id>')
def get_occurrence_by_id(id):
    print('id :', id)
    res = requests.get('https://api.obis.org/v3/occurrence/' + id)
    return res.json()

@app.route('/bydataset-id/<id>')
def get_occurrence_by_dataset(id):
    response = requests.get( 'https://api.obis.org/v3/dataset/' + id)
    return response.json()



@app.route('/test')
def test():
    no_inv_datasets = inspect_dataset()
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



if __name__ == '__main__':
    app.run(debug=True)
