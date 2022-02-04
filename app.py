from flask import Flask, request, jsonify
import json
from flask.scaffold import F
import requests
import re
import time
# instituteid=17925 
import func
import sql_scripts
import csv
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


@app.route('/harvesting')
def harvesting():
    datasets_validos = func.discard_datasets(False) # datasets que no esten en la base de datos y que pertenezcan al area 41 
    total = 0
    i = 0
    dict_vars = sql_scripts.create_dic_var()
    
    count_inserts = 0
    
            
    for dataset in datasets_validos:
        
        occurrences = []
        max_size = 10000
        dataset_id, title, url_datasert = dataset
        
        #if not re.search('ipt.biodiversidad.co/sibm', url_datasert):  # este condicional impide que venga cualquier dataset del ipt del siam
        #if dataset_id in datasetsid_in_db: # si dejamos este condicional traera los que no tenemos en la base de datos, incluso si estan en el ipt del siam
        
        url = "https://api.obis.org/v3/occurrence?areaid=41&datasetid=" + dataset_id
        response = requests.get(url)
        size = response.json()['total']
        
        total += size
        last_one = ''
        
        while True:
            if size <= max_size:
                url2 = url + f'&size={size}&after={last_one}'
                print("url menos de 10000 ::  ", url2)
                response2 = requests.get(url2)
                occurrences.extend(response2.json()['results'])
                break # este break rompe el while infinito cuando encuentra que el numero de ocrruencias en el datasets es menos a 10.000 y puede traerlas en una sola peticion
            elif size > max_size:
                size -= max_size
                url2 = url + f'&size={max_size}&after={last_one}'
                print("url mas de 10000 ::  ", url2)
                response2 = requests.get(url2)
                occurrences.extend(response2.json()['results'])
                last_one = response2.json()['results'][-1]['id']
                
        sql_scripts.insert_data(occurrences, dataset_id, title, url_datasert, dict_vars)
        count_inserts += 1
        
            
    print(f"Se insertaron {count_inserts} datasets")
    return jsonify({
        'occurrencias' : 'ok'
    })
        
    print('Total: ', total)
            
        
@app.route('/harvesting-full-update')
def update_harvesting():
    """
        Esta funcion cumplira con la funcion de traer todos los dataset de obis
        Si se encuentran en la db, se eliminaran y se re-insertaran 
        Si no estaban en la db, se insertaran normalmente.
    """
    datasets_validos = func.discard_datasets(True) # todos los datasets incuyendo los que estan en la db que hacen parte de obis
    datasetsid_in_db = func.invemar_datasets()
    dict_vars = sql_scripts.create_dic_var()
    count_inserts = 0
    
    total = 0
    # count_update = 0
    # for d in datasets_validos:
    #     dataset_id, title, url_datasert = d
    #     if re.search('ipt.biodiversidad.co/sibm', url_datasert):
    #         print('ESTA EN EL IPT Nombre: {}  || datasetid : {} || url:  {}'.format(title, dataset_id, url_datasert))
    #     elif dataset_id in datasetsid_in_db:
    #         print('ESTA EN LA DB  Nombre: {}  || datasetid : {} '.format(title, dataset_id))
    #         count_update += 1
    #     else:
    #         print('Este esta bien Nombre: {}  || datasetid : {} '.format(title, dataset_id))
    #         count_update += 1

    # print('Para updatear ', count_update)
    # return jsonify({'ok': 'ok'})
    
    
    
    for dataset in datasets_validos:
        occurrences = [] 
        max_size = 10000 # maximo de ocurrencias que se pediran a la api de obis por peticion
        dataset_id, title, url_datasert = dataset
        
        url = "https://api.obis.org/v3/occurrence?areaid=41&datasetid=" + dataset_id
        response = requests.get(url)
        size = response.json()['total']

        total += size
        last_one = ''
        
        # eliminamos el dataset si se encuentra en la db, y luego se inserta con los valores nuevos
        if dataset_id in datasetsid_in_db:
            func.delete_datasets(dataset_id)
            
        if not re.search('ipt.biodiversidad.co/sibm', url_datasert):  # solo ignoramoms los que estan en el ipt
           
            url = "https://api.obis.org/v3/occurrence?areaid=41&datasetid=" + dataset_id
            response = requests.get(url)
            size = response.json()['total']
            
            total += size
            last_one = ''
            
            while True:
                if size <= max_size:
                    url2 = url + f'&size={size}&after={last_one}'
                    print("url menos de 10000 ::  ", url2)
                    response2 = requests.get(url2)
                    occurrences.extend(response2.json()['results'])
                    break # este break rompe el while infinito cuando encuentra que el numero de ocrruencias en el datasets es menos a 10.000 y puede traerlas en una sola peticion
                elif size > max_size:
                    size -= max_size
                    url2 = url + f'&size={max_size}&after={last_one}'
                    print("url mas de 10000 ::  ", url2)
                    response2 = requests.get(url2)
                    occurrences.extend(response2.json()['results'])
                    last_one = response2.json()['results'][-1]['id']
                    
            sql_scripts.insert_data(occurrences, dataset_id, title, url_datasert, dict_vars)
            count_inserts += 1            

    return jsonify({
        'occurrencias' : 'ok'
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
    d = func.datasets_with_title()
    return jsonify({
        'datasets' : d,
        'len': len(d) 
    })


if __name__ == '__main__':
    app.run(debug=True)
