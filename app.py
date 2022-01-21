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
    for dataset in datasets_validos:
        
        occurrences = []
        max_size = 10000
        dataset_id, title, url_datasert = dataset
        
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
        
        flag_dataset_success = sql_scripts.insertar_dataset(dataset_id, title, url_datasert)
        print("total ", total)
        total_ocurrencias = len(occurrences)   
        init_pos = 0
        max_size = 5000
        next_pos = max_size
        while flag_dataset_success:
            if total_ocurrencias > max_size:
                sql_scripts.insert_occurrence(occurrences[init_pos:next_pos], dataset_id, title, url_datasert, dict_vars)
                init_pos += max_size
                next_pos += max_size
                total_ocurrencias -= max_size
                print("insertando las occurrencias del dataset {} ::: occreuncias restantes {}".format(dataset_id,total_ocurrencias) )     
            elif total_ocurrencias <= max_size:
                sql_scripts.insert_occurrence(occurrences[init_pos:len(occurrences)], dataset_id, title, url_datasert, dict_vars)
                    
        
            
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
    datasets_validos = func.discard_datasets(True) # todos los datasets incuyendo los que estan en la dv que hacen parte de obis
    datasetsid_in_db = func.invemar_datasets()
    dict_vars = sql_scripts.create_dic_var()
    
    total = 0
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
