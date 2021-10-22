from flask import Flask, request, jsonify
import json
from flask.scaffold import F
import requests
import re
import time
# instituteid=17925 
import func
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
    
    
# Esta es la funcion principal del programa
# cuando encedemos el servidor y hacemos una peticion GET a  localhosts:PORT/get/v3/<size>/<count>/<onlyInv>
# donde tenemos 3 parametros variables:
#   <size> : tamaño de occurrencias que hara por peticion
#   <count> : numero de peticiones que se haran a obis
#   <onlyInv>: este parametro es pasado como 'y' cuando queremos incluir en la consulta los datasets que ya se encuentran en el ipt del sibm
#   
# de tal manera que si consultamos
#   localhosts:port/get/v3/10/4 --> esto nos traera 40 registros biologicos que NO hagan parte del sibm
#   localhosts:port/get/v3/10/4/y --> esto nos traera 40 registros biologicos que pueden o no hacer parte del sibm
#
@app.route('/get/v3/<size>/<count>/<onlyInv>')
def var(size, count, onlyInv):
    
    # traemos la lista de nombres de variables
    all_name_vars = []
    with open('data/variables_obis.csv', 'r') as file:
        reader = csv.reader(file, delimiter=',')
        for row in reader:
            all_name_vars.append(row[0])
            #print('adding {} to the list'.format(row[0]))
       
    #return jsonify({'vars' : all_name_vars})

    url = 'https://api.obis.org/occurrence?areaid=41'
    response = requests.get( url + f'&size={size}')
    dic_resp = response.json()['results']
    flag_next = False
    news_occ = []
    olders_occ = []
    catalogNumbers = {}
    to_write = ''
    i = 1
    total = 0
    #datasets_id que no son de invemar
    if onlyInv == 'y':
        datasets_id_valid = func.discard_datasets(True)
    else:
        datasets_id_valid = func.discard_datasets(False)
    count = int(count)
    
    to_write_csv = []
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
            # aux_list_csv = []
            # for var in occ:
            #     aux_list_csv.append(occ[var])
            # to_write_csv.append(aux_list_csv)
            total += 1
            inv_flag = False
            # for var in occ:
            #     if occ['dataset_id'] in func.datasets_id_inv:
            #         try:
            #             catalogNumbers[occ['id']] = occ['occurrenceID']
            #             id_ = occ['id'] if occ['id'] else None
            #             occ_id = occ['occurrenceID'] if occ['occurrenceID'] else None
            #             catalog_ = occ['catalogNumber'] if occ['catalogNumber'] else None
            #             to_write += '{},{},{}\n'.format(id_, occ_id, catalog_)
            #         except:
            #             continue
            #             #catalogNumbers[occ['id']] = None
            #         finally:
            #             break
            
            if occ['dataset_id'] in datasets_id_valid:
                aux_occ = {} # diccionario auxiliar de occurencias para que las variables que no tenga esta ocurrencia se pasen en un str vacio ''
                occ_values = [] # 
                for var in all_name_vars:
                    if var not in occ.keys():
                        aux_occ[var] = ''
                    else:
                        aux_occ[var] = occ[var]
                    occ_values.append(aux_occ[var])
                    
                news_occ.append(aux_occ) # lista a retornar 
                to_write_json[occ['id']] = aux_occ # para escribir json
                to_write_csv.append(occ_values) # para escribir los datos en csv
            else:
                olders_occ.append(occ)
        count -= 1
        
        flag_next = True #  flag to next request 
    
    # with open(f'./data/numerosCatalogo.json', 'w') as file:
    #     file.write(json.dumps(catalogNumbers))
        
    print('LEN JSON', len(catalogNumbers) )
    
    # with open(f'./data/obis.txt', 'w') as file:
    #     file.write(to_write)
        
    with open('data/obis_csv2.csv', 'w', encoding='utf-8', newline='') as file:
        write = csv.writer(file, delimiter='|')
        write.writerow(all_name_vars)
        write.writerows(to_write_csv)
        
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
    d = func.datasets_with_title()
    return jsonify({
        'datasets' : d,
        'len': len(d) 
    })


if __name__ == '__main__':
    app.run(debug=True)
