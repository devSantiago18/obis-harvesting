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

@app.route('/harvest/<size>/<count>')
def harvest(size, count):
    """ funcion que me retorna las occurrencias que no son de invemar
        1. numero total de ellas
        2. persistencia de estas en la db
    """
    print("total iterations: ", count)
    inicio_request = time.time()
    resp = requests.get(URL_OBI + f'&size={size}') 
    fin_request = time.time()
    print('Tiempo de la request ', fin_request - inicio_request)
    dic_data = resp.json()['results']
    occ_arr = []
    var_search = "institutionCode"
    for x in range(int(count)):
        aux_id_occurrences = []
        
        for occurrence in dic_data:
            # aqui van las validaciones y eso 
            occ_arr.append(occurrence)
            aux_id_occurrences.append(occurrence['id'])

            # if  var_search in list(occurrence.keys()):
                
        new_id = dic_data[-1]['id']
        resp = requests.get(URL_OBI + f'&size={size}&after={new_id}')
        dic_data = resp.json()['results']
        
        #print(f'Primeros {size} ocurrencias\n{aux_id_occurrences}')
    return jsonify({ 'results' : occ_arr, 'len' : len(occ_arr)})


@app.route('/bydataset')
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
                datasets_no_inv.append( {data['id'] : data['institutes']} )
            else:
                datasets_inv.append( {data['id'] : data['institutes']} )
        except TypeError:
            errors.append( {data['id'] : data['institutes']} )
        


    return jsonify({
        'inv' : datasets_inv,
        'noInv' : datasets_no_inv,
        'errors' : errors
    })

    return response_dataset.json()
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
    maybe = ['eventID', 'institutionCode', 'occurrenceID', 'datasetID', 'institutionID', 'collectionCode']
    inv_occ = []
    others_occ = []
    counter_inv  = 0
    counter_other  = 0
    count = int(count)
    flag_next = False
    while count > 0:
        
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
            flag = False
            for key in occ:
                if key in maybe and re.findall(pattern, occ[key]):
                    flag = True
            if not flag :
                others_occ.append(occ)
                counter_other += 1
            else:
                inv_occ.append(occ)
                counter_inv += 1
            last_id = occ['id']

        if not flag_next : 
            flag_next = True
        
        proceso_fin = time.time()
        
        print('tiempo del proceso', proceso_fin - proceso_ini)
        print('Ultima peticion : {}'.format(URL_OBI + f'&size={str(size)}' + f'&after={last_id}'))
        
        count -= 1
    
    total_fin = time.time()
    print("Ya se acabo primo  ", total_fin - total_inicio)
    return jsonify({
        'others' : others_occ,
        'len_others' : counter_other,
        'inv'    : inv_occ,
        'len_inv' : counter_inv
    })
        




@app.route('/normal/<size>') 
def main(size):
    res = requests.get(URL_OBI + size_url + str(size))
    dic_resp = res.json()['results']
    with_flag = []
    without_flag = []
    institutes_codes = set()
    var_search = "institutionCode"
    away_inv = []
    inv_occ = []
    for occ in dic_resp:
        if  var_search in list(occ.keys()):
            with_flag.append(occ)
            institutes_codes.add(occ[var_search])
            if occ[var_search] not in INV_CODES:
                away_inv.append(occ['id'])
            else:
                inv_occ.append(occ['id'])

        else:
            without_flag.append(occ['id'])
    #print(dic_resp[-1])
    institutes_codes = list(institutes_codes)
    return jsonify({
        'institutionCode' : institutes_codes,
        #'occByInvemar' : inv_occ,
        'withoutInstituteCode' : without_flag,
        #'lenOccByInvemar' : len(inv_occ),
        'len_without' : len(without_flag),
        'otherInstitutesOccurrences' : away_inv,
        'lenOtherInstitutesOccurrences' : len(away_inv)
    })


@app.route('/byid/<id>')
def get_occurrence_by_id(id):
    res = requests.get(URL_OBI + '&id=' + id)
    return res.json()




if __name__ == '__main__':
    app.run(debug=True)
