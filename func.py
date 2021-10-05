import requests
import re
import time
import json
from flask import jsonify
def inspect_occ():
    """ funcion para ver que occurrencia no tiene datasetId """
    pass


def var(size, count):
    url = 'https://api.obis.org/occurrence?areaid=41'
    response = requests.get( url + f'&size={size}')
    dic_resp = response.json()['results']
    final_vars = []
    flag_next = False
    others_occ = []

    while count > 0:
        last_id = dic_resp[-1]['id']
        print(dic_resp[-1]['id'])
        if flag_next:
            inicio_request = time.time()
            print('antes de la peticion')
            response = requests.get( url + f'&size={str(size)}' + f'&after={last_id}' )
            fin_request = time.time()
            print('Tiempo de la request ', fin_request - inicio_request)
            dic_resp = response.json()['results']

        if dic_resp == []: # cuando no se encuentran occurencias se devuelbe una [] vacia
            print('Ultima peticion : {}'.format(url + f'&size={str(size)}' + f'&after={last_id}'))
            print('Ultimo result {}'.format(dic_resp))
            print('Ultimo id {}'.format(last_id))
            print('Fin de las peticiones')
            break

        # last_id = ''
        # print(dic_resp[-1]['id'])
        # print(dic_resp)
        for occ in dic_resp:
           print(occ.keys())
        if not flag_next:
            flag_next = True

        count -= 1


var(10,2)


