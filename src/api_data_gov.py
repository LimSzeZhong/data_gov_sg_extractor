# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 20:25:38 2024

@author: lim
"""

import requests
import json
from typing import Optional, List, Dict, Any, Tuple
import config as CFG

# url = f"https://api-production.data.gov.sg/v2/public/api/collections/2/metadata"
url = 'https://data.gov.sg/api/action/datastore_search?resource_id=d_dcda79be4aded5f9e769b8e23ff69b47&limit=1000'

json_content = requests.get(url).json()
content = json_content['result']
['resource_id', 'fields', 'records', '_links', 'total']
print(json_content['result'])
# print(json_content['data']['collectionMetadata'].keys())

# for x in json_content['data']['datasets']:
#     print(x['datasetId'])
#     print(x['name'])


def api_type(cd: str, inputitem: str) -> str:
    '''

    Parameters
    ----------
    cd : str
        either 'c' or 'd'.
        'c' will be for collections
        'd' will be for datasets
    inputitem : str
        either collectionId or datasetID

    Returns
    -------
    str
        DESCRIPTION.

    '''
    return None

def collection_info(collection_id: str) -> Tuple[str, str, List[str]]:
    '''
    Parameters
    ----------
    collection_id : str
        data.gov.sg collection_id

    Returns
    -------
    Tuple(str, str, List[str]
        returns the collection name, last updated date, and a list of datasetID

    '''
    url = CFG.api['collection'] + f'/{collection_id}/' + CFG.api['meta']
    
    json_content = requests.get(url).json()
    content = json_content['data']['collectionMetadata']
    
    collection_name = content['name']
    last_updated = content['lastUpdatedAt'][:10]
    dataset_id_list = content['childDatasets']
    
    return (collection_name, last_updated, dataset_id_list)

def dataset_info(dataset_id: str) -> Tuple[str, str, Dict[str,List[str]]]:
    '''
    Parameters
    ----------
    dataset_id : str
        data.gov.sg dataset_id

    Returns
    -------
    Tuple(str, str, Dict[str:List[str]])
        returns the collection name, last updated date, a dict of column info.
        The dict will contain mapping_id, name, datatype, description, index,
        as the keys.
    '''
    url = CFG.api['dataset'] + f'/{dataset_id}/' + CFG.api['meta']
    
    json_content = requests.get(url).json()
    content = json_content['data']
    content_inner = json_content['data']['columnMetadata']
    
    collection_name = content['name']
    last_updated = content['lastUpdatedAt'][:10]
    
    col_mapping_id = content_inner['order'] # returns a list
    col_name = []
    col_datatype = []
    col_description = []
    col_index = []
    
    for x in col_mapping_id:
        col_attr = content_inner['metaMapping'][x]
        col_name.append(col_attr['name'])
        col_datatype.append(col_attr['dataType'])
        col_description.append(col_attr['description'])
        col_index.append(col_attr['index'])
    
    col_info = {'col_mapping_id': col_mapping_id,
                'col_name': col_name,
                'col_datatype': col_datatype,
                'col_description': col_description,
                'col_index': col_index}
    
    return (collection_name, last_updated, col_info)

# def dataset_download(dataset_id):
#     url = CFG.api['dataset_dl'] + 


    