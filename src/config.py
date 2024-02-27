# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 20:30:40 2024

@author: lim
"""

###### Datasets ######
datasets = {
    'acra_ent' : 'datasets/d_3f960c10fed6145404ca7b821f263b87',
    'acra_reg_ent' : 'collections/1',
    'bca_con' : 'datasets/d_dcda79be4aded5f9e769b8e23ff69b47',
    'hdb_con' : 'datasets/d_9973d2c119ed4dd1560aebf8f0829b86'
    }

###### APIS ######
api = {
       'collection': 'https://api-production.data.gov.sg/v2/public/api/collections',
       'dataset': 'https://api-production.data.gov.sg/v2/public/api/datasets',
       'meta': 'metadata',
       'dataset_dl': 'https://data.gov.sg/api/action/datastore_search?resource_id='
       }

collection_list = 'https://api-production.data.gov.sg/v2/public/api/collections?page=2'
collection_metadata = 'https://api-production.data.gov.sg/v2/public/api/collections/1/metadata'
dataset_list = 'https://api-production.data.gov.sg/v2/public/api/datasets?page=2'
dataset_metadata = 'https://api-production.data.gov.sg/v2/public/api/datasets/d_dcda79be4aded5f9e769b8e23ff69b47/metadata'
dataset_info = 'https://data.gov.sg/api/action/datastore_search?resource_id=d_dcda79be4aded5f9e769b8e23ff69b47'