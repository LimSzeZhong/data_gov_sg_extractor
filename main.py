# -*- coding: utf-8 -*-
"""
Created on Fri Mar  1 23:01:56 2024

@author: lim
"""

from src.api_data_gov import download_collection

col_id = 2
chk = 'Yes'
combcsv = 'Yes'
indcsv = 'Yes'
csvdir = 'data'
df_combined = download_collection(col_id, chk, combcsv, indcsv, csvdir)
print(df_combined)
