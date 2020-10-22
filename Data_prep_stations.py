# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 17:06:58 2020

@author: J0545269
"""

import datetime
import os
import pandas as pd
import numpy as np
import cx_Oracle
import itertools

os.getcwd()
os.chdir("E:/Maverick/PROD/Modele_1_Toutes_Stations")
os.getcwd()

from fonctions_date import *
from fonctions_modele_1 import *



#==============================================================================
# Extraction des stations Total
#==============================================================================

def tp_tot(conn_DS):
    query = (" SELECT "
             " TERMINAL_ID "
             " FROM "
             " MABI_DBA.VATI_TERMINAL "
              )
    output = import_data(conn_DS, query)
    return output

DataStore_P_RO = cx_Oracle.Connection("MABI_READER/datalake2015@frmssvdorap02-scan.main.glb.corp.local:1521/MABIP")
print('-'*60)
print("Début d'Import Liste:", datetime.datetime.now())
df_tp = tp_tot(DataStore_P_RO)
print("Fin d'Import Liste:", datetime.datetime.now())
print('-'*60)



#==============================================================================
# Création de la liste des stations Total
#==============================================================================

list_tp = df_tp['TERMINAL_ID'].tolist()



#==============================================================================
# Création de la liste dates de prédiction
#==============================================================================

list_i = []
for i in range(0,Nb_pred):
    x = datetime2singlestr(datetime.datetime.now() + datetime.timedelta(days=+i))
    list_i.append(x)

    
  
#==============================================================================
# Création de la liste tranches horaires
#==============================================================================
    
list_th = ['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18','19','20','21','22','23']



#==============================================================================
# Un seul dataframe
#==============================================================================

def expandgrid(*itrs):
   product = list(itertools.product(*itrs))
   output = pd.DataFrame({'Var{}'.format(i+1):[x[i] for x in product] for i in range(len(itrs))})
   return output

df_a = expandgrid(list_tp,list_i,list_th)
df_a.columns=['TERMINAL_ID','dateprev','TRA_HOUR']
 


df_b = expandgrid(list_tp,list_th)
df_b.columns=['TERMINAL_ID','TRA_HOUR']
#==============================================================================
# Merge + fillna
#==============================================================================



#df_outer = pd.merge(pred,df_a,how='outer')
#df_outer = df_outer.drop_duplicates


#df_leftinner = pd.merge(pred,df_a,
                       # left_on='STATION',
                       # right_on='STATION',
                      #  how='left')
#df_leftinner = df_leftinner.drop_duplicates
