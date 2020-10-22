# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 17:06:58 2020

@author: J0545269
"""

#==============================================================================
# Préparation du log.txt
#==============================================================================



import sys
import os
import traceback
os.chdir("E:\Maverick\CockpitGerant")
from fonctions_date import *
os.getcwd()
os.chdir("E:/Maverick/PROD/CockpitGerant/LOG")
date_calcul = datetime2singlestr(datetime.datetime.now())
xtemp = sys.stdout
sys.stdout= open(""+date_calcul+"_log.txt", 'w')
print("Création du Fichier Log")
os.chdir("E:/Maverick/CockpitGerant")
try:
    print('-'*60)
    print(datetime.datetime.now())
    from fonctions_modele_1 import * 
    from Data_prep_stations import *
    print(datetime.datetime.now())
    print('-'*60)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)



#==============================================================================
# Import des librairies
#==============================================================================



print('-'*60)
print('--- Import des librairies ---- ')
print('-'*60)
import datetime
import numpy as np
import pandas as pd
import cx_Oracle
from sklearn.cluster import AgglomerativeClustering
os.getcwd()
os.chdir("E:/Maverick/CockpitGerant/")
os.getcwd()



#==============================================================================
# Chargement des transactions par heure
#==============================================================================



print('-'*60)
print("Process start:")
print(datetime.datetime.now())
print('-'*60)

print('--- Import des Macro Paramètres et fonction d\'import---- ')
paths = {"transac_heure" : "./PICKLE/data_interim/1_transac_par_h_tp.pkl",
         "profil_station" : "./PICKLE/data_interim/profil_tp.pkl", 
         "clusters_stations" : "./PICKLE/data_interim/clusters_tp.pkl"  }


print('-'*60)
print('--- Import des paramètres de run ----------- ')
print('-'*60)
try:        
    #Make your query here
    extract_start = datetime2singlestr(datetime.datetime.now() + datetime.timedelta(days=-30-365))
    extract_end = datetime2singlestr(datetime.datetime.now() +   datetime.timedelta(days= 30-365))
    print("Début d'extraction:", extract_start)
    predict_start = datetime2singlestr(datetime.datetime.now() + datetime.timedelta(days=+1))
    print("Début de Prédiction:", predict_start)
    Nb_pred = 7
    print("Nombre de jours de prédiction:", Nb_pred)
    n_clust = 15 
    print("Nombre de clusters:", n_clust)
    nb_jrs_min = n_clust +1  
    print("Nombre de jours minimum:", nb_jrs_min)
    horizon_mjmm = 30
    print("Horizon même jour même mois:", horizon_mjmm)
    horizon_last = 60
    print("Horizon Last:", horizon_last)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)

os.chdir("E:/Maverick/CockpitGerant/")													



# =============================================================================
# Préciser si on importe les données ou si on fait une requête
# =============================================================================



type_import = 'datastore' # 'pkl' # types_import = ['datastore', 'donnes_sauvegardees']
write_pkl = True

try:
    DataStore_P_RO = cx_Oracle.Connection("MABI_READER/datalake2015@frmssvdorap02-scan.main.glb.corp.local:1521/MABIP")
    #DataStore_D_RW = cx_Oracle.Connection("MABI_DEV/aug#1234@frmssvdorar01-scan.main.glb.corp.local:1521/MABIE")
    
    # =========================================================================
    # Lancement de la fonction de chargement des données brutes
    # =========================================================================
    
    print('-'*60)
    print('--- Import / ou chargement des transactions par heure ---- ')
    print(datetime.datetime.now())
    transac_par_h = load_data(type_import, paths["transac_heure"], extr_start=extract_start ,pred_start=extract_end )
    print(' Fin d\'import')
    print(datetime.datetime.now())
    print('-'*60)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)



# =============================================================================
# Sauvegarde des données brutes
# =============================================================================



try:
    if write_pkl :
        print('-'*60)
        print("Rédaction du pickle Transactions par Heure")
        print(datetime.datetime.now())
        transac_par_h.to_pickle("./PICKLE/data_interim/1_transac_par_h_tp.pkl")	
        print("Fin de rédaction")
        print(datetime.datetime.now())
        print('-'*60)
    						
    
    # =========================================================================
    # Deuxième Bloc : préparation de la table pour le clustering 
    # =========================================================================
    if type_import != 'datastore' :
        transac_par_h = pd.read_pickle("./PICKLE/data_interim/1_transac_par_h_tp.pkl")
        print('-'*60)
        print("Lecture du pickle Transactions par Heure")
        print('-'*60)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)



# =============================================================================
# Traitements des données
# =============================================================================



print('-'*60)
print('--- préparation referentiel dates et tp ---- ')
print('-'*60)
try:    
    referentiel = transac_par_h[['TERMINAL_ID', 'TRA_DATE','TERMINAL_DATE_STR']].drop_duplicates()
    referentiel["KEY"] = referentiel.TERMINAL_ID + referentiel.TRA_DATE.apply(datetime2str)
    
    print('-'*60)															
    print('--- Calcul Transactions par jour ---- ')
    print('-'*60)
    # Nombre de transactions par jour
    aggregate = { "TRA" : "sum"}
    group = ('TERMINAL_ID', 'TRA_DATE')
    transac_par_j  = transac_par_h.groupby(group, as_index=False).agg(aggregate)						
    transac_par_j  = transac_par_j.rename(columns={"TRA" : "TRA_DAY"})
    #transac_par_h.shape[0]/transac_par_j.shape[0]													
    print('-'*60)
    print('--- Calcul du profil horaire ---- ')
    print('-'*60)
    transac_par_h = transac_par_h.merge(transac_par_j [['TERMINAL_ID','TRA_DATE','TRA_DAY']],				
                                              left_on  = ['TERMINAL_ID','TRA_DATE'],		
                                              right_on = ['TERMINAL_ID','TRA_DATE'],					
                                              how = 'left')					
    transac_par_h["PROFIL_H"] = transac_par_h.TRA / transac_par_h.TRA_DAY							
							
    
    transac_par_h.head()
    print('-'*60)
    print('---- Filtering tp  ---- ')		
    print('-'*60)
    
    # Filtre sur les tp à predire
    aggregate = { "TRA_DATE" : "nunique"}
    group = ('TERMINAL_ID')														
    
    temp = transac_par_j.groupby(group, as_index=False).agg(aggregate) #pas simple
    list_tp = temp.TERMINAL_ID.loc[temp.TRA_DATE > nb_jrs_min] #IMPORTANT
    
    transac_par_j = transac_par_j.loc[transac_par_j.TERMINAL_ID.isin(list_tp)]
    transac_par_h = transac_par_h.loc[transac_par_h.TERMINAL_ID.isin(list_tp)]
    # Suppression des données à prédire
    transac_par_j = transac_par_j.loc[transac_par_j.TRA_DATE < predict_start]
    transac_par_h = transac_par_h.loc[transac_par_h.TRA_DATE < predict_start]
    print("TIME : ", datetime.datetime.now())
    
    print('-'*60)
    print('----- Transposition du profil Horaire ----- ')
    print('-'*60)
    transac_par_h['KEY'] = transac_par_h.TERMINAL_ID + transac_par_h.TRA_DATE.apply(datetime2str)
    #Transposition du profil Horaire de la table transac_par_h en colonnes
    profil_tp  = transac_par_h.pivot(index='KEY', columns='TRA_HOUR', values='PROFIL_H')
    
    profil_tp  = Profil_tp.fillna(0)
    profil_tp['KEY'] = profil_tp.index
    
    #ajout de l'info terminaux
    profil_tp  = profil_tp.merge(referentiel,
                      left_on  = "KEY",
                      right_on = "KEY",
                      how      = "left")			
    
    #recuperation de l'info activité journaliere
    profil_tp = profil_tp.merge(transac_par_j[["TERMINAL_ID","TRA_DATE","TRA_DAY"]],
                      left_on  = ["TERMINAL_ID","TRA_DATE"],
                      right_on = ["TERMINAL_ID","TRA_DATE"],
                      how      = "left")					
    print('-'*60)						
    print("Suppression des transactions par jour, du référentiel tp pour libérer de la mémoire")
    print('-'*60)
    del(transac_par_j) #pour libérer de la mémoire
    del(referentiel)
    del(temp)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)
    
    

# =============================================================================
# Sauvegarde des données brutes traitées et des données transposées
# =============================================================================



try:
    os.chdir("E:/Maverick/CockpitGerant/")
    #si mise à jour des données: 
    if write_pkl :
        print('-'*60)
        print("Mise à jour des données:")
        print("Rédaction des pickles Profil tp & Transactions par Heure")
        print(datetime.datetime.now())
        profil_tp.to_pickle("./PICKLE/data_interim/2a_profil_tp.pkl")
        transac_par_h.to_pickle("./PICKLE/data_interim/2b_transac_par_h__tp_processed.pkl")
        print("Fin de rédaction")
        print(datetime.datetime.now())
        print('-'*60)
    
    # =========================================================================
    # Chargement des données pour réentrainer le clustering si besoin
    # =========================================================================
    if type_import != 'datastore' :
        profil_tp = pd.read_pickle("./PICKLE/data_interim/2a_profil_tp.pkl")
        print('-'*60)
        print("Read pickle Profil tp")
        print('-'*60)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)
  


# =============================================================================
# Troisième bloc :Clustering 
# =============================================================================



try:
    print('-'*60)
    print('--- Import ou calcul du Clustering ---- ')
    print()
    print("Debut de Clustering : ", datetime.datetime.now())
    print()
    print('----- Clustering WARD  ----- ')
    print()
    clusters_tp = cluster_tp(profil_tp,n_clust)   
    print()
    print("Fin de Clustering : ", datetime.datetime.now())
    print()
    print("Detail du clustering")
    print(clusters_tp.CLUSTER.describe())
    print()
    print(clusters_tp.head())
    print()
    print("Nombre d/'éléments dans chaque cluster : ")
    print(clusters_tp.CLUSTER.value_counts())
    print('-'*60)
    
    
    # =========================================================================
    # Sauvegarde de la table avec les cluster
    # =========================================================================
    
    #si sauvegarde du modèle
    if write_pkl :
        print('-'*60)
        print("Rédaction du Pickle Clusters tp")
        print(datetime.datetime.now())
        clusters_tp.to_pickle("./PICKLE/data_interim/3_Clusters_tp.pkl")
        print("Fin de rédaction")
        print(datetime.datetime.now())
        print('-'*60)
    
    
    
    # =========================================================================
    # Quatrieme bloc : Préparation données pour prédictions
    # =========================================================================
    print('-'*60)
    print('-------PostProcessing Clust ----- ')
    print('-'*60)
    
    # =========================================================================
    # Chargement des données si besoin
    # =========================================================================
    if type_import != 'datastore' :
        clusters_tp = pd.read_pickle("./PICKLE/data_interim/3_Clusters_tp.pkl")
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)



# =============================================================================
# Traitement des données pour prédictions
# =============================================================================



try:
    # Conservation des clusters avec un poids supperieur à l'equi repartition
    aggregate = { "TRA_DATE" : "nunique"}
    group = ('TERMINAL_ID')
    temp_1 = clusters_tp.groupby(group, as_index=False).agg(aggregate)
    temp_1 = temp_1.rename(columns={"TRA_DATE":"Nb_jrs_tp"})					
    
    

    group = ('TERMINAL_ID', 'CLUSTER')									
    temp_2 = clusters_tp.groupby(group, as_index=False).agg(aggregate)
    temp_2 = temp_2.rename(columns={"TRA_DATE":"Nb_jrs_tp_cluster"})
    
    temp_2 = temp_2.merge(temp_1,
                          right_on = "TERMINAL_ID",
                          left_on = "TERMINAL_ID",
                          how = "left")
    
    temp_2.head()
    
    
    #cluster significatif si la tp est souvent catégorisée dans le cluster
    temp_2['significant'] = (temp_2.Nb_jrs_tp_cluster/temp_2.Nb_jrs_tp >= 1/n_clust)*1
    
    #temp_2.loc[temp_2['significant'] > 1]
    
    clusters_tp.CLUSTER = clusters_tp.CLUSTER.infer_objects()
    
    
    temp_2 = temp_2[["TERMINAL_ID","CLUSTER","significant"]].merge(clusters_tp[["TERMINAL_ID","TRA_DATE","CLUSTER"]],
                                                                  right_on = ["TERMINAL_ID","CLUSTER"],
                                                                  left_on  = ["TERMINAL_ID","CLUSTER"],
                                                                  how = "left")
                          
    Data_pred = clusters_tp.merge(temp_2[["TERMINAL_ID","TRA_DATE","significant"]],
                              right_on = ["TERMINAL_ID","TRA_DATE"],
                              left_on  = ["TERMINAL_ID","TRA_DATE"],
                              how = "left")
    
    Data_pred = Data_pred.loc[Data_pred.significant == 1 ]
    
    Data_pred['WeekDay']  = Data_pred.TRA_DATE.apply(week_day)
    Data_pred['Annee']    = Data_pred.TRA_DATE.apply(get_year)
    Data_pred['Mois']     = Data_pred.TRA_DATE.apply(get_month)
    
    
    Data_pred = Data_pred.merge(transac_par_h[["TERMINAL_ID","TRA_DATE","TRA_HOUR","PROFIL_H","TRA"]].rename(columns={"TRA":"Nb_tra_h"}),
                                left_on = ["TERMINAL_ID","TRA_DATE"],
                                right_on = ["TERMINAL_ID","TRA_DATE"] ,
                                how = 'left')
    
    del(clusters_tp)
    
    del(transac_par_h)
    

    
    #expand lists
    
    # =========================================================================
    # Sauvegarde de la table avec les cluster pour faire les prédictions
    # =========================================================================
    #si sauvegarde du modèle
    if write_pkl :
        print('-'*60)
        print("Rédaction du Pickle Data Pred tp")
        print(datetime.datetime.now())
        data_pred.to_pickle("./PICKLE/data_interim/4_Data_pred_tp.pkl")
        print("Fin de rédaction")
        print(datetime.datetime.now())
        print('-'*60)
        
    
    
    # =========================================================================
    # Cinquième bloc : prédictions
    # =========================================================================
    
    # =========================================================================
    # Si chargement des données
    # =========================================================================
    if type_import != 'datastore' :
        data_pred = pd.read_pickle("./PICKLE/data_interim/4_Data_pred_tp.pkl")
        print('-'*60)
        print("Lecture du Pickle Data Pred tp")
        print('-'*60)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)



#==============================================================================
# Prédictions
#==============================================================================



# =============================================================================
# Lancement de la fonction de prédiction
# =============================================================================



print('-'*60)
print('------- Debut Prediction ------ ')
print('-'*60)
try:
    print('-'*60)
    print("Debut de prediction : ", datetime.datetime.now())
    pred = predict_tp_covid(predict_start, profil_tp, Data_pred, df_b, horizon_last, horizon_mjmm)
    print("Fin de prediction : ", datetime.datetime.now())
    print('-'*60)
    
    
    #==========================================================================
    # Prévisions sur "Nb_pred" jours
    #==========================================================================
    print('-'*60)
    print("Prévisions sur nombre de jours définis")
    print('-'*60)
    
    for i in range(0,Nb_pred -1):
        date = timestamp2str(str2datetime(predict_start) + datetime.timedelta(days=i+1))[0:8]
        pred = pd.concat([pred, predict_tp_covid(date, profil_tp, data_pred, df_b, horizon_last, horizon_mjmm)])
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)


#==============================================================================
# Pred to pickle
#==============================================================================
    

    
print('-'*60)
print("Rédaction du Pickle Mav Predictions")
print(datetime.datetime.now())
pred.to_pickle("./PICKLE/data_interim/5_pred_tp.pkl")
print("Fin de rédaction")
print('-'*60)


#==============================================================================
# Export to csv
#==============================================================================



print('-'*60)    
print("Export du fichier en CSV (Maverick/PROD)")
print('-'*60) 
try:   
    os.getcwd()
    os.chdir("E:/Maverick/CockpitGerant/PREDICTIONS")
    pred.to_csv(""+date_calcul+"_pred.csv")
    print('-'*60)    
    print("Informations sur l'output:")
    print("Nombre de lignes de la table de prédiction:", len(pred.index))
    print("Nombre de tp concernées:",(len(pred.index)/168))
    print('-'*60)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)


#==============================================================================
# Load Datastore
#==============================================================================



print('-'*60)
print(datetime.datetime.now())
print("Lecture du pickle Mav_Prédictions")
print(datetime.datetime.now())
print('-'*60)



try:
    def my_func(date_time):
        return str(date_time.day) + "/" + str(date_time.month) + "/" + str(date_time.year) + " " + str(date_time.hour) + ":" + str(date_time.minute) + ":" +str(date_time.second) + "," + str(date_time.microsecond)
                  
    def write_into_table(conn, DataStore_Table, pd_dataframe):
        cursor = conn.cursor()  
        for record in pd_dataframe.to_records(index = False):
            query = "INSERT INTO " + DataStore_Table + " VALUES " + str(record).replace('nan', 'NULL')
            #print(query)
            cursor.execute(query)
        conn.commit()
        cursor.close()
        return "writen in DataStore"
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)



try:
    print('-'*60)
    print("Connexion au DataStore")
    print(datetime.datetime.now())
    DataStore_D_RW = cx_Oracle.Connection("MABI_DEV/aug#1234@frmssvdorar01-scan.main.glb.corp.local:1521/MABIE")
    print(datetime.datetime.now())  
    print('-'*60)                                    
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)



try:                                      
    print('-'*60)
    print("Application de la fonction date et ajout des intervalles IC-IP")
    print(datetime.datetime.now())
    pred['datecalcul'] = pred['datecalcul'].apply(my_func)
    pred['TimeStamp_Start'] = pred['TimeStamp_Start'].apply(my_func)
    pred['TimeStamp_End'] = pred['TimeStamp_End'].apply(my_func)
    pred = pred[['TERMINAL_ID','TimeStamp_Start','TimeStamp_End','TimeStamp_STR_Start','TimeStamp_STR_End', 'datecalcul', 'prevmjlast','borne_inf','borne_sup']]
    pred['IC_inf'] = np.nan
    pred['IC_sup'] = np.nan
    pred['Alim_type'] = 'modele_1_datetimenow'
    print(datetime.datetime.now())
    print('-'*60)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)



#DO NOT DROP PROD TABLES
#cursor = DataStore_WRITE.cursor()  
#cursor.execute("drop table MABI_DEV.MAV_PRED " + "_doNotWriteInProd" )
#DataStore_WRITE.commit()
#cursor.close()  
#print("prod table dropped")

#creation = ("	CREATE TABLE MAV_PRED    "
#"    (    STATION_ID VARCHAR(50),    "
#"		TimeStamp_Start	TIMESTAMP,	"
#"		TimeStamp_End	TIMESTAMP,	"
#"		TimeStamp_STR_Start	VARCHAR(20),	"
#"		TimeStamp_STR_End	VARCHAR(20),		"
#"		COMPUTATION_DATE	TIMESTAMP, 	"
#"      PREDICTED_NB 	NUMERIC(20,10),	"
#"      IP_inf 	NUMERIC(20,10),	"
#"      IP_sup 	NUMERIC(20,10),	"
#"      IC_inf 	NUMERIC(20,10),	"
#"      IC_sup 	NUMERIC(20,10),   "
#"      Alim_type VARCHAR(50))	").replace("\t", " ")

#DO NOT CREATE PROD TABLES
#cursor = DataStore_WRITE.cursor()  
#cursor.execute(creation + "_doNotWriteInProd")
#DataStore_WRITE.commit()
#cursor.close()  
#print("prod table created")


try:
    print('-'*60)
    print("Ecriture de la table dans le DataStore : MABI_DEV.MAV_PRED")
    print(datetime.datetime.now())
    write_into_table(DataStore_WRITE,'MABI_DEV.MAV_PRED', pred)
    print("Fin d'écriture")
    print(datetime.datetime.now())
    print('-'*60)
except:
    print("Exception in user code:")
    print('-'*60)
    traceback.print_exc(file=sys.stdout)
    print('-'*60)
        
#del DataStore_WRITE



# =============================================================================
# Si sauvegarde des résultats 
# =============================================================================



print('-'*60)
print("Visualisation de la table:", pred.head())
print('-'*60)



# =============================================================================
# Fin
# =============================================================================



print('-'*60)
print("Process end")
print(datetime.datetime.now())
print('-'*60)
sys.stdout.close()
sys.stdout = xtemp





