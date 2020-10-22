# -*- coding: utf-8 -*-
"""
Created on Mon Oct 12 17:06:58 2020

@author: J0545269
"""

import datetime
import numpy as np
import pandas as pd
from tqdm import tqdm
import cx_Oracle
from sklearn.cluster import AgglomerativeClustering
from fonctions_date import * 


print('--- Import des Macro Paramètres et fonction d\'import---- ')
paths = {"transac_heure" : "./data_interim/1_transac_par_h_tp.pkl",
         "profil_station" : "./data_interim/profil_tp.pkl", 
         "clusters_stations" : "./data_interim/clusters_tp.pkl"  }

DataStore_P_RO = cx_Oracle.Connection("MABI_READER/datalake2015@frmssvdorap02-scan.main.glb.corp.local:1521/MABIP")


#extract_start = datetime2singlestr(datetime.datetime.now() + datetime.timedelta(days=-100))
#predict_start = datetime2singlestr(datetime.datetime.now() + datetime.timedelta(days=+1))
Nb_pred = 7
n_clust = 15 
nb_jrs_min = n_clust +1  
horizon_mjmm = 30
horizon_last = 60


def import_data(connection, query): # returns a list cf - procédure d'installation
    list_imported = []
    cursor = connection.cursor()  
    try:
        cursor.execute(query)
    except cx_Oracle.DatabaseError as exc:
        error, = exc.args
        print("Oracle-Error-Code:", error.code)
        print("Oracle-Error-Message:", error.message)
    
    list_imported = cursor.fetchall() 
    
        # Transform the list in a Python Pandas dataframe
    table_imported = pd.DataFrame(list_imported)

    try:
        table_imported.columns = [desc[0] for desc in cursor.description]
    except ValueError: # If the dataframe is empty, there is an Error
        pass
    
    # Close the cursor
    cursor.close()
    
    return table_imported 		


def extract_tp(conn_DS, extr_start, pred_start): 
    query = (" SELECT "
             " t.TERMINAL_ID,"
             " t.TERMINAL_TYPE_NAME as TERMINAL_TYPE, "
             " t.TERMINAL_DATE_STR, "
             " SUBSTR(t.TERMINAL_HOUR,1 ,2) as TRA_HOUR, "
             " count(1) as TRA, "
             " sum(t.AMOUNT) as CA, "
             " sum(t.AMOUNT_ALL_FUELS) as CA_FUELS, "
             " sum(t.AMOUNT_EX_FUELS) as CA_HFUELS "
             " FROM "
             " MABI_DBA.VATI_TRANSACTION t "
             " WHERE "
             " t.AFFILIATE = 'FR' "
             " AND RESPONSE_CODE_NAME in ('Authorised','Approved') "
             " AND TRANSACTION_TYPE_NAME in ('Purchase','Completion') "
             " AND (t.TERMINAL_DATE_STR >= '" + extr_start + "' ) "
             " AND (t.TERMINAL_DATE_STR <= '" + pred_start + "' ) "
             " GROUP BY t.TERMINAL_ID, t.TERMINAL_TYPE_NAME, t.TERMINAL_DATE_STR, SUBSTR(t.TERMINAL_HOUR,1 ,2) ")
    output = import_data(conn_DS, query)
    output['TRA_DATE'] = output.TERMINAL_DATE_STR.apply(str2datetime)
    return output



def load_data(type_import : str , path = paths["transac_heure"],extr_start = datetime2singlestr(datetime.datetime.now() + datetime.timedelta(days=-100)),pred_start = datetime2singlestr(datetime.datetime.now() + datetime.timedelta(days=+1))):
    
    print("mode de chargement : " , type_import)
    dataframe = pd.DataFrame()
    if type_import == 'datastore':
       print('... requête en cours' )
       print("Début de requête : ", datetime.datetime.now())
       dataframe = extract_tp(DataStore_P_RO,extr_start, pred_start)
    else :
       print('... chargement en cours')
       dataframe = pd.read_pickle(path)
    print('... chargement ou calcul terminé')
    print("fin du chargement ou calcul : ", datetime.datetime.now())
    return dataframe	


def cluster_tp(profil_df, nb_classe):
    linkage  = "ward"
    n_clust = nb_classe #15
    clustering = AgglomerativeClustering(linkage=linkage, n_clusters=n_clust)
    output = profil_df[["TERMINAL_ID","TRA_DATE","TRA_DAY"]].loc[profil_df.TERMINAL_ID == ""]
    output['CLUSTER'] = ""
    tp_list = profil_df.TERMINAL_ID.drop_duplicates()
    

    for tp in tqdm(tp_list) :         #tqdm pour barre de chargement
        temp = profil_df.loc[profil_df.TERMINAL_ID == tp]
        X_red = temp[["00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"]]
        try :
            clustering.fit(X_red) 
            temp = temp[["TERMINAL_ID","TRA_DATE","TRA_DAY"]]
            temp['CLUSTER'] = clustering.labels_
            output = output.append(temp)
        except :
            temp = temp[["TERMINAL_ID","TRA_DATE","TRA_DAY"]]
            temp['CLUSTER'] = "-1"
            output = output.append(temp)
        
    print("Fin de Clustering : ", datetime.datetime.now())
    
    return output




def predict_tp(date_pred, data_profil, data_pred, ref_stations_heures, hrz_last = 30, hrz_mjmm = 30):
    
     # Caclul des elements de dates
    predict_weekday = str2datetime(date_pred).isoweekday()
    predict_month =  str2datetime(date_pred).month
    predict_year =  str2datetime(date_pred).year
    per_glissant = hrz_mjmm
    window_end = str2datetime(date_pred)
    window_start = window_end + datetime.timedelta(days=-per_glissant)
    
    # Calcul des coefficients correctifs selon les periodes
    coeff = data_profil.copy()
    coeff['window_end'] = window_end
    coeff['window_start'] = window_start
    coeff['Periode'] = 1*( ( (coeff.TRA_DATE.apply(get_year)*10000 + coeff.window_start.apply(get_month)*100 + coeff.window_start.apply(get_day)).apply(int2datetime) <= coeff.TRA_DATE) & \
                           (coeff.TRA_DATE <  ((coeff.TRA_DATE.apply(get_month) == 12)*10000 + coeff.TRA_DATE.apply(get_year)*10000 + coeff.window_end.apply(get_month)*100 + coeff.window_end.apply(get_day)).apply(int2datetime) ) )
    coeff.Periode = 2*( (coeff.TRA_DATE.apply(get_year) == coeff.window_start.apply(get_year)) & (coeff.Periode==1) )
    coeff = coeff.loc[coeff.Periode >= 1]
    coeff["Periode_group"] = "P0" + coeff.Periode.apply(str)


    aggregate = { "TRA_DATE" : "nunique",
                 "TRA_DAY" : "sum"}
    group = ['TERMINAL_ID','Periode_group']
    coeff_sum = coeff.groupby(group, as_index=False).agg(aggregate)
    coeff_sum['volume_mois_co'] = per_glissant * coeff_sum.TRA_DAY/coeff_sum.TRA_DATE


    coeff_sum2 = coeff_sum.pivot(index='TERMINAL_ID', columns='Periode_group', values='volume_mois_co')
    coeff_sum2 = coeff_sum2.fillna(0)
#     coeff_sum2['TERMINAL_ID'] = coeff_sum2.index
    if len(coeff.Periode.drop_duplicates())>1:
        coeff_sum2['correctif'] = coeff_sum2.P01/coeff_sum2.P02
    else:
        coeff_sum2['correctif'] = 1
    
    # Calcul des pred mjmm
    mjmm = data_pred.loc[(data_pred.WeekDay == predict_weekday) & ( data_pred.Mois== predict_month) ].copy()
    mjmm['Periode'] = 1*(predict_year != mjmm.Annee) + 2 * (predict_year == mjmm.Annee)
    aggregate = { "TRA_DATE" : "nunique"}
    group = ['TERMINAL_ID']
    nb_njmm = mjmm.groupby(group, as_index=False).agg(aggregate)
    nb_njmm = nb_njmm.rename(columns={"TRA_DATE" : "nb_jrs"})

    mjmm = mjmm.merge(nb_njmm[["TERMINAL_ID","nb_jrs"]],
                      left_on = "TERMINAL_ID",
                      right_on = "TERMINAL_ID")

    mjmm = mjmm.merge(coeff_sum2[["TERMINAL_ID","correctif"]],
                      left_on = "TERMINAL_ID",
                      right_on = "TERMINAL_ID")

    mjmm.correctif = mjmm.correctif*(mjmm.Periode != 2) + 1*(mjmm.Periode == 2)
    mjmm['Nb_tra_h_cor'] = mjmm.TRA_DAY*mjmm.correctif

    aggregate = { "Nb_tra_h_cor" : "sum",
                 "Nb_tra_h" : "sum",
                 "PROFIL_H" : "sum"}
    group = ['TERMINAL_ID', "TRA_HOUR", "nb_jrs"]
    prev_mjmm = mjmm.groupby(group, as_index=False).agg(aggregate)

    aggregate = { "Nb_tra_h_cor" : "count"}
    group = ['TERMINAL_ID',"TRA_DATE"]
    mjmm_sel = mjmm.groupby(group, as_index=False).agg(aggregate)
    del mjmm_sel['Nb_tra_h_cor']
    
    prev_mjmm = prev_mjmm.merge(mjmm_sel,
                                left_on = "TERMINAL_ID",
                                right_on = 'TERMINAL_ID' )

    prev_mjmm['profil_prevmjmm'] = prev_mjmm.PROFIL_H / prev_mjmm.nb_jrs
    prev_mjmm['prevmjmm'] =  prev_mjmm.Nb_tra_h / prev_mjmm.nb_jrs
    prev_mjmm['prevmjmm_cor'] = prev_mjmm.Nb_tra_h_cor / prev_mjmm.nb_jrs
    prev_mjmm = prev_mjmm[["TERMINAL_ID","TRA_HOUR","profil_prevmjmm","prevmjmm","prevmjmm_cor"]].drop_duplicates()


    # Caclul des pred mjlast
    mjlast = data_pred.loc[(data_pred.WeekDay == predict_weekday) & ( data_pred.TRA_DATE > str2datetime(date_pred) - datetime.timedelta(days=hrz_last)) ].copy()

    aggregate = { "TRA_DATE" : "nunique"}
    group = ['TERMINAL_ID']
    nb_mjlast = mjlast.groupby(group, as_index=False).agg(aggregate)
    nb_mjlast = nb_mjlast.rename(columns={"TRA_DATE" : "nb_jrs"})

    mjlast = mjlast.merge(nb_mjlast[["TERMINAL_ID","nb_jrs"]],
                          left_on = "TERMINAL_ID",
                          right_on = "TERMINAL_ID")

    aggregate = { "Nb_tra_h" : "sum",
                 "PROFIL_H" : "sum"}
    group = ['TERMINAL_ID', "TRA_HOUR",'nb_jrs']
    prev_mjlast = mjlast.groupby(group, as_index=False).agg(aggregate)
    
    aggregate = { "Nb_tra_h" : "count"}
    group = ['TERMINAL_ID',"TRA_DATE"]
    mjlast_sel = mjlast.groupby(group, as_index=False).agg(aggregate)
    del mjlast_sel['Nb_tra_h']

    prev_mjlast = prev_mjlast.merge(mjlast_sel,
                                    left_on = "TERMINAL_ID",
                                    right_on = 'TERMINAL_ID' )

    prev_mjlast['profil_prevmjlast'] = prev_mjlast.PROFIL_H / prev_mjlast.nb_jrs
    prev_mjlast['prevmjlast'] =  prev_mjlast.Nb_tra_h / prev_mjlast.nb_jrs
    prev_mjlast = prev_mjlast[["TERMINAL_ID","TRA_HOUR","profil_prevmjlast","prevmjlast"]].drop_duplicates()


    aggregate = { "Nb_tra_h" : ["max","min","count"]}
    group = ['TERMINAL_ID', "TRA_HOUR"]
    TRAVAIL_IC  = mjlast.groupby(group, as_index=False).agg(aggregate)
    TRAVAIL_IC['borne_inf'] = TRAVAIL_IC['Nb_tra_h']['min']
    TRAVAIL_IC['borne_sup'] = TRAVAIL_IC['Nb_tra_h']['max']
    TRAVAIL_IC = TRAVAIL_IC[["TERMINAL_ID","TRA_HOUR","borne_inf","borne_sup"]]
    TRAVAIL_IC.columns = ['TERMINAL_ID', 'TRA_HOUR','borne_inf','borne_sup']
    
    ma_prev = prev_mjlast.merge(prev_mjmm[["TERMINAL_ID","TRA_HOUR","profil_prevmjmm","prevmjmm","prevmjmm_cor"]],
                                left_on = ["TERMINAL_ID","TRA_HOUR"] ,
                                right_on =["TERMINAL_ID","TRA_HOUR"],
                                how = 'left')
    
    ma_prev = ma_prev.merge(TRAVAIL_IC,
                            right_on = ['TERMINAL_ID', "TRA_HOUR"],
                            left_on  = ['TERMINAL_ID', "TRA_HOUR"],
                            how = 'left')
    ma_prev = ma_prev.fillna(0)

    ma_prev.borne_inf = ma_prev.borne_inf *( (ma_prev.borne_inf < ma_prev.prevmjlast) | ( ( ma_prev.TRA_HOUR.apply(int) >8) & (ma_prev.TRA_HOUR.apply(int) <22) & ( ma_prev.prevmjlast > 3 )) )
    ma_prev.borne_sup = ma_prev.borne_sup * (ma_prev.borne_sup > ma_prev.prevmjlast) + ma_prev.prevmjlast * (ma_prev.borne_sup <= ma_prev.prevmjlast)

    
    
    ma_prev = ma_prev.merge(ref_stations_heures,
                            right_on = ['TERMINAL_ID', "TRA_HOUR"],
                            left_on  = ['TERMINAL_ID', "TRA_HOUR"],
                            how = 'right')
    ma_prev['dateprev'] = date_pred
    ma_prev['datecalcul'] = datetime.datetime.now()   
    ma_prev = ma_prev.fillna(0)

    ma_prev["TimeStamp_STR_Start"] = ma_prev["dateprev"].map(str) + ma_prev["TRA_HOUR"].map(str) + "00"
    ma_prev["TimeStamp_Start"] = ma_prev.TimeStamp_STR_Start.apply(str2timestamp)
    ma_prev["TimeStamp_End"] = ma_prev.TimeStamp_Start + datetime.timedelta(hours=1)  
    ma_prev["TimeStamp_STR_End"] = ma_prev.TimeStamp_End.apply(timestamp2str)
    
    return ma_prev



def predict_station_covid(date_pred, data_profil, data_pred, ref_stations_heures, hrz_last = 30, hrz_mjmm = 30):
    
    # Caclul des elements de dates
    predict_weekday = str2datetime(date_pred).isoweekday()
    predict_month =  str2datetime(date_pred).month
    predict_year =  str2datetime(date_pred).year
    per_glissant = hrz_mjmm

    
    # Calcul des coefficients correctifs selon les periodes
    coeff = data_profil.copy()
    coeff["Periode_group"] = "P0" 


    aggregate = { "TRA_DATE" : "nunique",
                 "TRA_DAY" : "sum"}
    group = ['STATION','Periode_group']
    coeff_sum = coeff.groupby(group, as_index=False).agg(aggregate)
    coeff_sum['volume_mois_co'] = 120 * coeff_sum.TRA_DAY/coeff_sum.TRA_DATE
    coeff_sum2 = coeff_sum.pivot(index='STATION', columns='Periode_group', values='volume_mois_co')
    coeff_sum2 = coeff_sum2.fillna(0)
    coeff_sum2['STATION'] = coeff_sum2.index
    coeff_sum2['correctif'] = 1
    
    # Calcul des pred mjmm
    mjmm = data_pred.loc[(data_pred.WeekDay == predict_weekday) & ( data_pred.Mois== predict_month) ].copy()
    mjmm['Periode'] = 1*(predict_year != mjmm.Annee) + 2 * (predict_year == mjmm.Annee)
    aggregate = { "TRA_DATE" : "nunique"}
    group = ['STATION']
    nb_njmm = mjmm.groupby(group, as_index=False).agg(aggregate)
    nb_njmm = nb_njmm.rename(columns={"TRA_DATE" : "nb_jrs"})

    mjmm = mjmm.merge(nb_njmm[["STATION","nb_jrs"]],
                      left_on = "STATION",
                      right_on = "STATION")

    mjmm = mjmm.merge(coeff_sum2[["STATION","correctif"]],
                      left_on = "STATION",
                      right_on = "STATION")

    mjmm.correctif = mjmm.correctif*(mjmm.Periode != 2) + 1*(mjmm.Periode == 2)
    mjmm['Nb_tra_h_cor'] = mjmm.TRA_DAY*mjmm.correctif

    aggregate = { "Nb_tra_h_cor" : "sum",
                 "Nb_tra_h" : "sum",
                 "PROFIL_H" : "sum"}
    group = ['STATION', "TRA_HOUR", "nb_jrs"]
    prev_mjmm = mjmm.groupby(group, as_index=False).agg(aggregate)

    aggregate = { "Nb_tra_h_cor" : "count"}
    group = ['STATION',"TRA_DATE"]
    mjmm_sel = mjmm.groupby(group, as_index=False).agg(aggregate)
    del mjmm_sel['Nb_tra_h_cor']
    
    prev_mjmm = prev_mjmm.merge(mjmm_sel,
                                left_on = "STATION",
                                right_on = 'STATION' )

    prev_mjmm['profil_prevmjmm'] = prev_mjmm.PROFIL_H / prev_mjmm.nb_jrs
    prev_mjmm['prevmjmm'] =  prev_mjmm.Nb_tra_h / prev_mjmm.nb_jrs
    prev_mjmm['prevmjmm_cor'] = prev_mjmm.Nb_tra_h_cor / prev_mjmm.nb_jrs
    prev_mjmm = prev_mjmm[["STATION","TRA_HOUR","profil_prevmjmm","prevmjmm","prevmjmm_cor"]].drop_duplicates()


    # Caclul des pred mjlast
    mjlast = data_pred.loc[(data_pred.WeekDay == predict_weekday) ].copy()

    aggregate = { "TRA_DATE" : "nunique"}
    group = ['STATION']
    nb_mjlast = mjlast.groupby(group, as_index=False).agg(aggregate)
    nb_mjlast = nb_mjlast.rename(columns={"TRA_DATE" : "nb_jrs"})

    mjlast = mjlast.merge(nb_mjlast[["STATION","nb_jrs"]],
                          left_on = "STATION",
                          right_on = "STATION")

    aggregate = { "Nb_tra_h" : "sum",
                 "PROFIL_H" : "sum"}
    group = ['STATION', "TRA_HOUR",'nb_jrs']
    prev_mjlast = mjlast.groupby(group, as_index=False).agg(aggregate)
    
    aggregate = { "Nb_tra_h" : "count"}
    group = ['STATION',"TRA_DATE"]
    mjlast_sel = mjlast.groupby(group, as_index=False).agg(aggregate)
    del mjlast_sel['Nb_tra_h']

    prev_mjlast = prev_mjlast.merge(mjlast_sel,
                                    left_on = "STATION",
                                    right_on = 'STATION' )

    prev_mjlast['profil_prevmjlast'] = prev_mjlast.PROFIL_H / prev_mjlast.nb_jrs
    prev_mjlast['prevmjlast'] =  prev_mjlast.Nb_tra_h / prev_mjlast.nb_jrs
    prev_mjlast = prev_mjlast[["STATION","TRA_HOUR","profil_prevmjlast","prevmjlast"]].drop_duplicates()


    aggregate = { "Nb_tra_h" : ["max","min","count"]}
    group = ['STATION', "TRA_HOUR"]
    TRAVAIL_IC  = mjlast.groupby(group, as_index=False).agg(aggregate)
    TRAVAIL_IC['borne_inf'] = TRAVAIL_IC['Nb_tra_h']['min']
    TRAVAIL_IC['borne_sup'] = TRAVAIL_IC['Nb_tra_h']['max']
    TRAVAIL_IC = TRAVAIL_IC[["STATION","TRA_HOUR","borne_inf","borne_sup"]]
    TRAVAIL_IC.columns = ['STATION', 'TRA_HOUR','borne_inf','borne_sup']
    
    ma_prev = prev_mjlast.merge(prev_mjmm[["STATION","TRA_HOUR","profil_prevmjmm","prevmjmm","prevmjmm_cor"]],
                                left_on = ["STATION","TRA_HOUR"] ,
                                right_on =["STATION","TRA_HOUR"],
                                how = 'left')
    
    ma_prev = ma_prev.merge(TRAVAIL_IC,
                            right_on = ['STATION', "TRA_HOUR"],
                            left_on  = ['STATION', "TRA_HOUR"],
                            how = 'left')
    ma_prev = ma_prev.fillna(0)

    ma_prev.borne_inf = ma_prev.borne_inf *( (ma_prev.borne_inf < ma_prev.prevmjlast) | ( ( ma_prev.TRA_HOUR.apply(int) >8) & (ma_prev.TRA_HOUR.apply(int) <22) & ( ma_prev.prevmjlast > 3 )) )
    ma_prev.borne_sup = ma_prev.borne_sup * (ma_prev.borne_sup > ma_prev.prevmjlast) + ma_prev.prevmjlast * (ma_prev.borne_sup <= ma_prev.prevmjlast)

    
    
    ma_prev = ma_prev.merge(ref_stations_heures,
                            right_on = ['STATION', "TRA_HOUR"],
                            left_on  = ['STATION', "TRA_HOUR"],
                            how = 'right')
    ma_prev['dateprev'] = date_pred
    ma_prev['datecalcul'] = datetime.datetime.now()   
    ma_prev = ma_prev.fillna(0)

    ma_prev["TimeStamp_STR_Start"] = ma_prev["dateprev"].map(str) + ma_prev["TRA_HOUR"].map(str) + "00"
    ma_prev["TimeStamp_Start"] = ma_prev.TimeStamp_STR_Start.apply(str2timestamp)
    ma_prev["TimeStamp_End"] = ma_prev.TimeStamp_Start + datetime.timedelta(hours=1)  
    ma_prev["TimeStamp_STR_End"] = ma_prev.TimeStamp_End.apply(timestamp2str)
    
    return ma_prev
