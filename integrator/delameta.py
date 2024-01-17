import index
import pandas as pd
import logging
import pandas as pd
from datetime import date
import logging
import psycopg2
import config
import mysql.connector
import sys
import integrator
        
def data_delameta():
    cnxdb_data = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        cursor_data = cnxdb_data.cursor()
        query_delameta = "SELECT \
            '0' AS asal_gerbang_id,\
            idgerbang,\
            gardu,\
            tanggal_siklus,\
            shift,\
            periode,\
            resi,\
            gol,\
            no_card,\
            jenis_transaksi,\
            jenis_transaksi,\
            waktu_transaksi,\
            idkspt,\
            idpultol,\
            etoll_hash,\
            tarif,\
            saldo,\
            jenis_dinas\
            from dbsharejmto."+str(config.namaGerbang)+".vtbltransaksi_open where jenis_transaksi NOT IN ('91') and waktu_transaksi >= NOW() - interval '"+str(config.intervalQuery)+" HOURS' ORDER BY waktu_transaksi asc "
        # print ("Query : " + str(query_delameta))
        cursor_data.execute(query_delameta)
        data = pd.DataFrame(cursor_data.fetchall())
        rc = cursor_data.rowcount
        logging.info("JUMLAH DATA REALTIME : " + str(rc))
        dataDelameta = data.values.tolist()
        # print (dataDelameta)
        cnxdb_data.close()
        return dataDelameta

    except Exception as e:
        logging.error("ERROR Call Data Delameta " + str(e))
        
def compare_data_delameta():
    cnxdb_compare = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        cursor_compare = cnxdb_compare.cursor()
        query_delameta = " SELECT \
        tanggal_siklus, \
        idgerbang, \
        gardu, \
        shift, \
        gol, \
        count(DISTINCT waktu_transaksi) AS dataIntegrator \
        FROM dbsharejmto."+str(config.namaGerbang)+ ".vtbltransaksi_open WHERE jenis_transaksi NOT IN ('91') and tanggal_siklus BETWEEN now() - INTERVAL '2 DAY' AND now()  \
        GROUP BY idgerbang, tanggal_siklus,gardu, shift, gol \
        ORDER BY tanggal_siklus, gardu, shift "
        cursor_compare.execute(query_delameta)
        data = pd.DataFrame(cursor_compare.fetchall())
        rc = cursor_compare.rowcount
        logging.info("JUMLAH DATA Compare : " + str(rc))
        compareDelameta = data.values.tolist()
        # print (compareDelameta)
        cnxdb_compare.close()
        return compareDelameta
    
    except Exception as e:
        logging.error("ERROR Call Data Delameta" + str(e))
        
def datasusulandelameta():
    
    results = index.checkSelisihdata()
    
    cnxdb_susulan = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        
        cursor_susulan = cnxdb_susulan.cursor()
        
        for x in results:
            tgl_lap = str(x[0])
            gardu_id = str(x[1])
            shift = str(x[2])
            gol = str(x[3])
            # print ('tanggal print :' + str(tgl_lap))

            query_delameta = "SELECT \
            '0' AS asal_gerbang_id,\
            idgerbang,\
            gardu,\
            tanggal_siklus,\
            shift,\
            periode,\
            resi,\
            gol,\
            no_card,\
            jenis_transaksi,\
            jenis_transaksi,\
            waktu_transaksi,\
            idkspt,\
            idpultol,\
            etoll_hash,\
            tarif,\
            saldo,\
            jenis_dinas\
            from dbsharejmto."+str(config.namaGerbang)+".vtbltransaksi_open where jenis_transaksi NOT IN ('91') and \
            tanggal_siklus = '" + tgl_lap + "' and " \
            " gardu = '" + gardu_id + "' and " \
            " shift = '" + shift + "' and " \
            " gol = '" + gol +"'"
            # print (query_delameta)
            cursor_susulan.execute(query_delameta)
            data = pd.DataFrame(cursor_susulan.fetchall())
            # print (data)
            rc = cursor_susulan.rowcount
            logging.info("JUMLAH DATA SUSULAN : " + str(rc) + " | " + str(tgl_lap) + ' Gardu:' + str(gardu_id) + "- Shift:" + str(shift) + "- Gol:" + str(gol))
            datasusulandelameta = data.values.tolist()
            InsertresultData(datasusulandelameta)
            
        
    except Exception as e:
        logging.error("ERROR CALL Data Database Delameta :" + e)
        
def data_delameta_entrance():
    cnxdb_data = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        cursor_data = cnxdb_data.cursor()
        query_delameta = "SELECT \
            '0' AS asal_gerbang_id,\
            idgerbang,\
            gardu,\
            tanggal_siklus,\
            shift,\
            periode,\
            resi,\
            gol,\
            no_card,\
            jenis_transaksi,\
            jenis_transaksi,\
            waktu_transaksi,\
            idkspt,\
            idpultol,\
            '' AS etoll_hash,\
            '0' AS tarif,\
            '0' AS saldo,\
            jenis_dinas\
            from dbsharejmto."+str(config.namaGerbang)+".vtbltransaksi_entry where jenis_transaksi NOT IN ('91') and waktu_transaksi >= NOW() - interval '"+str(config.intervalQuery)+" HOURS' ORDER BY waktu_transaksi asc"
        # print ("Query : " + str(query_delameta))
        cursor_data.execute(query_delameta)
        data = pd.DataFrame(cursor_data.fetchall())
        rc = cursor_data.rowcount
        logging.info("JUMLAH DATA REALTIME ENTRANCE : " + str(rc))
        data_delameta_entrance = data.values.tolist()
        # print (dataDelameta)
        cnxdb_data.close()
        return data_delameta_entrance

    except Exception as e:
        logging.error("ERROR Data Susulan Delameta: " + str(e))    

def compare_data_delameta_entrance():
    cnxdb_compare = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        cursor_compare = cnxdb_compare.cursor()
        query_delameta = " SELECT \
        tanggal_siklus, \
        idgerbang, \
        gardu, \
        shift, \
        gol, \
        count(DISTINCT waktu_transaksi) AS dataIntegrator \
        FROM dbsharejmto." +config.namaGerbang+ ".vtbltransaksi_entry WHERE jenis_transaksi NOT IN ('91') and tanggal_siklus BETWEEN now() - INTERVAL '2 DAY' AND now()  \
        GROUP BY idgerbang, tanggal_siklus,gardu, shift, gol \
        ORDER BY tanggal_siklus, gardu, shift "
        cursor_compare.execute(query_delameta)
        data = pd.DataFrame(cursor_compare.fetchall())
        rc = cursor_compare.rowcount
        logging.info("JUMLAH DATA Compare : " + str(rc))
        compare_data_delameta_entrance = data.values.tolist()
        # print (compareDelameta)
        cnxdb_compare.close()
        return compare_data_delameta_entrance
    
    except Exception as e:
        logging.error("ERROR Call Data Delameta" + str(e))

def datasusulandelameta_entrance():
    
    results = index.checkSelisihdata()
    
    cnxdb_susulan = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        
        cursor_susulan = cnxdb_susulan.cursor()
        
        for x in results:
            tgl_lap = str(x[0])
            gardu_id = str(x[1])
            shift = str(x[2])
            gol = str(x[3])
            # print ('tanggal print :' + str(tgl_lap))

            query_delameta = "SELECT \
            '0' AS asal_gerbang_id,\
            idgerbang,\
            gardu,\
            tanggal_siklus,\
            shift,\
            periode,\
            resi,\
            gol,\
            no_card,\
            jenis_transaksi,\
            jenis_transaksi,\
            waktu_transaksi,\
            idkspt,\
            idpultol,\
            '0' AS etoll_hash,\
            '0' AS tarif,\
            '0' AS saldo,\
            jenis_dinas\
            from dbsharejmto."+str(config.namaGerbang)+".vtbltransaksi_entry where jenis_transaksi NOT IN ('91') and \
            tanggal_siklus = '" + tgl_lap + "' and " \
            " gardu = '" + gardu_id + "' and " \
            " shift = '" + shift + "' and " \
            " gol = '" + gol +"'"
            # print (query_delameta)
            cursor_susulan.execute(query_delameta)
            data = pd.DataFrame(cursor_susulan.fetchall())
            # print (data)
            rc = cursor_susulan.rowcount
            logging.info("JUMLAH DATA SUSULAN : " + str(rc) + " | " + str(tgl_lap) + ' Gardu:' + str(gardu_id) + "- Shift:" + str(shift) + "- Gol:" + str(gol))
            datasusulandelameta_entrance = data.values.tolist()
            InsertresultData(datasusulandelameta_entrance)
            
        
    except Exception as e:
        logging.error("ERROR Data Susulan Database Delameta :" + e)

def data_delameta_exit():
    cnxdb_data = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        cursor_data = cnxdb_data.cursor()
        query_delameta = "SELECT \
            gerbang_masuk,\
            gerbang_keluar,\
            gardu,\
            tanggal_siklus,\
            shift,\
            periode,\
            resi,\
            gol,\
            no_card,\
            jenis_transaksi,\
            jenis_transaksi,\
            waktu_trans_exit,\
            idkspt,\
            idpultol,\
            etoll_hash,\
            tarif,\
            saldo,\
            jenis_dinas\
            from dbsharejmto."+str(config.namaGerbang)+".vtbltransaksi_exit where jenis_transaksi NOT IN ('91') and waktu_trans_exit >= NOW() - interval '"+str(config.intervalQuery)+" HOURS' ORDER BY waktu_trans_exit asc"
        # print ("Query : " + str(query_delameta))
        cursor_data.execute(query_delameta)
        data = pd.DataFrame(cursor_data.fetchall())
        rc = cursor_data.rowcount
        logging.info("JUMLAH DATA REALTIME EXIT : " + str(rc))
        data_delameta_exit = data.values.tolist()
        # print (dataDelameta)
        cnxdb_data.close()
        return data_delameta_exit

    except Exception as e:
        logging.error("ERROR Call Data Delameta Exit : " + str(e))    
    
def compare_data_delameta_exit():
    cnxdb_compare = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        cursor_compare = cnxdb_compare.cursor()
        query_delameta = " SELECT \
        tanggal_siklus, \
        gerbang_keluar, \
        gardu, \
        shift, \
        gol, \
        count(DISTINCT waktu_trans_exit) AS dataIntegrator \
        FROM dbsharejmto." +config.namaGerbang+ ".vtbltransaksi_exit WHERE jenis_transaksi NOT IN ('91') and tanggal_siklus BETWEEN now() - INTERVAL '2 DAY' AND now()\
        GROUP BY gerbang_keluar, tanggal_siklus,gardu, shift, gol \
        ORDER BY tanggal_siklus, gardu, shift "
        cursor_compare.execute(query_delameta)
        data = pd.DataFrame(cursor_compare.fetchall())
        rc = cursor_compare.rowcount
        logging.info("JUMLAH DATA Compare : " + str(rc))
        compare_data_delameta_exit = data.values.tolist()
        # print (compare_data_delameta_exit)
        cnxdb_compare.close()
        return compare_data_delameta_exit
    
    except Exception as e:
        logging.error("ERROR Call Data Delameta" + str(e))    
        
def datasusulandelameta_exit():
    
    results = index.checkSelisihdata()
    
    cnxdb_susulan = psycopg2.connect(
            host=config.srcHost,
            user=config.srcUser,
            password=config.srcPass,
            database=config.srcDb,
            port=config.srcPort)
    try:
        
        cursor_susulan = cnxdb_susulan.cursor()
        
        for x in results:
            tgl_lap = str(x[0])
            gardu_id = str(x[1])
            shift = str(x[2])
            gol = str(x[3])
            # print ('tanggal print :' + str(tgl_lap))

            query_delameta = "SELECT \
            gerbang_masuk,\
            gerbang_keluar,\
            gardu,\
            tanggal_siklus,\
            shift,\
            periode,\
            resi,\
            gol,\
            no_card,\
            jenis_transaksi,\
            jenis_transaksi,\
            waktu_trans_exit,\
            idkspt,\
            idpultol,\
            etoll_hash,\
            tarif,\
            saldo,\
            jenis_dinas\
            from dbsharejmto."+str(config.namaGerbang)+".vtbltransaksi_exit where jenis_transaksi NOT IN ('91') and \
            tanggal_siklus = '" + tgl_lap + "' and " \
            " gardu = '" + gardu_id + "' and " \
            " shift = '" + shift + "' and " \
            " gol = '" + gol +"'"
            # print (query_delameta)
            cursor_susulan.execute(query_delameta)
            data = pd.DataFrame(cursor_susulan.fetchall())
            # print (data)
            rc = cursor_susulan.rowcount
            logging.info("JUMLAH DATA SUSULAN : " + str(rc) + " | " + str(tgl_lap) + ' Gardu:' + str(gardu_id) + "- Shift:" + str(shift) + "- Gol:" + str(gol))
            datasusulandelameta_exit = data.values.tolist()
            InsertresultData(datasusulandelameta_exit)
            
        
    except Exception as e:
        logging.error("ERROR CALL Data Database Delameta Exit :" + e)  
        
def InsertresultData(resultData):
    try:       
        if config.configMode == '60' or '61' or '62' or '63': #DELAMETA 
            for x in resultData:
                asal_gerbang_id = str(x[0])
                gerbang_id = str(x[1])
                gardu_id = str(x[2])
                tgl_lap = str(x[3])
                shift = str(x[4])
                perioda = str(x[5]) 
                no_resi = str(x[6])
                gol_sah = str(x[7])
                etoll_id = str(x[8])
                jenis_dinas = str(x[17])
                metoda_bayar_sah = str(x[9])
                jenis_notran = str(x[10])
                if metoda_bayar_sah == '11' or metoda_bayar_sah == '12': 
                    metoda_bayar_sah = "21"
                    jenis_notran = '1'
                elif metoda_bayar_sah == '14' or metoda_bayar_sah == '15' :
                    metoda_bayar_sah = "22"
                    jenis_notran = '1'
                elif metoda_bayar_sah == '16' or metoda_bayar_sah == '17' :
                    metoda_bayar_sah = "23"
                    jenis_notran = '1'
                elif metoda_bayar_sah == '18' or metoda_bayar_sah == '19': 
                    metoda_bayar_sah = "24"
                    jenis_notran = '1'
                elif metoda_bayar_sah == '05' or metoda_bayar_sah == '06': 
                    metoda_bayar_sah = "25"
                    jenis_notran = '1'
                elif metoda_bayar_sah == '31' or metoda_bayar_sah == '32' or metoda_bayar_sah == '60' or metoda_bayar_sah == '61': 
                    metoda_bayar_sah = "28"
                    jenis_notran = '1'
                elif metoda_bayar_sah == '01' or metoda_bayar_sah == '02': 
                    metoda_bayar_sah = "40"
                    jenis_notran = '1'
                elif metoda_bayar_sah == '21' and jenis_dinas == '20': 
                    metoda_bayar_sah = "11"
                    jenis_notran = '1'           
                elif metoda_bayar_sah == '21' and jenis_dinas == '21': 
                    metoda_bayar_sah = "12"
                    jenis_notran = '1'
                elif metoda_bayar_sah == '21' and jenis_dinas == '22':
                    metoda_bayar_sah = "13"
                    jenis_notran = '1'        
                if metoda_bayar_sah == '84' or metoda_bayar_sah == '83': 
                    metoda_bayar_sah =  "48"
                    jenis_notran = "1"           
                elif jenis_notran == '81':
                    jenis_notran = "2"   
                elif jenis_notran == '80' or jenis_notran == '82': 
                    jenis_notran = "3"
                
                tgl_transaksi = str(x[11])
                kspt_id = str(x[12])
                pultol_id = str(x[13])
                etoll_hash = str(x[14])
                tarif = str(x[15])
                sisa_saldo = str(x[16])
                query_data = "Insert INTO jid_transaksi_deteksi(\
                    asal_gerbang_id, \
                    gerbang_id, \
                    gardu_id, \
                    tgl_lap, \
                    shift, \
                    perioda, \
                    no_resi, \
                    gol_sah, \
                    etoll_id, \
                    metoda_bayar_sah, \
                    jenis_notran, \
                    tgl_transaksi, \
                    kspt_id, \
                    pultol_id, \
                    etoll_hash, \
                    tarif, \
                    sisa_saldo)" \
                                    "VALUES('" + asal_gerbang_id + "'" + ',' \
                                    + "'" + gerbang_id + "'" + ',' \
                                    + "'" + gardu_id + "'" + ',' \
                                    + "'" + tgl_lap + "'" + ',' \
                                    + "'" + shift + "'" + ',' \
                                    + "'" + perioda + "'" + ',' \
                                    + "'" + no_resi + "'" + ',' \
                                    + "'" + gol_sah + "'" + ',' \
                                    + "'" + etoll_id + "'" + ',' \
                                    + "'" + metoda_bayar_sah + "'" + ',' \
                                    + "'" + jenis_notran + "'" + ',' \
                                    + "'" + tgl_transaksi + "'" + ',' \
                                    + "'" + kspt_id + "'" + ',' \
                                    + "'" + pultol_id + "'" + ',' \
                                    + "'" + etoll_hash + "'" + ',' \
                                    + "'" + tarif + "'" + ',' \
                                    + "'" + sisa_saldo + "'" + ')' \
                                    "ON DUPLICATE KEY UPDATE " \
                                    + 'gerbang_id' + "=" + "'" + gerbang_id + "'" + ',' \
                                    + 'tgl_lap' + "=" + "'" + tgl_lap + "'" + ',' \
                                    + 'shift' + "=" + "'" + shift + "'" + ','  \
                                    + 'gardu_id' + "=" + "'" + gardu_id + "'" + ','  \
                                    + 'no_resi' + "=" + "'" + no_resi + "'" + ','  \
                                    + 'perioda' + "=" + "'" + perioda + "'" + ','  \
                                    + 'metoda_bayar_sah' + "=" + "'" + metoda_bayar_sah + "'" + ','  \
                                    + 'jenis_notran' + "=" + "'" + jenis_notran + "'" + ','  \
                                    + 'gol_sah' + "=" + "'" + gol_sah + "'" + ','  \
                                    + 'tarif' + "=" + "'" + tarif + "'" + ','  \
                                    + 'sisa_saldo' + "=" + "'" + sisa_saldo + "'" + ','  \
                                    + 'etoll_id' + "=" + "'" + etoll_id + "'" + ','  \
                                    + 'etoll_hash' + "=" + "'" + etoll_hash + "'" + ','  \
                                    + 'tgl_transaksi' + "=" + "'" + tgl_transaksi + "'" + ';'    
                # print ("Insert :" + str(query_data))          
                cnxmyjmto = mysql.connector.connect(
                user= config.jmtoUser, 
                password= config.jmtoPass, 
                host= config.jmtoHost, 
                database= config.jmtoDb)              
                cursorjmto = cnxmyjmto.cursor()
                cursorjmto.execute(query_data)
                cnxmyjmto.commit()   
                cnxmyjmto.close()
        else:
            logging.error(f'Config Not Falid : {config.configMode}')
                
    except Exception as e:
        logging.error("Gagal Insert Data Realtime : " + str(e))
        sys.exit()
