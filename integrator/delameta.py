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
        query_delameta = (
            "Select "
            "a.id, a.transaksi,a.lolos,a.pendapatan,b.transaksi AS Transaksi_etoll, b.pendapatan AS Pendapatan_etoll "
            f"from bali.{config.namaGerbang}.tblshifttrno a, bali.{config.namaGerbang}.tblshiftetolltrno b WHERE (a.id=b.id) limit 1"
        )
        print (f"Query : {query_delameta}")
        cursor_data.execute(query_delameta)
        data = pd.DataFrame(cursor_data.fetchall())
        rc = cursor_data.rowcount
        logging.info("JUMLAH DATA REKAP : " + str(rc))
        dataDelameta = data.values.tolist()
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
        query_delameta = (
            "Select "
            "a.id, a.transaksi,a.lolos,a.pendapatan,b.transaksi AS Transaksi_etoll, b.pendapatan AS Pendapatan_etoll "
            f"from bali.{config.namaGerbang}.tblshifttrno a, bali.{config.namaGerbang}.tblshiftetolltrno b WHERE (a.id=b.id)"
        )
        print (f"query {query_delameta}")
        cursor_compare.execute(query_delameta)
        data = pd.DataFrame(cursor_compare.fetchall())
        print (data)
        # print (f"id 0 0: {data[0][0]}")
        # print (f"transaksi 1 0: {data[1][0]}")
        # print (f"lolos: {data[2][0]}")
        # print (f"pendapatan: {data[3][0][3]}")
        rc = cursor_compare.rowcount
        logging.info("JUMLAH DATA Rekap : " + str(rc))
        compareDelameta = data.values.tolist()
        print (f"rekap data {compareDelameta}")
        cnxdb_compare.close()
        return compareDelameta
    
    except Exception as e:
        logging.error("ERROR Call Data Delameta" + str(e))
        
def InsertresultData(resultData):
    try:       
        if config.configMode == '60': #DELAMETA 
            count = 0 
            for x in resultData:
                # print (x)
                resID= x[0]
                tanggal = resID[1:7]
                shift = resID[0:1]
                resTransaksi = x[1][0]
                tunaiGol1 = resTransaksi[0]
                print (tunaiGol1)
                # IdAsalGerbang = x[3]
                # if IdAsalGerbang == '':
                #    IdAsalGerbang = 0
                # Tunai = x[4]
                # eMandiri = x[5]
                # eBri = x[6]
                # eBni = x[7]
                # eBca = x[8]
                # eDKI = x[9]
                # eFlo = x[10]
                # DinasOpr = x[11]
                # DinasMitra = x[12]
                # DinasKary = x[13]
                # RpTunai = x[14]
                # RpeMandiri = x[15]
                # RpeBri = x[16]
                # RpeBni = x[17]
                # RpeBca = x[18]
                # RpeDKI = x[19]
                # RpeFlo = x[20]
                # RpDinasKary = x[21]
                # RpDinasMitra = x[22]
                # Lolos = x[23]
                # Indamal = x[24]
                # Mjmdr = x[25]
                # query_data = (
                # "Insert INTO jid_rekap_at4_2023("
                # "IdGerbang,"
                # "Tanggal,"
                # "shift," 
                # "IdAsalGerbang,"
                # "Tunai,"
                # "eMandiri,"
                # "eBri," 
                # "eBni," 
                # "eBca," 
                # "eDKI," 
                # "eFlo," 
                # "DinasOpr,"
                # "DinasMitra," 
                # "DinasKary," 
                # "RpTunai,"
                # "RpeMandiri," 
                # "RpeBri,"
                # "RpeBni," 
                # "RpeBca," 
                # "RpeDKI,"
                # "RpeFlo,"
                # "RpDinasKary,"
                # "RpDinasMitra,"
                # "Lolos,"
                # "Indamal,"
                # "Mjmdr)"                
                # "VALUES ("
                # f'{IdGerbang},"{Tanggal}",{shift},{IdAsalGerbang},{Tunai},{eMandiri},{eBri},{eBni},{eBca},{eDKI},{eFlo},' 
                # f'{DinasOpr},{DinasMitra},{DinasKary},{RpTunai},{RpeMandiri},{RpeBri},{RpeBni},{RpeBca},{RpeDKI},{RpeFlo},{RpDinasKary},{RpDinasMitra},{Lolos},{Indamal},{Mjmdr})'   
                # "ON DUPLICATE KEY UPDATE " \
                # f"IdGerbang = {IdGerbang},"
                # f"Tanggal = '{Tanggal}',"
                # f"shift = {shift}," 
                # f"IdAsalGerbang = {IdAsalGerbang}," 
                # f"Tunai = {Tunai},"
                # f"eMandiri = {eMandiri},"
                # f"eBri = {eBri},"
                # f"eBni = {eBni},"
                # f"eBca = {eBca},"
                # f"eDKI = {eDKI},"
                # f"eFlo = {eFlo},"
                # f"DinasOpr = {DinasOpr},"
                # f"DinasMitra = {DinasMitra}," 
                # f"DinasKary = {DinasKary},"
                # f"RpTunai = {RpTunai}," 
                # f"RpeMandiri = {RpeMandiri}," 
                # f"RpeBri = {RpeBri},"
                # f"RpeBni = {RpeBni},"
                # f"RpeBca = {RpeBca},"
                # f"RpeDKI = {RpeDKI},"
                # f"RpeFlo = {RpeFlo},"
                # f"RpDinasKary = {RpDinasKary},"
                # f"RpDinasMitra = {RpDinasMitra},"
                # f"Lolos = {Lolos},"
                # f"Indamal = {Indamal},"
                # f"Mjmdr = {Mjmdr};" 
                # )  
                # # print (f"Insert : {query_data}" )        
                # cnxmyjmto = mysql.connector.connect(
                # user= config.jmtoUser, 
                # password= config.jmtoPass, 
                # host= config.jmtoHost, 
                # database= config.jmtoDb)              
                # cursorjmto = cnxmyjmto.cursor()
                # cursorjmto.execute(query_data)
                # cnxmyjmto.commit()
                # count += 1   
                # cnxmyjmto.close()
            logging.info(F"JUMLAH RECORD INSERT DATA REKAP (MEDIASI) : {count} ")
                
        else:
            logging.error(f'Config Not Falid : {config.configMode}')
                
    except Exception as e:
        logging.error("Gagal Insert Data Rekap : " + str(e))
        sys.exit()

                