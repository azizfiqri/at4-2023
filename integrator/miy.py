import index
import pandas as pd
import logging
import mysql.connector
import pandas as pd
from datetime import date
import logging
import pymysql.cursors
import config
import sys


def dataRekap_miypcds():
    if config.configMode == '20' or config.configMode == '23':
            cnxdb_miy_pcds = pymysql.connect(
                host=config.srcHost,
                user=config.srcUser,
                password=config.srcPass,
                database=config.srcDb,
                cursorclass=pymysql.cursors.DictCursor)

    elif config.configMode == '30' or config.configMode == '33':
            cnxdb_miy_pcds = mysql.connector.connect(
                    host=config.srcHost,
                    user=config.srcUser,
                    password=config.srcPass,
                    database=config.srcDb)
    try:
        cursorsql = cnxdb_miy_pcds.cursor()
        query_sql =  (
            "Select "
            "SUBSTRING(Gb,2,2),"
             "Tanggal,"
             "Shift,"
             "SUBSTRING(Asal,2,2),"
             "Tun1 + Tun2 + Tun3 + Tun4 + Tun5 AS Tunai,"
             "Mdr1 + Mdr2 + Mdr3 + Mdr4 + Mdr5 AS Mdr,"
             "Bri1 + Bri2 + Bri3 + Bri4 + Bri5 AS Bri,"
             "Bni1 + Bni2 + Bni3 + Bni4 + Bni5 AS Bni,"
             "Bca1 + Bca2 + Bca3 + Bca4 + Bca5 AS Bca,"
             "Dki1 + Dki2 + Dki3 + Dki4 + Dki5 AS Dki,"
             "Flo11 + Flo12 + Flo13 + Flo14 + Flo15 + Flo21 + Flo22 + Flo23 + Flo24 + Flo25  AS Flo,"
             "Opr + Opr2 + Opr3 AS Opr,"
             "Mit1 + Mit2 AS Mit,"
             "Kary1 + Kary2 AS Kary,"
             "RpTun,"
             "RpMdr,"
             "RpBri,"
             "RpBni,"
             "RpBca,"
             "RpDki,"
             "RpFlo1 + RpFlo2 AS RpFlo,"
             "RpKary,"
             "RpMit,"
             "Lolos,"
             "Indamal,"
             "Majumundur "
             "from view_lalin_at4_2023 ORDER BY Tanggal asc "
        )
        # print (f" query = {query_sql}")
        cursorsql.execute(query_sql)
        data = pd.DataFrame(cursorsql.fetchall())
        rc = cursorsql.rowcount
        logging.info(f"JUMLAH RECORD DATA REKAP (MIY) : {rc}")
        dataRekap_miypcds = data.values.tolist()
        # print (f'res :{dataRekap_miypcds}')
        cursorsql.close()
        cnxdb_miy_pcds.close()
        return dataRekap_miypcds
    
    except Exception as e:
        logging.error("ERROR GET DATA MIY :" + e)  

def dataPendapatan_miypcds():
    if config.configMode == '20' or config.configMode == '23':
            cnxdb_miy_pcds = pymysql.connect(
                host=config.srcHost,
                user=config.srcUser,
                password=config.srcPass,
                database=config.srcDb,
                cursorclass=pymysql.cursors.DictCursor)

    elif config.configMode == '30' or config.configMode == '33':
            cnxdb_miy_pcds = mysql.connector.connect(
                    host=config.srcHost,
                    user=config.srcUser,
                    password=config.srcPass,
                    database=config.srcDb)
    try:
        cursorsql = cnxdb_miy_pcds.cursor()
        query_sql =  (
            "Select "
            "GerbangId,"
            "TanggalLaporan,"
            "Shift,"
            "IdInvestor,"
            "NamaInvestor,"
            "RpBagiHasilTunai,"
            "RpBagiHasileTollMandiri,"
            "RpBagiHasileTollBRI,"
            "RpBagiHasileTollBNI,"
            "RpBagiHasileTollBCA,"
            "RpBagiHasileTollDKI,"
            "RpBagiHasileTollFlo2 "
            "from view_lalin_rekap_split_2023 ORDER BY TanggalLaporan asc"
        )
        # print (f" query = {query_sql}")
        cursorsql.execute(query_sql)
        data = pd.DataFrame(cursorsql.fetchall())
        rc = cursorsql.rowcount
        logging.info(f"JUMLAH RECORD DATA Pendapatan (MIY) : {rc}")
        dataPendapatan_miypcds = data.values.tolist()
        # print ('res :' + str(dataMIY10))
        cursorsql.close()
        cnxdb_miy_pcds.close()
        return dataPendapatan_miypcds
    
    except Exception as e:
        logging.error("ERROR GET DATA MIY :" + e)  
        
def dataBagihasil_miypcds():
    if config.configMode == '20' or config.configMode == '23':
            cnxdb_miy_pcds = pymysql.connect(
                host=config.srcHost,
                user=config.srcUser,
                password=config.srcPass,
                database=config.srcDb,
                cursorclass=pymysql.cursors.DictCursor)

    elif config.configMode == '30' or config.configMode == '33':
            cnxdb_miy_pcds = mysql.connector.connect(
                    host=config.srcHost,
                    user=config.srcUser,
                    password=config.srcPass,
                    database=config.srcDb)
    try:
        cursorsql = cnxdb_miy_pcds.cursor()
        query_sql =  (
            "Select "
            "GerbangId,"
            "TanggalLaporan,"
            "Shift,"
            "IdInvestor,"
            "NamaInvestor,"
            "RpBagiHasilTunai,"
            "RpBagiHasileTollMandiri,"
            "RpBagiHasileTollBRI,"
            "RpBagiHasileTollBNI,"
            "RpBagiHasileTollBCA,"
            "RpBagiHasileTollDKI,"
            "RpBagiHasileTollFlo2 "
            "from view_lalin_rekap_split_2023 ORDER BY TanggalLaporan asc"
        )
        # print (f" query = {query_sql}")
        cursorsql.execute(query_sql)
        data = pd.DataFrame(cursorsql.fetchall())
        rc = cursorsql.rowcount
        logging.info(f"JUMLAH RECORD DATA Pendapatan (MIY) : {rc}")
        dataPendapatan_miypcds = data.values.tolist()
        # print ('res :' + str(dataMIY10))
        cursorsql.close()
        cnxdb_miy_pcds.close()
        return dataPendapatan_miypcds
    
    except Exception as e:
        logging.error("ERROR GET DATA MIY :" + e)  

def compare_dataRekap_miypcds():
    try:
        if config.configMode == '20' or config.configMode == '23':
                cnxdb_miy_pcds = pymysql.connect(
                    host=config.srcHost,
                    user=config.srcUser,
                    password=config.srcPass,
                    database=config.srcDb,
                    cursorclass=pymysql.cursors.DictCursor)
                try:
                    cursor_miy = cnxdb_miy_pcds.cursor()
                    query_miy = (
                    "SELECT count(Tanggal) AS dataIntegrator " 
                    "FROM view_lalin_at4_2023"
                    )
                    cursor_miy.execute(query_miy)
                    resdata = cursor_miy.fetchone()  
                    # print (f'resdata ={resdata}')                 
                    data = resdata['dataIntegrator']
                    logging.info(f"JUMLAH DATA SERVER MIY = {data} ")
                    cnxdb_miy_pcds.close()
                    return data
                except Exception as e:
                    logging.error("ERROR GET DATA COMPARE MIY : " + str(e))  
        elif config.configMode == '30' or config.configMode == '33':
                cnxdb_miy_pcds = mysql.connector.connect(
                        host=config.srcHost,
                        user=config.srcUser,
                        password=config.srcPass,
                        database=config.srcDb)
                try:
                    cursor_miy = cnxdb_miy_pcds.cursor()
                    query_miy = (
                    "SELECT count(Tanggal) AS dataIntegrator " 
                    "FROM view_lalin_at4_2023"
                    )
                    cursor_miy.execute(query_miy)
                    resdata = cursor_miy.fetchone()
                    data = resdata[0]
                    # print (data)
                    logging.info(f"JUMLAH DATA SERVER MIY = {data} ")
                    cnxdb_miy_pcds.close()
                    return data

                except Exception as e:
                    logging.error("ERROR GET DATA COMPARE MIY : " + str(e))   
    except Exception as e:
        logging.error("FAIL CONNECT DATABASE MIY :" + e)    

def compare_dataPendapatan_miypcds():
    try:
        if config.configMode == '20' or config.configMode == '23':
                cnxdb_miy_pcds = pymysql.connect(
                    host=config.srcHost,
                    user=config.srcUser,
                    password=config.srcPass,
                    database=config.srcDb,
                    cursorclass=pymysql.cursors.DictCursor)
                try:
                    cursor_miy = cnxdb_miy_pcds.cursor()
                    query_miy = (
                    "SELECT count(Tanggal) AS dataIntegrator " 
                    "FROM view_lalin_at4_2023"
                    )
                    cursor_miy.execute(query_miy)
                    resdata = cursor_miy.fetchone()  
                    # print (f'resdata ={resdata}')                 
                    data = resdata['dataIntegrator']
                    logging.info(f"JUMLAH DATA SERVER MIY = {data} ")
                    cnxdb_miy_pcds.close()
                    return data
                except Exception as e:
                    logging.error("ERROR GET DATA COMPARE MIY : " + str(e))  
        elif config.configMode == '30' or config.configMode == '33':
                cnxdb_miy_pcds = mysql.connector.connect(
                        host=config.srcHost,
                        user=config.srcUser,
                        password=config.srcPass,
                        database=config.srcDb)
                try:
                    cursor_miy = cnxdb_miy_pcds.cursor()
                    query_miy = (
                    "SELECT count(Tanggal) AS dataIntegrator " 
                    "FROM view_lalin_at4_2023"
                    )
                    cursor_miy.execute(query_miy)
                    resdata = cursor_miy.fetchone()
                    data = resdata[0]
                    # print (data)
                    logging.info(f"JUMLAH DATA SERVER MIY = {data} ")
                    cnxdb_miy_pcds.close()
                    return data

                except Exception as e:
                    logging.error("ERROR GET DATA COMPARE MIY : " + str(e))   
    except Exception as e:
        logging.error("FAIL CONNECT DATABASE MIY :" + e)    

def compare_dataBagihasil_miypcds():
    try:
        if config.configMode == '20' or config.configMode == '23':
                cnxdb_miy_pcds = pymysql.connect(
                    host=config.srcHost,
                    user=config.srcUser,
                    password=config.srcPass,
                    database=config.srcDb,
                    cursorclass=pymysql.cursors.DictCursor)
                try:
                    cursor_miy = cnxdb_miy_pcds.cursor()
                    query_miy = (
                    "SELECT count(Tanggal) AS dataIntegrator " 
                    "FROM view_lalin_at4_2023"
                    )
                    cursor_miy.execute(query_miy)
                    resdata = cursor_miy.fetchone()  
                    # print (f'resdata ={resdata}')                 
                    data = resdata['dataIntegrator']
                    logging.info(f"JUMLAH DATA SERVER MIY = {data} ")
                    cnxdb_miy_pcds.close()
                    return data
                except Exception as e:
                    logging.error("ERROR GET DATA COMPARE MIY : " + str(e))  
        elif config.configMode == '30' or config.configMode == '33':
                cnxdb_miy_pcds = mysql.connector.connect(
                        host=config.srcHost,
                        user=config.srcUser,
                        password=config.srcPass,
                        database=config.srcDb)
                try:
                    cursor_miy = cnxdb_miy_pcds.cursor()
                    query_miy = (
                    "SELECT count(Tanggal) AS dataIntegrator " 
                    "FROM view_lalin_at4_2023"
                    )
                    cursor_miy.execute(query_miy)
                    resdata = cursor_miy.fetchone()
                    data = resdata[0]
                    # print (data)
                    logging.info(f"JUMLAH DATA SERVER MIY = {data} ")
                    cnxdb_miy_pcds.close()
                    return data

                except Exception as e:
                    logging.error("ERROR GET DATA COMPARE MIY : " + str(e))   
    except Exception as e:
        logging.error("FAIL CONNECT DATABASE MIY :" + e)    

def InsertresultDataRekap(resultData):
    try:
          
        if config.configMode == '30' or config.configMode == '20'  : #MIY
            count = 0 
            for x in resultData:
                IdGerbang = x[0]
                Tanggal = x[1]
                shift = x[2]
                IdAsalGerbang = x[3]
                if IdAsalGerbang == '':
                   IdAsalGerbang = 0
                Tunai = x[4]
                eMandiri = x[5]
                eBri = x[6]
                eBni = x[7]
                eBca = x[8]
                eDKI = x[9]
                eFlo = x[10]
                DinasOpr = x[11]
                DinasMitra = x[12]
                DinasKary = x[13]
                RpTunai = x[14]
                RpeMandiri = x[15]
                RpeBri = x[16]
                RpeBni = x[17]
                RpeBca = x[18]
                RpeDKI = x[19]
                RpeFlo = x[20]
                RpDinasKary = x[21]
                RpDinasMitra = x[22]
                Lolos = x[23]
                Indamal = x[24]
                Mjmdr = x[25]
                query_data = (
                "Insert INTO jid_rekap_at4_2023("
                "IdGerbang,"
                "Tanggal,"
                "shift," 
                "IdAsalGerbang,"
                "Tunai,"
                "eMandiri,"
                "eBri," 
                "eBni," 
                "eBca," 
                "eDKI," 
                "eFlo," 
                "DinasOpr,"
                "DinasMitra," 
                "DinasKary," 
                "RpTunai,"
                "RpeMandiri," 
                "RpeBri,"
                "RpeBni," 
                "RpeBca," 
                "RpeDKI,"
                "RpeFlo,"
                "RpDinasKary,"
                "RpDinasMitra,"
                "Lolos,"
                "Indamal,"
                "Mjmdr)"                
                "VALUES ("
                f'{IdGerbang},"{Tanggal}",{shift},{IdAsalGerbang},{Tunai},{eMandiri},{eBri},{eBni},{eBca},{eDKI},{eFlo},' 
                f'{DinasOpr},{DinasMitra},{DinasKary},{RpTunai},{RpeMandiri},{RpeBri},{RpeBni},{RpeBca},{RpeDKI},{RpeFlo},{RpDinasKary},{RpDinasMitra},{Lolos},{Indamal},{Mjmdr})'   
                "ON DUPLICATE KEY UPDATE " \
                f"IdGerbang = {IdGerbang},"
                f"Tanggal = '{Tanggal}',"
                f"shift = {shift}," 
                f"IdAsalGerbang = {IdAsalGerbang}," 
                f"Tunai = {Tunai},"
                f"eMandiri = {eMandiri},"
                f"eBri = {eBri},"
                f"eBni = {eBni},"
                f"eBca = {eBca},"
                f"eDKI = {eDKI},"
                f"eFlo = {eFlo},"
                f"DinasOpr = {DinasOpr},"
                f"DinasMitra = {DinasMitra}," 
                f"DinasKary = {DinasKary},"
                f"RpTunai = {RpTunai}," 
                f"RpeMandiri = {RpeMandiri}," 
                f"RpeBri = {RpeBri},"
                f"RpeBni = {RpeBni},"
                f"RpeBca = {RpeBca},"
                f"RpeDKI = {RpeDKI},"
                f"RpeFlo = {RpeFlo},"
                f"RpDinasKary = {RpDinasKary},"
                f"RpDinasMitra = {RpDinasMitra},"
                f"Lolos = {Lolos},"
                f"Indamal = {Indamal},"
                f"Mjmdr = {Mjmdr};" 
                )  
                # print (f"Insert : {query_data}" )        
                cnxmyjmto = mysql.connector.connect(
                user= config.jmtoUser, 
                password= config.jmtoPass, 
                host= config.jmtoHost, 
                database= config.jmtoDb)              
                cursorjmto = cnxmyjmto.cursor()
                cursorjmto.execute(query_data)
                cnxmyjmto.commit()
                count += 1   
                cnxmyjmto.close()
            logging.info(F"JUMLAH RECORD INSERT DATA REKAP (MEDIASI) : {count} ")
                
        else:
            logging.error(f'Config Not Falid : {config.configMode}')
                
    except Exception as e:
        logging.error("Gagal Insert Data Rekap : " + str(e))
        sys.exit()

def InsertresultDataPendapatan(resultData):
    try:
          
        if config.configMode == '30' or config.configMode == '20'  : #MIY
            count = 0 
            for x in resultData:
                IdGerbang = x[0]
                Tanggal = x[1]
                shift = x[2]
                IdAsalGerbang = x[3]
                if IdAsalGerbang == '':
                   IdAsalGerbang = 0
                Tunai = x[4]
                eMandiri = x[5]
                eBri = x[6]
                eBni = x[7]
                eBca = x[8]
                eDKI = x[9]
                eFlo = x[10]
                DinasOpr = x[11]
                DinasMitra = x[12]
                DinasKary = x[13]
                RpTunai = x[14]
                RpeMandiri = x[15]
                RpeBri = x[16]
                RpeBni = x[17]
                RpeBca = x[18]
                RpeDKI = x[19]
                RpeFlo = x[20]
                RpDinasKary = x[21]
                RpDinasMitra = x[22]
                Lolos = x[23]
                Indamal = x[24]
                Mjmdr = x[25]
                query_data = (
                "Insert INTO jid_Pendapatan_at4_2023("
                "IdGerbang,"
                "Tanggal,"
                "shift," 
                "IdAsalGerbang,"
                "Tunai,"
                "eMandiri,"
                "eBri," 
                "eBni," 
                "eBca," 
                "eDKI," 
                "eFlo," 
                "DinasOpr,"
                "DinasMitra," 
                "DinasKary," 
                "RpTunai,"
                "RpeMandiri," 
                "RpeBri,"
                "RpeBni," 
                "RpeBca," 
                "RpeDKI,"
                "RpeFlo,"
                "RpDinasKary,"
                "RpDinasMitra,"
                "Lolos,"
                "Indamal,"
                "Mjmdr)"                
                "VALUES ("
                f'{IdGerbang},"{Tanggal}",{shift},{IdAsalGerbang},{Tunai},{eMandiri},{eBri},{eBni},{eBca},{eDKI},{eFlo},' 
                f'{DinasOpr},{DinasMitra},{DinasKary},{RpTunai},{RpeMandiri},{RpeBri},{RpeBni},{RpeBca},{RpeDKI},{RpeFlo},{RpDinasKary},{RpDinasMitra},{Lolos},{Indamal},{Mjmdr})'   
                "ON DUPLICATE KEY UPDATE " \
                f"IdGerbang = {IdGerbang},"
                f"Tanggal = '{Tanggal}',"
                f"shift = {shift}," 
                f"IdAsalGerbang = {IdAsalGerbang}," 
                f"Tunai = {Tunai},"
                f"eMandiri = {eMandiri},"
                f"eBri = {eBri},"
                f"eBni = {eBni},"
                f"eBca = {eBca},"
                f"eDKI = {eDKI},"
                f"eFlo = {eFlo},"
                f"DinasOpr = {DinasOpr},"
                f"DinasMitra = {DinasMitra}," 
                f"DinasKary = {DinasKary},"
                f"RpTunai = {RpTunai}," 
                f"RpeMandiri = {RpeMandiri}," 
                f"RpeBri = {RpeBri},"
                f"RpeBni = {RpeBni},"
                f"RpeBca = {RpeBca},"
                f"RpeDKI = {RpeDKI},"
                f"RpeFlo = {RpeFlo},"
                f"RpDinasKary = {RpDinasKary},"
                f"RpDinasMitra = {RpDinasMitra},"
                f"Lolos = {Lolos},"
                f"Indamal = {Indamal},"
                f"Mjmdr = {Mjmdr};" 
                )  
                # print (f"Insert : {query_data}" )        
                cnxmyjmto = mysql.connector.connect(
                user= config.jmtoUser, 
                password= config.jmtoPass, 
                host= config.jmtoHost, 
                database= config.jmtoDb)              
                cursorjmto = cnxmyjmto.cursor()
                cursorjmto.execute(query_data)
                cnxmyjmto.commit()
                count += 1   
                cnxmyjmto.close()
            logging.info(F"JUMLAH RECORD INSERT DATA Pendapatan (MEDIASI) : {count} ")
                
        else:
            logging.error(f'Config Not Falid : {config.configMode}')
                
    except Exception as e:
        logging.error("Gagal Insert Data Pendapatan : " + str(e))
        sys.exit()
