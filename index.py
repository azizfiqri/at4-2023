import logging
import mysql.connector
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import config
import integrator.delameta
import integrator.miy
import integrator.mediasi
import time
import pandas as pd
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%m/%d/%Y %H:%M:%S",
)


def db_jmto():
    try:
        cnxdb_jmto = mysql.connector.connect(
            user=config.jmtoUser,
            password=config.jmtoPass,
            host=config.jmtoHost,
            database=config.jmtoDb,
            port=config.jmtoPort)
        if cnxdb_jmto.is_connected():
            db_info = cnxdb_jmto.get_server_info()
            logging.info("Database Mediasi Connected ")
            logging.info(f"Database Version : {db_info}")
            cnxdb_jmto.close()
    except Exception as e:
        logging.error(f"Error while connecting to Database Mediasi JMTO {e}")
        sys.exit()

def insertDataRekap():
    result = 0
    try:
        if config.configMode == '30' or config.configMode == '20': #open - exit
            result = integrator.miy.data_miypcds()   
            integrator.miy.InsertresultData(result)
        elif config.configMode == '23': #entrance & exit            
            resultentrance = integrator.miy.data_miypcds_ent()
            integrator.miy.InsertresultData(resultentrance)    
            resultexit = integrator.miy.data_miypcds()   
            integrator.miy.InsertresultData(resultexit)      
        #DELAMETA
        elif config.configMode == '60': # open
            result = integrator.delameta.data_delameta()
            integrator.delameta.InsertresultData(result)
        elif config.configMode == '63': # entrance & Exit
            resultentrance = integrator.delameta.data_delameta_entrance()
            integrator.delameta.InsertresultData(resultentrance)
            resultexit = integrator.delameta.data_delameta_exit()
            integrator.delameta.InsertresultData(resultexit)  
        #JMTO
        elif config.configMode == '99':    
            return False
        else:
            logging.warning("Invalid Configuration Mode")

    except Exception as e:
        logging.error("Gagal Get Data Rekap : " + str(e))

def checkSelisihdata():
    cnxdb_jmto = mysql.connector.connect(
            user=config.jmtoUser,
            password=config.jmtoPass,
            host=config.jmtoHost,
            database='data_master',
            port=config.jmtoPort)
    cursorsql = cnxdb_jmto.cursor()
    querycompare = f"SELECT tgl_lap,"
    f"shift,"
    "gerbang_nama"
    "from v_AllDataCompare"
    "where tgl_lap <= DATE(NOW() - INTERVAL 7 DAY) AND gerbang_id = '{config.idGerbang}' ORDER BY tgl_lap desc"
    # print ('Query Data Compare : ' + str(querycompare))
    cursorsql.execute(querycompare)
    resdataCompare = pd.DataFrame(cursorsql.fetchall())
    rc = cursorsql.rowcount
    logging.info(f"Total Jumlah Selisih Data Gardu  : {rc} ")
    resSelisihdata = resdataCompare.values.tolist()
    # print ('res :' + str(checkSelisihdata))
    return resSelisihdata     
        

def compare_insert_data():
    try:
        #MIY
        if config.configMode == '20' or config.configMode == '30' :
            result = integrator.miy.compare_data_miypcds()
        elif config.configMode == '21' or config.configMode == '31' :
            result = integrator.miy.compare_data_miypcds_ent() 
        elif config.configMode == '23' or config.configMode == '33' :
            resultent = integrator.miy.compare_data_miypcds_ent() 
            resultext = integrator.miy.compare_data_miypcds()
            result = resultent + resultext
        elif config.configMode == '40' or config.configMode == '50':
            result = integrator.miy.compare_data_miybcds()
        #DELAMETA
        elif config.configMode == '60':    
            result = integrator.delameta.compare_data_delameta()
        elif config.configMode == '61':
            result = integrator.delameta.compare_data_delameta_entrance()
        elif config.configMode == '62':    
            result = integrator.delameta.compare_data_delameta_exit()
        elif config.configMode == '63':      
            resultent = integrator.delameta.compare_data_delameta_entrance()
            resultext = integrator.delameta.compare_data_delameta_exit()
            result = resultent + resultext
        #JMTO
        elif config.configMode == '99':    
            result = integrator.jmto.compare_data_jmtobcds()
        for x in result:
                tgl_lap = str(x[0])
                gerbang_id = str(x[1])
                gardu_id = str(x[2])
                shift = str(x[3])
                gol = str(x[4])
                data_integrator = str(x[5])
                query_data = "Insert INTO tbl_data_compare( tgl_lap, gerbang_id, shift, data_integrator )" \
                        "VALUES('" + tgl_lap + "'" + ',' \
                        + "'" + gerbang_id + "'" + ',' \
                        + "'" + shift + "'" + ',' \
                        + "'" + data_integrator + "'" + ')' \
                        "ON DUPLICATE KEY UPDATE " \
                        + 'tgl_lap' + "=" + "'" + tgl_lap + "'" + ',' \
                        + 'gerbang_id' + "=" + "'" + gerbang_id + "'" + ',' \
                        + 'shift' + "=" + "'" + shift + "'" + ','  \
                        + 'data_integrator' + "=" + "'" + data_integrator + "'" + ';' 
                # print (query_data)          
                cnxmyjmto = mysql.connector.connect(
                user= config.jmtoUser, 
                password= config.jmtoPass, 
                host= config.jmtoHost, 
                database= config.jmtoDb)              
                cursorjmto = cnxmyjmto.cursor()
                cursorjmto.execute(query_data)
                cnxmyjmto.commit()   
                cnxmyjmto.close()
            
    except Exception as e:
        logging.error("Gagal Insert Data Compare " + str(e))

def main():
    logging.info("START")
    logging.info(f"Version : {config.version} ")
    logging.info(f"CFG Mode : {config.configMode} ")
    logging.info(f"Gerbang : {config.namaGerbang} ({config.idGerbang})")
    db_jmto()
    if config.configMode == '30' or  config.configMode == '20':
        resdataMIY = integrator.miy.compare_data_miypcds()
        resdataMediasi = integrator.mediasi.compare_data_mediasi()
        if resdataMIY == resdataMediasi:
            logging.info("Data Source dengan Data Server Mediasi Sama")
        else:
            logging.info("Data Source dengan Mediasi berbeda")
            logging.info("Proses Sync Data")
            insertDataRekap()
    elif config.configMode == '60':
        resdataDB = integrator.delameta.compare_data_delameta()
        resdataMediasi = integrator.mediasi.compare_data_mediasi()
        if resdataDB == resdataMediasi:
            logging.info("Data Source dengan Data Server Mediasi Sama")
        else:
            logging.info("Data Source dengan Mediasi berbeda")
            logging.info("Proses Sync Data")
            insertDataRekap()
    else:
        logging.info("--")
    
if __name__ == '__main__': 
    main()
    # scheduler = BackgroundScheduler()
    # scheduler.add_job(main, 'interval', minutes= 240)
    # scheduler.start()
    # try:
    #     # This is here to simulate application activity (which keeps the main thread alive).
    #     while True:
    #         time.sleep(2)
    # except (KeyboardInterrupt, SystemExit):
    #     # Not strictly necessary if daemonic mode is enabled but should be done if possible
    #     scheduler.shutdown()