import pandas as pd
import logging
import mysql.connector
import pandas as pd
from datetime import date
import logging
import pymysql.cursors
import config

def compare_data_mediasi():
    try:
        cnxmyjmto = mysql.connector.connect(
        user= config.jmtoUser, 
        password= config.jmtoPass, 
        host= config.jmtoHost, 
        database= config.jmtoDb)              
    except Exception as e:
        logging.error("FAIL CONNECT DATABASE MIY :" + e)    
    try:
        cursor_miy = cnxmyjmto.cursor()
        query_miy = (
            "SELECT count(Tanggal) AS dataIntegrator " 
            "FROM jid_rekap_at4_2023"
            )
        cursor_miy.execute(query_miy)
        resdata = cursor_miy.fetchone()
        data = resdata[0]
        logging.info(f"JUMLAH DATA SERVER MEDIASI = {data} ")
        cursor_miy.close()
        return data
    
    except Exception as e:
        logging.error("ERROR GET DATA COMPARE MEDIASI : " + str(e))
    