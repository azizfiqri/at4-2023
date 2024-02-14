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
            f'from "{config.srcDb}".dbsharejmto.tblshift_trno_{config.namaGerbang}_2023 a, "{config.srcDb}".dbsharejmto.tblshiftetoll_trno_{config.namaGerbang}_2023 b WHERE (a.id=b.id) order by id asc'
        )
        # print (f"Query : {query_delameta}")
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
            f'from "{config.srcDb}".dbsharejmto.tblshift_trno_{config.namaGerbang}_2023 a, "{config.srcDb}".dbsharejmto.tblshiftetoll_trno_{config.namaGerbang}_2023 b WHERE (a.id=b.id) order by id asc'
        )
        # print (f"query {query_delameta}")
        cursor_compare.execute(query_delameta)
        data = pd.DataFrame(cursor_compare.fetchall())
        rc = cursor_compare.rowcount
        logging.info("JUMLAH DATA Rekap : " + str(rc))
        compareDelameta = data.values.tolist()
        # print (f"rekap data {compareDelameta}")
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
                hari = resID[1:3]
                bulan = resID[3:5]
                tahun = resID[5:7]
                Tanggal = str(tahun) + str(bulan) + str(hari)
                Shift = resID[0:1]
                IdGerbang = config.idGerbang
                IdAsalGerbang = 0
                if IdAsalGerbang == '':
                   IdAsalGerbang = 0
                resTransaksiTunai = x[1][0]
                tunaiGol1 = resTransaksiTunai[0]
                tunaiGol2 = resTransaksiTunai[1]
                tunaiGol3 = resTransaksiTunai[2]
                tunaiGol4 = resTransaksiTunai[3]
                tunaiGol5 = resTransaksiTunai[4]
                tunaiGol6 = resTransaksiTunai[5]
                totalTunai = tunaiGol1 + tunaiGol2 + tunaiGol3 + tunaiGol4 + tunaiGol5 + tunaiGol6
                # print (f"Tunai Lalin : {tunaiGol1} + {tunaiGol2} + {tunaiGol3} + {tunaiGol4} + {tunaiGol5} + {tunaiGol6} = {totalTunai}")
                resTransaksiMandiri = x[4][0]
                mandiriGol1 = resTransaksiMandiri[0]
                mandiriGol2 = resTransaksiMandiri[1]
                mandiriGol3 = resTransaksiMandiri[2]
                mandiriGol4 = resTransaksiMandiri[3]
                mandiriGol5 = resTransaksiMandiri[4]
                mandiriGol6 = resTransaksiMandiri[5]
                totalMandiri = mandiriGol1 + mandiriGol2 + mandiriGol3 + mandiriGol4 + mandiriGol5 + mandiriGol6
                # print (f"total Mandiri = {mandiriGol1} + {mandiriGol2} + {mandiriGol3} + {mandiriGol4} + {mandiriGol5} + {mandiriGol6} = {totalMandiri}")
                resTransaksiBri = x[4][1]
                briGol1 = resTransaksiBri[0]
                briGol2 = resTransaksiBri[1]
                briGol3 = resTransaksiBri[2]
                briGol4 = resTransaksiBri[3]
                briGol5 = resTransaksiBri[4]
                briGol6 = resTransaksiBri[5]
                totalBri = briGol1 + briGol2 + briGol3 + briGol4 + briGol5 + briGol6
                # print (f"total Bri = {briGol1} + {briGol2} + {briGol3} + {briGol4} + {briGol5} + {briGol6} = {totalBri}")
                resTransaksiBni = x[4][2]
                bniGol1 = resTransaksiBni[0]
                bniGol2 = resTransaksiBni[1]
                bniGol3 = resTransaksiBni[2]
                bniGol4 = resTransaksiBni[3]
                bniGol5 = resTransaksiBni[4]
                bniGol6 = resTransaksiBni[5]
                totalBni = bniGol1 + bniGol2 + bniGol3 + bniGol4 + bniGol5 + bniGol6
                # print (f"total Bni = {bniGol1} + {bniGol2} + {bniGol3} + {bniGol4} + {bniGol5} + {bniGol6} = {totalBni}")
                resTransaksiBca = x[4][3]
                bcaGol1 = resTransaksiBca[0]
                bcaGol2 = resTransaksiBca[1]
                bcaGol3 = resTransaksiBca[2]
                bcaGol4 = resTransaksiBca[3]
                bcaGol5 = resTransaksiBca[4]
                bcaGol6 = resTransaksiBca[5]
                totalBca = bcaGol1 + bcaGol2 + bcaGol3 + bcaGol4 + bcaGol5 + bcaGol6
                # print (f"total Bca = {bcaGol1} + {bcaGol2} + {bcaGol3} + {bcaGol4} + {bcaGol5} + {bcaGol6} = {totalBca}")
                resTransaksiDki = x[4][4]
                dkiGol1 = resTransaksiDki[0]
                dkiGol2 = resTransaksiDki[1]
                dkiGol3 = resTransaksiDki[2]
                dkiGol4 = resTransaksiDki[3]
                dkiGol5 = resTransaksiDki[4]
                dkiGol6 = resTransaksiDki[5]
                totalDki = dkiGol1 + dkiGol2 + dkiGol3 + dkiGol4 + dkiGol5 + dkiGol6
                # print (f"total DKI = {dkiGol1} + {dkiGol2} + {dkiGol3} + {dkiGol4} + {dkiGol5} + {dkiGol6} = {totalDki}")
                resTransaksiFlo = x[1][1]
                floGol1 = resTransaksiFlo[0]
                floGol2 = resTransaksiFlo[1]
                floGol3 = resTransaksiFlo[2]
                floGol4 = resTransaksiFlo[3]
                floGol5 = resTransaksiFlo[4]
                floGol6 = resTransaksiFlo[5]
                totalFlo = floGol1 + floGol2 + floGol3 + floGol4 + floGol5 + floGol6
                # print (f"total FLO = {floGol1} + {floGol2} + {floGol3} + {floGol4} + {floGol5} + {floGol6} = {totalFlo}")
                DinasOpr = 0
                DinasMitra = 0
                DinasKary =0
                # print (f"total Dinas = {DinasGol1} + {DinasGol2} + {DinasGol3} + {DinasGol4} + {DinasGol5} + {DinasGol6} = {totalDinas}")
                RpTunai=x[3][0]
                # print (f"Tunai Pendapatan Tunai : {tunaiPendapatanGol1} + {tunaiPendapatanGol2} + {tunaiPendapatanGol3} + {tunaiPendapatanGol4} + {tunaiPendapatanGol5} + {tunaiPendapatanGol6} = {RpTunai}")
                RpeMandiri = x[5][0]
                # print (f'pendapatan mandiri : {RpeMandiri}')
                RpeBri = x[5][1]
                # print (f"Bri Pendapatan Bri : {RpeBri}")
                RpeBni = x[5][2]
                # print (f"Bni Pendapatan Bni : {RpeBni}")
                RpeBca = x[5][3]
                # print (f"Bca Pendapatan Bca : {RpeBca}")
                RpeDki = x[5][4]
                # print (f"Dki Pendapatan Dki : {RpeDki}")
                RpeFlo = x[3][2]
                # print (f"Flo Pendapatan Flo : {RpeFlo}")
                RpDinasKary = 0
                RpDinasMitra = 0
                # print (f"Dinas Pendapatan Dinas : {RpeDinas}")
                Lolos = x[2]
                Indamal = 0
                Mjmdr = 0
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
                f'{IdGerbang},"{Tanggal}",{Shift},{IdAsalGerbang},{totalTunai},{totalMandiri},{totalBri},{totalBni},{totalBca},{totalDki},{totalFlo},' 
                f'{DinasOpr},{DinasMitra},{DinasKary},{RpTunai},{RpeMandiri},{RpeBri},{RpeBni},{RpeBca},{RpeDki},{RpeFlo},{RpDinasKary},{RpDinasMitra},{Lolos},{Indamal},{Mjmdr})'   
                "ON DUPLICATE KEY UPDATE " \
                f"IdGerbang = {IdGerbang},"
                f"Tanggal = '{Tanggal}',"
                f"shift = {Shift}," 
                f"IdAsalGerbang = {IdAsalGerbang}," 
                f"Tunai = {totalTunai},"
                f"eMandiri = {totalMandiri},"
                f"eBri = {totalBri},"
                f"eBni = {totalBni},"
                f"eBca = {totalBca},"
                f"eDKI = {totalDki},"
                f"eFlo = {totalFlo},"
                f"DinasOpr = {DinasOpr},"
                f"DinasMitra = {DinasMitra}," 
                f"DinasKary = {DinasKary},"
                f"RpTunai = {RpTunai}," 
                f"RpeMandiri = {RpeMandiri}," 
                f"RpeBri = {RpeBri},"
                f"RpeBni = {RpeBni},"
                f"RpeBca = {RpeBca},"
                f"RpeDKI = {RpeDki},"
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

                