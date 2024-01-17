import os

version = "DWH VERSION 1.0.0-AT4-2023"

intervalQuery = 1

############## MIY PCDS ################
# 4 20 = src - Mariddb Versi 5 PCDS OPEN/EXIT (MIY)
#21 = src - Mariadb Versi 5 PCDS ENTRANCE (MIY)
#23 = src - Mariadb Versi 5 PCDS ENTRANCE & EXIT (MIY)

# 5 30 = src - Mariadb Versi 10 PCDS OPEN/EXIT (MIY)
#31 = src - Mariadb Versi 10 PCDS ENTRANCE (MIY)
#33 = src - Mariadb Versi 10 PCDS ENTRANCE & EXIT (MIY)

############### MIY BCDS ###################
# 7 40 = src - Mariadb Versi 5 BCDS OPEN/EXIT (MIY)

# 8 50 = src - Mariadb Versi 10 BCDS OPEN/EXIT (MIY)

############### DELAMETA ###################
# 6 60 = Delameta - PostgreSQL OPEN (DELAMETA)
# 10 61 = Delameta - PostgreSQL CLOSE ENTRANCE (DELAMETA)
# 11 62 = Delameta - PostgreSQL CLOSE EXIT (DELAMETA)
# 9 63 = Delameta - PostgreSQL CLOSE ENTRANCE & EXIT (DELAMETA)
############### JMTO #######################
# 99 = JMTO


configMode=os.environ['CFG_MODE']
jmtoHost=os.environ['IP_MEDIASI']
jmtoUser=os.environ['USER_MEDIASI']
jmtoPass=os.environ['PASS_MEDIASI']
jmtoDb= os.environ['DB_MEDIASI']
jmtoPort= os.environ['DB_PORT']

idGerbang= os.environ['GERBANG_SRC']
namaGerbang= os.environ['GERBANG_NAMA']
srcHost=os.environ['IP_SRC']
srcUser=os.environ['USER_SRC']
srcPass=os.environ['PASS_SRC']
srcDb=os.environ['DB_SRC']
srcPort=os.environ['PORT_SRC']

