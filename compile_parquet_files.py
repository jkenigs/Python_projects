"""
Creamos este codigo para compilar los lotes de parquet en 3 grandes archivos:
Uno para cada marca y uno para el control
Hicimos esto porque teniamos carpetas con lotes de descargas con archivos con igual nombre
entre carpetas, y nos fue mas facil separar las marcas de esta manera
"""

import os
import pyarrow.parquet as pq
import pyarrow as pa

#Funcion para levantar los archivos
def get_parquet(directory, brand):
    # first_time = True
    files = os.listdir(directory)
    df = pd.DataFrame()
    for loFile in files:
        if brand in loFile:
            # print(loFile)
            dir_file = str(directory + "/" + loFile)
            lodf = pq.read_pandas(dir_file).to_pandas()
            df = df.append(lodf)
            """
            if first_time:
                #print(dir_file)
                df = pq.read_pandas(dir_file).to_pandas()
                first_time = False
            else:
                lodf = pq.read_pandas(dir_file).to_pandas()
                df = df.append(lodf)
            #df['brand'] = directory
            """
        # else:
        #    df = pd.DataFrame()
    df['brand'] = brand
    return df

#busqueda de todos las carpetas con archivos parquet
first_time = Truex
dirs = [x for x in os.listdir(".") if "." not in x and "Scripts" not in x and not in "final_tweets"]

xboxdf = pd.DataFrame()
psdf = pd.DataFrame()
controldf = pd.DataFrame()

#Ejecutamos la funcion para todos los directorios
for lodir in dirs:
    print(lodir)
    xboxdf = xboxdf.append(get_parquet(lodir, 'xbox'))
    psdf = psdf.append(get_parquet(lodir, 'playstation'))
    controldf = controldf.append(get_parquet(lodir, 'control'))


###### Guardamos los 3 archivos parquet ############

#control
table = pa.Table.from_pandas(controldf)
pqdf = pq.write_table(table, 'final_tweets/control/tweets_control.parquet')

#xbox
table = pa.Table.from_pandas(xboxdf)
pqdf = pq.write_table(table, 'final_tweets/tweets_xbox.parquet')

#playstation
table = pa.Table.from_pandas(psdf)
pqdf = pq.write_table(table, 'final_tweets/tweets_playstation.parquet')


#END