import snowflake.connector

file_path = 'file:///output/'

vol_table_name = 'Volume'
vol_field = 'A INTEGER,B INTEGER,vol1 FLOAT,vol2 FLOAT,vol3 FLOAT,vol4 FLOAT,vol5 FLOAT,vol6 FLOAT,vol7 FLOAT,vol8 FLOAT,vol9 FLOAT,vol10 FLOAT,' \
'vol11 FLOAT,vol12 FLOAT,vol13 FLOAT,vol14 FLOAT,vol15 FLOAT,vol16 FLOAT,vol17 FLOAT,vol18 FLOAT,vol19 FLOAT,vol20 FLOAT,' \
'vol21 FLOAT,vol22 FLOAT,vol23 FLOAT,vol24 FLOAT,vol25 FLOAT,vol26 FLOAT,vol27 FLOAT,vol28 FLOAT,vol29 FLOAT,vol30 FLOAT,' \
'vol31 FLOAT,vol32 FLOAT,vol33 FLOAT,vol34 FLOAT,vol35 FLOAT,vol36 FLOAT,vol37 FLOAT,vol38 FLOAT,vol39 FLOAT,vol40 FLOAT,' \
'vol41 FLOAT,vol42 FLOAT,vol43 FLOAT,vol44 FLOAT,vol45 FLOAT,vol46 FLOAT,vol47 FLOAT,vol48 FLOAT,vol49 FLOAT,vol50 FLOAT,' \
'vol51 FLOAT,vol52 FLOAT,vol53 FLOAT,vol54 FLOAT,vol55 FLOAT,vol56 FLOAT,vol57 FLOAT,vol58 FLOAT,vol59 FLOAT,vol60 FLOAT,' \
'vol61 FLOAT,vol62 FLOAT,vol63 FLOAT,vol64 FLOAT,vol65 FLOAT,vol66 FLOAT,vol67 FLOAT,vol68 FLOAT,vol69 FLOAT,vol70 FLOAT,' \
'vol71 FLOAT,vol72 FLOAT,vol73 FLOAT,vol74 FLOAT,vol75 FLOAT,vol76 FLOAT,vol77 FLOAT,vol78 FLOAT,vol79 FLOAT,vol80 FLOAT,' \
'vol81 FLOAT,vol82 FLOAT,vol83 FLOAT,vol84 FLOAT,vol85 FLOAT,vol86 FLOAT,vol87 FLOAT,vol88 FLOAT,vol89 FLOAT,vol90 FLOAT,' \
'vol91 FLOAT,vol92 FLOAT,vol93 FLOAT,vol94 FLOAT,vol95 FLOAT,vol96 FLOAT'
vol_format = 'UTIL.PUBLIC.CSV_FORMAT'

def save_vol():
    ctx = snowflake.connector.connect(
        user='lihewang',
        password='******',
        account='eya42508.us-east-1',
        warehouse='COMPUTE_WH',
        database='TEST',
        schema='PUBLIC'
        )
    cs = ctx.cursor()
    cs.execute("DROP TABLE if exists " + vol_table_name)
    cs.execute("CREATE TABLE TEST.PUBLIC." + vol_table_name + " (" + vol_field + ")")
    cs.execute("PUT " + file_path + vol_table_name + ".csv @%" + vol_table_name)
    cs.execute("COPY INTO " + vol_table_name + " FILE_FORMAT = " + vol_format)
    one_row = cs.fetchone()
    print('upload successfully to ' + one_row[0])
