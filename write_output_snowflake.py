import snowflake.connector

file_path = 'file:///output/'

vol_table_name = 'Volume'
vol_format = 'UTIL.PUBLIC.CSV_FORMAT'

def create_fields(ts):
    vf = 'A INTEGER,B INTEGER,'
    for i in range(ts):
        s = 'vol' + str(i+1) + ' FLOAT'
        vf = vf + s
        if i + 1 < ts:
            vf = vf + ','
    return vf

def save_vol(db, ts):
    vol_field = create_fields(ts)
    ctx = snowflake.connector.connect(
        user='lihewang',
        password='Ui123456',
        account='eya42508.us-east-1',
        warehouse='COMPUTE_WH',
        database=db,
        schema='PUBLIC'
        )
    cs = ctx.cursor()
    cs.execute("DROP TABLE if exists " + vol_table_name)
    cs.execute("CREATE TABLE " + db + ".PUBLIC." + vol_table_name + " (" + vol_field + ")")
    cs.execute("PUT " + file_path + vol_table_name + ".csv @%" + vol_table_name)
    cs.execute("COPY INTO " + vol_table_name + " FILE_FORMAT = " + vol_format)
    one_row = cs.fetchone()
    print('upload successfully to ' + one_row[0])