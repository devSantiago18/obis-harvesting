import cx_Oracle

dsn_connection = cx_Oracle.makedsn('192.168.3.70', port='1521', sid='SCI')
connection = cx_Oracle.connect(user='CURADOR', password='paque', dsn=dsn_connection,  nencoding = "UTF-8")

cursor = connection.cursor()

sql = """SELECT NOMBRE FROM CMDWC_VARIABLES"""

cursor.execute('select * from cmdwc_variables')


for row in cursor:
    print(row)
    
print("#######")

print(cursor)
connection.close()