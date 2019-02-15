import cx_Oracle
import time 

print ("About to sleep")
time.sleep(30)

dsnStr = cx_Oracle.makedsn("oradb", 1521, "xe")

print ("No mo sleeping brah")
connection = cx_Oracle.connect(user="hr", password="hr", dsn=dsnStr)
cursor = connection.cursor()
cursor.execute("select sysdate from dual")
today, = cursor.fetchone()

print("The current date is", today)