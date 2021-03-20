import os
import time

while 1:
	comm='cmd /k "cd /d D:\get_isbn_ssid_pack && python .\get_isbn_ssid_pack.py"'
	a=os.system(comm)
	if a!=0:
		time.sleep(180)
		continue