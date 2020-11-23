import os
import time
import sys

import subprocess

while 1:
	get_pack_path=r"D:\get_isbn_ssid_pack\get_isbn_ssid_pack.py"
	# comm='cmd /k "cd /d D:\get_isbn_ssid_pack && python .\get_isbn_ssid_pack.py"'
	# comm='cmd /k "cd /d D:\get_isbn_ssid_pack && python .\kb.py"'
	# p=subprocess.Popen([sys.executable,get_pack_path],shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	p=subprocess.Popen([sys.executable,get_pack_path],shell=True)
	ret=p.wait()

	if ret!=0:
		print("crash!")
		print("sleep for 3min...")
		time.sleep(180)
		continue

	# print(ret)
	# break

