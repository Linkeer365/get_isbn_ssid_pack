import os
import time
import sys

import subprocess

cnt=0
max_cnt=10**9
while cnt<=max_cnt:
	get_pack_path=r"D:\get_isbn_ssid_pack\get_isbn_ssid_pack.py"
	try:
	# comm='cmd /k "cd /d D:\get_isbn_ssid_pack && python .\get_isbn_ssid_pack.py"'
	# comm='cmd /k "cd /d D:\get_isbn_ssid_pack && python .\kb.py"'
	# p=subprocess.Popen([sys.executable,get_pack_path],shell=True,stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p=subprocess.run([sys.executable,get_pack_path],shell=True,check=True)
		p.wait()
		cnt+=1
		# time.sleep(2)
		# ret=subprocess.check_output(["python",get_pack_path],stderr=subprocess.STDOUT,shell=True)
	except subprocess.CalledProcessError as e:
		print("output:",e.output)
		print("returncode",e.returncode)

		print("gg.")
		print("crash!")
		print("sleep for 45s...")
		time.sleep(45)
		continue

	# print(ret)

	# if ret!=0:


	# print(ret)
	# break

