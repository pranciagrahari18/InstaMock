import datetime
import os

class logger():
	def __init__(self,name):
		os.system("mkdir -p log")
		self.log_file=open("log/%s_%s"%(name,datetime.datetime.now()),"w")
		
	def info(self,s):
		self.log_file.write(str(datetime.datetime.now())+"  :   "+s+"\n")