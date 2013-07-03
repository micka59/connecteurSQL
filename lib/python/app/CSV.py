#!/opt/connecteurSQL/bin/python2.7
# We implement the JSON libraries
#import manage
import sys
import os

global base
base = os.environ['HOME_CONNECTEUR']


global cache_dir
if (os.environ.has_key( 'HOME_CONNECTEUR_CACHE_DIR' ) ):
	cache_dir = os.environ['HOME_CONNECTEUR_CACHE_DIR'] 
else:
	cache_dir = ("%s/cache" % (base) ) 

global resource_dir
if ( os.environ.has_key('HOME_CONNECTEUR_RESOURCE_DIR') ):
	resource_dir = os.environ['HOME_CONNECTEUR_RESOURCE_DIR']
else:
	resource_dir = ("%s/resources" % (base) )
global log_dir
if ( os.environ.has_key('HOME_CONNECTEUR_LOG_DIR' ) ) :
	log_dir = os.environ['HOME_CONNECTEUR_LOG_DIR'] 
else:
	log_dir = ( "%s/log" % (base ) )

sys.path.append( "%s" % ( base ) )
sys.path.append( os.environ['HOME_CONNECTEUR']+"/lib/python")
sys.path.append( os.environ['HOME_CONNECTEUR']+"/lib/python2.7")

#import settings
import simplejson as json
import csv
from Base import Base

class CSV (Base):

	def __init__(self):
		pass

	def convert_to_json(self):
		if self.file.has_key('header') and self.file['header']:
			fieldnames = self.file["header"]
	                try:
        	                csv_reader = csv.DictReader(self.content,fieldnames,skipinitialspace=True)
				csv_reader.next()
                	except:
                        	self.message.addError('012', 'CSV Data retrieval with header failure', 2)

		else:
			try:
	        		csv_reader = csv.DictReader(self.content,skipinitialspace=True)
        		except:
				self.message.addError('012', 'CSV Data retrieval without header failure', 2)
		try:
			data = json.loads(json.dumps([r for r in csv_reader]))
        	except:
			self.message.addError('012', 'Data structuration from CSV to Python failure', 2)
        	return data

	def init(self):
		if self.conf.has_key('name'):
			self.name = self.conf['name']
		self.getData_generic()

        # returns the datas from the source
        def getData(self):
		self.getDataFile()
