#!/opt/connecteurSQL/bin/python2.7
# We implement the JSON libraries
#import manage
import sys
import os

from Base import Base

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

#import settings
import simplejson as json
from controller_library.candles import getJsonToArray

class JSON (Base):

	def __init__(self):
		pass

	def init(self):
		if self.conf.has_key('name'):
			self.name = self.conf['name']
		self.getData_generic()


        def convert_to_json(self):
                return getJsonToArray(self.filename)

        # returns the datas from the source
        def getData(self):
		self.getDataFile()        

