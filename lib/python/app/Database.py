#!/opt/connecteurSQL/bin/python2.7
# We implement the JSON libraries
#import manage
import sys
import os

sys.path.append(os.environ['HOME_CONNECTEUR']+'/smartengine/libs/connector/dtobib/python/lib')
sys.path.append(os.environ['HOME_CONNECTEUR']+'/smartengine/libs/connector/dtobib/python')

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
sys.path.append( os.environ['HOME_CONNECTEUR']+"/lib/python2.7/site-packages")

#import settings
from connector import biblog, bibutils, bibtools, bibsql

import simplejson as json

from controller_library import candles
from connector.bibsql import BibSql
from Base import Base


class Database (Base):

	def __init__(self):
		pass

        # init function
        def init(self):
		try:
			# the database configuration file
			source     		=       "%s/Database/%s.json" % (resource_dir,self.conf['instance'])
			self.name		=	self.conf['instance']
			self.encode_orig	=	False
		except:
			source			=	""
			self.message.addError("011", "The connection to the database failed : %s" % e, 2)

		# We affect the configuration infos
		self.settings 	= 	candles.getJsonToArray(source)
		# We connect to the db via bibsql
		#try:
		self.db = 	bibsql.sqldb(conf=self.settings)
		#except Exception as e :
		#	self.message.addError("011", "The connection to the database failed : %s" % e, 2)
		
		self.getData_generic()

        # returns the datas from the source
        def getData(self):
		results = { }
		for query in self.conf['list_queries']:

                        if query.has_key("encode_origin"):
                                self.encode_orig = query["encode_origin"]

			try:
				self.query_name = query['name']
			except Exception as e:
				self.message.addError("022", "Imposible to find a query name : %s" % e, 2)
			if ( query['query'] ) :

				self.message.addMessage( "Query : %s executed" % query["query"] ) 

				self.current_query = query

				if ( query.has_key('mapping') ):
					self.current_mapping = query['mapping']
				else:
					self.current_mapping = None 
				try:
					if query.has_key('limit') and query['limit']:
						self.res = self.db.select(query['query'],query['limit']) 
					else:
						self.res = self.db.select(query['query'])
				except Exception as e:
					self.message.addError("013", "Impossible to execute query (%s) : %s" % (query['query'], e), 2) 

				self.treatData_generic()
				self.formatData_generic()
				results[self.query_name] = self.result
				self.result = {} 
				self.current_mapping = {}
				self.current_query = {}
			else :
				self.message.addError('012', 'Impossible to find query', 2)
		if ( self.settings['dbtype'] == 'MSSQL' ):
			self.db.restaure_prev_environ_var
		self.data = results
        # catches the datas and launch the request
        def treatData(self):
		self.message.addMessage("Treating data")
		data_tmp = []
		desc_tmp = []
		
		if self.encode_orig:
			self.res = candles.dataDecodeEncode(self.res,self.encode_orig,'ascii')

                for row in self.res:
			data_tmp.append(row)

		self.result['data'] = data_tmp
		self.result['success'] = 'true'
		self.result['total'] = len(data_tmp)

		try:
			if self.current_query.has_key('description') and self.current_query['description']:
        			for desc in self.db.description:
					row_desc_tmp = {}
					row_desc_tmp['name'] = getattr(desc,'name')
					if self.settings["dbtype"] == 'ORACLE':
						row_desc_tmp['type'] = repr( getattr(desc,'type'))
					else:
						row_desc_tmp['type'] = getattr(desc,'type')
					row_desc_tmp['display_size'] = getattr(desc,'display_size')
					row_desc_tmp['internal_size'] = getattr(desc,'internal_size')
					row_desc_tmp['precision'] = getattr(desc,'precision')
					row_desc_tmp['scale'] = getattr(desc,'scale')
					row_desc_tmp['null_ok'] = getattr(desc,'null_ok')
	
        	        		desc_tmp.append(row_desc_tmp)
	
				self.result['description'] = desc_tmp
		except Exception as e:
			self.message.addError("014", "Error during treating data : %s" % e, 2 )

		self.formatData_generic()

	def formatData(self):
		self.message.addMessage("Formating Data")
#		try:
#			if ( self.settings['dbtype'] == 'ORACLE' and self.result.has_key('data')  ):
#				data = self.result['data'] 
#				tmp_data = []
#				for record in data:
#					newrecord = {}
#					for field, value in record.items():
#						newrecord[field] = value
#					tmp_data.append(newrecord)

#				self.result['data'] = data
#		except Exception as e:
#			self.message.addError("015", "Error during formating data : %s" %e, 2)		

