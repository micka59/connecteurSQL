#!/opt/connecteurSQL/bin/python2.7
#################################################
#						#
#		MOTOR JSON			#
# or how to return datas from multipe sources	#
# in order to register them through one format	#
#						#
#################################################
import sys
import os
import simplejson as json
from app import dynamic_values
from controller_library import candles
from datetime import date, datetime, timedelta
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

class Base:

        # init function
        def __init__(self):
                pass
	
	def init_generic(self, conf, message, args, proxy):
		# we store the configuration file
		self.name	=	"no_name"
                self.conf	=	conf
		self.message	=	message
		self.args	=	args
		self.proxy	=	proxy
		self.data	=	{ }
		self.result	= 	{ }
		self.query_name =	'no_name'
		self.current_mapping	=	{}
	
		self.init()

	def init(self):
		self.getData_generic()

	def getData_generic(self):
		self.getData()

        # returns the datas from the source
        def getData(self):
                self.treatData_generic()

	def convert_to_json(self):
		pass

        # returns the datas from the source
        def getDataFile(self):
                results = { } 
                for file in self.conf['list_files']:
                        self.file = file
			if self.file.has_key("encode_origin"):
				self.encode_orig = self.file["encode_origin"]
			else:
				self.encode_orig
			try:
				self.query_name = file['name'] 
			except Exception as e:
				self.message.addError('022',  "No name found for the file query : %s" % e, 2) 
			if ( file.has_key('mapping') ):
				self.current_mapping = file['mapping']
			else:
				self.current_mapping = {}
                        if ( (file.has_key("dynamic") and file["dynamic"]) or (file.has_key("url") and file["url"]) ) :
                                if file.has_key("dynamic") and file["dynamic"]:
                                        if len(self.args) and file.has_key("param_name"):
                                                for arg in self.args:
                                                        tmp_arg = arg.split(":")
                                                        if tmp_arg[0] == file["param_name"]:
                                                                try:
                                                                	self.filename = tmp_arg[1]
									self.content = getFileContent(self.filename,self.proxy)
                                                                	self.result['data'] = self.convert_to_json()
                                                                except:
                                                                        self.message.addError('012', 'Impossible to load file defined in configuration', 2)
                                                                break
                                        else:
                                                self.message.addError('012', 'Impossible to match the call with the configuration', 2)
                                else:
                                        try:
                                        	self.filename = file['url']
		                        	self.content = candles.getFileContent(self.filename,self.encode_orig,self.proxy)
        		                        self.result['data'] = self.convert_to_json()
                                        except:
                                                self.message.addError('012', 'Impossible to load file defined in configuration', 2)

				self.content = ""
                                self.treatData_generic()
                                results[self.query_name] = self.result
                                self.result = {}
                                #self.current_mapping = {}
                                self.current_file = {}
                        else :
                                self.message.addError('012', 'Impossible to find file in configuration', 2)
			self.current_mapping = { }
                self.data = results
	
	def treatData_generic(self):
		self.treatData()
        
	# catches the datas and launch the request
        def treatData(self):
                self.message.addMessage("Treating data")
                if self.result.has_key('data') == False:
                        self.result['data'] = []
                self.result['success'] = 'true'
		if self.result['data']:
	                self.result['total'] = len(self.result['data'])
		else:
			self.result['total'] = 0

		self.formatData_generic()

	def formatData_generic(self):
		tmpdata = []
		if self.result.has_key('data') and self.result['data']:
			for result in self.result['data']:
				fields = result.keys()
				newresult = { }
				if (self.current_mapping and  self.current_mapping.has_key( 'fields_renaming' ) ):
					try:
						for fnamemapped, values in self.current_mapping['fields_renaming'].items():
							fnameorig 	=	values[0]
							copie		=	values[1]
							retour		=	values[2]
							if ( len(values) == 4 ):
								force_type = values[3]
							else:
								force_type = False
							if not fnameorig in fields:
								self.message.addError('022', 'The field %s does not exist in the result of query' % ( fnameorig ), 2 )
							else:
								if type (result[fnameorig]) is date or type(result[fnameorig]) is datetime:
									result[fnameorig] = result[fnameorig].strftime('%Y-%m-%d %H:%M:%S')
								if ( force_type ):
									if (force_type == 'int'):
										newresult[fnamemapped] = int(result[fnameorig])
									else:
										newresult[fnamemapped] = result[fnameorig]
								else:
									newresult[fnamemapped] = result[fnameorig]
								if copie:
									newresult[fnameorig] = result[fnameorig]
									
					except Exception as e:
						self.message.addError("016", "Error during mapping > renaming fields : %s" % e, 2)
				
				for field in fields:
					try:
						if type(result[field]) is date or type(result[field]) is datetime :
							result[field] = result[field].strftime('%Y-%m-%d %H:%M:%S')
						elif type(result[field]) is timedelta:
							result[field] = result[field].total_seconds()
					except Exception as e:
						self.message.addError("017", "Error during converting datetime field in string field for json export : %s" % e, 2)
					try:
						if ( self.current_mapping and self.current_mapping.has_key('prefix') ):
							newresult[self.current_mapping['prefix']+field] = result[field]
						else:
							newresult[field] = result[field]
					except Exception as e:
						self.message.addError("018", "Error during prefixing field : %s" % e, 2)

				try:
					if ( self.current_mapping and self.current_mapping.has_key('records_constants') ):
						for fname, value in self.current_mapping['records_constants'].items() :
							newresult[fname] = value
				except Exception as e:
					self.message.addError("019", "Error during adding constants records : %s" % e, 2 )
	
				try:
					if ( self.current_mapping and self.current_mapping.has_key('dynamic_values') ):
						for fname, value in self.current_mapping['dynamic_values'].items() :
							if value.has_key('function') and value['function'].has_key('name'):
								try:
									function_name		=	value['function']['name']
				        	                        newvalue                =       getattr( dynamic_values, function_name )
									newresult[fname]	=	newvalue( value, result)
								except Exception as e:
									self.message.addError("021", "Error during calling dynamic_values module for function %s : %s" % ( function_name, e), 2)
				except Exception as e:
					self.message.addError("020", "Error during calculating dynamic values : %s" % e, 2)
				try:
					if ( self.current_mapping and self.current_mapping.has_key("delete_fields") ):
						for f_delete in self.current_mapping['delete_fields']:
							if ( newresult.has_key(f_delete) ):
								newresult.pop( f_delete, None)
				except Exception as e:
					self.messsage.addError("021", "Error during deleting files : %s" % e, 2 )

				tmpdata.append(newresult)
		
		if ( len(tmpdata) > 0 ):
			self.result['data'] = tmpdata 
		self.result['mapping'] = self.current_mapping	
		self.formatData()

        # formats the datas according the requested format
        def formatData(self):
		pass

	def getResult(self):
		return self.data

	def getName(self):
		return self.name
