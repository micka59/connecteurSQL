#!/opt/connecteurSQL/bin/python2.7

# System Libs
import sys
import os
from time import sleep


# We define our vars
global base
if not os.environ.has_key('HOME_CONNECTEUR') or os.environ['HOME_CONNECTEUR'] == '':
	dir_path = os.path.dirname(os.path.abspath(__file__))
	home_connecteur = "%s/../../" % dir_path
	os.environ['HOME_CONNECTEUR'] = home_connecteur

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

#We import the right library path
sys.path.append( "%s" % ( base ) )
sys.path.append( os.environ['HOME_CONNECTEUR']+"/lib/python")
sys.path.append( os.environ['HOME_CONNECTEUR']+"/lib/python2.7")
sys.path.append( "%s/postTreatments" % ( resource_dir ) )

#we delete the false librayr path
try:
	sys.path.remove("/opt/connecteurSQL/smartengine")
except: 
	pass
try:
	sys.path.remove("/opt/connecteurSQL/lib/python")
except: 
	pass
try:
	sys.path.remove("/opt/connecteurSQL/lib/python2.7")
except:
	pass
try:
	sys.path.remove("/opt/connecteurSQL/smartengine/resources/postTreatments")
except:
	pass


# General Libs
try:
	import simplejson as json
except:
	import json

from os import listdir
from os.path import isfile, join
from os import environ

import re
import glob
import types
import inspect
import time

# Export CSV
import csv

# Lib for Messages
from app.Message import Message
global message
message = Message( log_dir )

# Export XML
from xml.dom import minidom

# Other dev
import candles

# Conf Message


def getAllExtDataType( ) :
	"""
	We get all the type we may have, return an array with all type available
	"""
	result 		=	[]	
	print base
	classdir	= 	"%s/lib/python/app/" % ( base )
	for f in listdir( classdir ):
		if isfile( join( classdir, f) ) and re.search("^.+\.py$", f ) and f[:-3] != 'Base' and f[:-3] != '__init__' and f[:-3] != 'Error':
			result.append(f[:-3] )
	
	return result

def dataOutput(data,output,source):
	"""
	Format the results into required output
	"""
	if data:
		"""
		INIT
		"""
		if output and output.has_key('type'):
			type = output['type']
		else:
			type = 'JSON'

		if output and output.has_key('encoding'):
			encoding = output['encoding']
		else:
			encoding = 'utf-8'

		data = candles.dataDecodeEncode(data, 'ascii', encoding)

		"""
		TREATMENTS
		"""
		if type == 'Python':
			return data

                elif type == 'JSON' :
                        try:
                                return json.dumps(data, use_decimal=True, indent=4)
                        except Exception  as e :
                                message.addError("010", "Impossible to dump python object as json %s" % e, 2 )
                                return False

		elif type in ['CSV','XML','SQL']:
			
			directory = "%s/%s" % (cache_dir,source)
			dir_files = []

			for name, tmp_data in data.items():
				cmpt_filename = {}
				for query_name, line in tmp_data.items():
			
	
					if line.has_key('data'):
						line_data = line['data']
						# We determine the filename of the output - 1 output per query
						filename = "%s/%s.%s" % (directory,query_name,type.lower())
						# We already have a filename similar for another query
						if filename in dir_files:
							if cmpt_filename.has_key(query_name) == False:
								cmpt_filename[query_name] = 0
							cmpt_filename[query_name] += 1
							filename = "%s/%s_%d.%s" % (directory,query_name,cmpt_filename[query_name],type.lower())
						else:
							dir_files.append(filename)
						
	
						if type == 'CSV':

							# Default value of delimiter
							delimiter_val = ','
							# If the delimiter is paramed
                                        	        if output.has_key('params') and output['params'].has_key('delimiter'):
                                                	        delimiter_val = output['params']['delimiter']
							# We writer the content in CSV
							try:
								out = csv.writer(open(filename,"w"), delimiter=delimiter_val, quoting=csv.QUOTE_ALL)
								out.writerow(line_data[0].keys())
								for row in line_data:
									out.writerow(row)
							except Exception as e:
								message.addError("010", "Impossible to dump python object as csv %s" % e, 2 )
					
						elif type == 'XML':
							# Default value of root tag element
        	                                        root_val = 'root'
							# If paramed
                	                                if output.has_key('params') and output['params'].has_key('root_name'):
                        	                                root_val = output['params']['root_name']
							# Default value of item tag element
                                	                elt_val = 'element'
							# If paramed
                                        	        if output.has_key('params') and output['params'].has_key('line_name'):
                                                	        elt_val = output['params']['line_name']
							# We write the XML in the file
	                                                try:

        	                                                dict            =       {root_val: line_data}
                	                                        result          =       candles.dict2xml(dict,True,elt_val=elt_val)
								candles.writeInFile(result,filename)

                                        	        except Exception as e:
                                                	        message.addError("010", "Impossible to dump python object as xml %s" % e, 2 )
					
						elif type == 'SQL':
							# Default value of the table (insert into ***)
        	                                        tablename_val = '__tablename__'
							# If paramed
                	                                if output.has_key('params') and output['params'].has_key('tables'):
								# We parse to match the name of query to the conf data_output
                        	                                for inst,tablename in output['params']['tables'].iteritems():
                                	                                if inst == queryname:
                                        	                                tablename_val = tablename
                                                	                        break
							# We create the query and write it in file
	                                                try:
        	                                                iteration = 0
                	                                        request = ""
                        	                                for elt in line_data:
                                	                                separator = ','
                                        	                        if iteration == 0:
                                                	                        request += "insert into %s (%s) values " % (tablename_val,(','.join(elt.keys())))
                                                        	        if iteration == (len(line_data)-1):
                                                                	        separator = ';'
	                                                                request += '("%s")%s' % ('","'.join(elt.values()),separator)
        	                                                        iteration += 1
								
								candles.writeInFile(request,filename)

                                        	        except Exception as e:
                                                	        message.addError("010", "Impossible to dump python object as sql %s" % e, 2 )

			# If only one filename we return the path of the file
			if len(data) == 1:
				return filename
			# If several, we return the directory
			else:
				return directory

		else:
			# We try to dump in Json if no other type is found
			try:
				return json.dumps(data, use_decimal=True, indent=4)
			except Exception as e:
				message.addError("010", "Impossible to dump python object as json %s" % e, 2 )
				return False
	else:
		return False

def processRefresh(source,args=False):
	"""
	handles multi-call of the same sources (one processing the other fifo)
	lauches treatment of the source
	"""	
	message.addMessage("Order receveid to deal with %s source" % source )
	is_lock = False
	
	# We parse all the cache files having the source name
        parse           =       "%s/tmp/%s_[0-9]*.lock" % (cache_dir,source)
	try:
	        list            =       glob.glob(parse)
	except Exception as e:
		message.addError('002', "Impossible to list lock file", 2)

        ts              =       int(time.time())

	# If we have lock files
	if len(list):
		for lock in list:
			# We retrieve the timestamp of the lock file
			ts_lock 	= 	candles.getTimestampFile(lock)
			# We don't consider lock files older than an hour
			if (ts - 3600) > ts_lock :
		                if os.path.isfile(lock):
                	        	# We remove the lock file
                        		os.remove(lock)
			# If we have a lock file younger than an hour we consider the source running
			else: 
				is_lock = 	True

	# The filename of the current lock file
	curr_lock 	= 	"%s/tmp/%s_%s.lock" % (cache_dir,source,ts)

	# We create an empty file to tell we are processing the source
	candles.writeInFile("",curr_lock)

        # We init our vars
	ts 		= 	int(time.time())
        source_file     =       "%s/configuration/%s.json" % (resource_dir,source)

        # We get the configuration file
        data            =       candles.getJsonToArray(file=source_file)

	# We set that we want to create results from the source VS we have datas fresh enough stored
        write_file	=	True

	# We retrieve the configuration datas
        if data:

		# init output values
		if data.has_key('data_output')==False:
			dataoutput 		= 	{ 'type': 'json', 'encoding': 'utf-8' }
			data['data_output'] 	= 	dataoutput
		# default values (for dumps)
		default_output 			= 	{}
                default_output['type'] 		= 	"json"
                default_output['encoding'] 	= 	"utf-8"

                # We parse all the cache files having the source name
                parse           		=       "%s/sources/%s_[0-9]*.json" % (cache_dir,source)
		try:
	                list            	=       glob.glob(parse)
		except Exception as e:
			message.addError('004', 'Impossible to list file in sources directory (%s) : %s' % (parse, e) )
		
                if len(list):
			# Current timestamp
			ts_curr 		= 	int(time.time())

			# We parse the dump files of the source
                        for i in range(len(list)):
				# We retrieve their timestamp
				ts_val 		= 	candles.getTimestampFile(list[i])
				# We check if there is a refresh time parametered VS we always process the queries
				if data.has_key('refresh'):
					# If the dump file is too old, we delete it
					if (ts_val + data['refresh']) < ts_curr:
						try:
							os.remove(list[i])
						except Exception as e:
							message.addError('005', "Impossible to remove source file (%s) : %s" % ( list[i], e) , 2)
					# The dump file is recent enough, we can just retrieve the datas from it
					else:
						try:
							message.addMessage("Found a fresh data files for this source, system will return it")
							response = candles.getJsonToArray(file=list[i])
							write_file = False
						except Exception as e:
							message.addError('006', "Impossible to read existing file (%s) : %s" % (list[i], e) , 2)
				# We always process the queries, no need to keep dump file
				else:
					try:
						os.remove(list[i])
					except Exception as e:
						message.addError('005', "Impossible to remove source file (%s) : %s" % ( list[i], e) , 2)

		# We prepare the source directory for output purposes
        	directory = "%s/%s" % (cache_dir,source)
	        candles.make_sure_path_exists(directory)

		# We want to save the datas
		if write_file:
			# We lauch the process and retrieve a py dictionnary
			response = processMethod(data,source,is_lock,args)

		# the py dictionnary is converted in utf-8 json for dumps
	        data_json = dataOutput(response,default_output,source)

		# if it succeeded and we have a json output
		if data_json:
			if write_file:
				candles.writeInFile(data_json,("%s/sources/%s_%s.json" % (cache_dir,source,ts))) 
			# if a treatment is already happing
			if is_lock:
				# We create a dump at the current timestamp
				filename = "%s/combine/%s_%s.json" % (cache_dir,source,ts)
				candles.writeInFile(data_json,filename)
			elif ( os.path.isfile( "%s/combine/%s_%s.json" % (cache_dir,source,ts) ) ):
				os.remove("%s/combine/%s_%s.json" % (cache_dir,source,ts) ) 

			# We create a dump having the sourcename in the source folder for an easy retrieve
			filename = "%s/%s/%s.json" % (cache_dir,source,source)
        	        candles.writeInFile(data_json,filename)
		
		if os.path.isfile(curr_lock):
	        	# We remove the lock file
		        os.remove(curr_lock)
        
		# the real output as asked. Could return straight json or file/directory if xml,csv,sql
		return dataOutput(response,data['data_output'],source)
	else:
		if os.path.isfile(curr_lock):
	                # We remove the lock file
        	        os.remove(curr_lock)

		return False
	
#merge data sources
def merge_data_sources( merge, result ):
	common_fields = []
	res_to_merge = { } 
	for s_name in merge:
		for source_name in result.keys():
			for s_name2 in result[source_name].keys():
				if ( s_name == s_name2):
					res_to_merge[s_name] = result[source_name][s_name]['data']
					if ( len(common_fields) == 0 ):
						common_fields = result[source_name][s_name]['data'][0].keys()
					else:
						record = result[source_name][s_name]['data'][0] 
						common_fields_tmp = []
						for common_f in common_fields:
							if record.has_key(common_f):
								common_fields_tmp.append(common_f)
						common_fields = common_fields_tmp
	resultat = []
	for s_name, records in res_to_merge.iteritems():
		for record in records:
			fields = record.keys()
			fields_to_delete = list_difference(fields, common_fields)
			for field in fields_to_delete:
				record.pop(field, None)
				to_add = True
				for record2 in resultat:
					fields_diff = set( record.keys() ) - set( record2.keys() )
					values_diff = set( record.values() ) - set( record2.values() ) 
					if ( len(fields_diff) == 0 and  len(values_diff) == 0 ):
						to_add = False
						break
				if to_add:
					resultat.append(record)


	ret = { 'total': len(resultat), 'success': True, 'data': resultat}
	
	title = '_'.join( res_to_merge.keys() )
	ret = { title : ret }
	return { 'merge' : ret }
		
		
	
def list_difference(list1, list2):
	"""
	uses list1 as the reference, returns list of items not in list2
	"""
	diff_list = []
	for item in list1:
        	if not item in list2:
	            diff_list.append(item)
	return diff_list
		
								
		
# Opens the configuration file and lauch treatments
def processMethod(data,s_name, is_lock=False,args=False):
	"""
	retrieve attempted datas defined in the source configuration
	"""
	type_list 	= 	getAllExtDataType()		
	results 	= 	{}

	# We retrieve the proxy configuration
	if data and data.has_key('proxy'):
		proxy 	= 	data['proxy']
	else:
		proxy 	=	False

	# 1. Observe the type of source (Database, File, ...)
	if data and data['data_sources']:
		# We want to process each source defined in the configuration
		for source in data['data_sources']:
	
			message.addMessage( "Get data from source" )
	
			#If the source is well enough parametered
			if source['type'] != None and source['type'] in type_list:
				#try:
					# We init the class dynamically called using source-type
					module 		=	__import__( "app" )
					item		=	getattr(module, source['type'])()
					
					# We lauch our own init fct with our params
					item.init_generic( source, message, args, proxy ) 
					
					# We retrieve the results of all queries
					results[item.getName()] = item.getResult()

				#except Exception as e:
				#	message.addError('008', "Impossible to load dynamically module (%s) and class (%s) : %s" % ( "app", source["type"], e) , 2 )
			else:
				message.addError('009', "impossible to find an existing source type (%s). verify the value of type in %s/configuration/%s.json" % (source['type'], resource_dir, source), 2 )

		# If we want to merge the datas retrieved		
		if data.has_key('merge'):
			result 		= 	merge_data_sources(data['merge'], results)

		# if the source is locked we wait 3 seconds and we rechecked the lock value
		if ( is_lock ):
			# We parse all the cache files having the source name
		        parse           =       "%s/tmp/%s_*.lock" % (cache_dir,source)
			try:
	        		list            =       glob.glob(parse)
			except Exception as e:
				message.addError('002', "Impossible to list lock file", 2)

		        ts              =       int(time.time())
			is_lock==False
			sleep(3)
			# If we have lock files
			if len(list):
				for lock in list:
					# We retrieve the timestamp of the lock file
					ts_lock 	= 	candles.getTimestampFile(lock)
					# We don't consider lock files older than an hour
					if (ts - 3600) > ts_lock :
		                		if os.path.isfile(lock):
		                	        	# We remove the lock file
                		        		os.remove(lock)
					# If we have a lock file younger than an hour we consider the source running
					else: 
						is_lock = 	True
			
		# If we have post-treatments defined
		if data.has_key('post_treatments') and data['post_treatments'] and is_lock==False:
                	# We parse all the cache files having the source name
                	parse           =       "%s/combine/%s_*.json" % (cache_dir,s_name)
			list		=	False
			try:
                		list	=       glob.glob(parse)
			except Exception as e:
				message.addError('002', "Impossible to list json file", 2)
                		
			# We check if we have other source files
			if list and len(list) > 0:
				# We parse the list of other source files
				for i in range(0, len(list)):
					# We retrieve the timestamp of the source file
					ts_val 			= 	candles.getTimestampFile(list[i])
					# We check if the lock file associated with the source file exists
					# We retrieve the old datas
					tmp_response 	= 	candles.getJsonToArray(file=list[i])
					# We merge data and tmp_response
					results 	= 	candles.combine(results,tmp_response)
					# We remove the lock file
					os.remove(list[i])
				# If we have datas
				if results.has_key('data'):
					results['total'] 	= 	len(results['data'])
		
			# We process all post-treatments
			for post in data['post_treatments']:
				# We check if there are params
				if not isinstance( post, basestring):
					mypost 			= 	post.keys()[0]
					params 			= 	post.values()[0]
				else:
					mypost 			= 	post
					params 			= 	None
				message.addMessage("Post Treatments %s called" % mypost)
				# First arg is file, second is function
				file 				=	mypost.split('.')[0]
				function 			= 	mypost.split('.')[1]
				# We try to process the treatment
				try:		
					module_post 		=	__import__( file )
					results			=	getattr(module_post, function )( results, data, params )
				except Exception as e:
					message.addError('008', "Impossible to load dynamically module (%s) and class (%s) : %s" % ( file, function, e), 2 )

		return results
	else:
		return False

# Returns all the methods available
def allMethods():
	"""
		Send all the available methods
		aka. all the methods in the resources/configuration folder
	"""
	message.addMessage('Request for getting all the sources available')
	# We init our vars
	sources 	= 	[ ]

	# We parse all the source files
	parse 		= 	resource_dir+"/configuration/*.json"
	try:
		list 		= 	glob.glob(parse)
	except Exception as e:
		message.addError('001', "Impossible to list data sources : %s" % e, 2 )

	if list and len(list) > 0:
		for i in range(len(list)):
		
	        	# We get the configuration files datas
        		data = candles.getJsonToArray(file=list[i])

        		# We make sure it went well
        		if  data:
                		# The name of the file aka the source
				filename = os.path.splitext(os.path.basename(list[i]))[0]
				# Default values
				if data.has_key('name'):
					name = data['name']
				else:
					name = 'N/A'
				if data.has_key('description'):
					desc = data['description']
				else:
					desc = 'N/A'
				# We create a pre-formatted array to catch the datas
	             		sources.append(dict(
						id			=	i,			# for canopsis
						source			=	filename,		# the filename
						source_name		=	name,			# to show in canopsis
						source_description	=	desc			# to show in canopsis
					))
	
		# The datas in a Python Array
		data_file 	= 	dict(total=len(list), success=True, data=sources)
	else:
		data_file 	= 	dict(total=0, success=False)
	# We return the structured JSON
	return json.dumps( data_file, indent=4 )
