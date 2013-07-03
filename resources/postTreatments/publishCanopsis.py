#!/opt/connecteurSQL/bin/python2.7
from camqp import camqp
from copy import deepcopy
import json, os
from controller_library.candles import getJsonToArray
from datetime import datetime

import biblog
import bibutils
import bibtools
import bibsql
global base
base = os.environ['HOME_CONNECTEUR']
import sys
print sys.path
def formatRoutingKey ( record ) :
	connector	=	"ExtDataSource"
	connector_name	=	"ExtDataSource_1"
	event_type	=	"check"
	source_type	=	"resource"
	component	=	record['component']
	resource	=	record['resource']
	
	return "%s.%s.%s.%s.%s.%s" % ( connector, connector_name, event_type, source_type, component, resource) 

def build_arbo ( array, value, recursion=0):
	resultat = {}
	if ( len(array) == 1):
		indice = array[0]
		resultat[indice] = value
		return resultat
	elif ( recursion < 3) :
		recursion = recursion + 1
		indice = array[-1]
		resultat[indice] = value
		return build_arbo( array[:-1], resultat, recursion )

def combine(a, b):
	'''recursively merges dict's. not just simple a['key'] = b['key'], if
	   both a and bhave a key who's value is a dict then dict_merge is called
	   on both values and the result stored in the returned dictionary.'''
	if not isinstance(b, dict):
        	return b
	result = deepcopy(a)
	for k, v in b.iteritems():
        	if k in result and isinstance(result[k], dict):
                	result[k] = combine(result[k], v)
        	else:
            		result[k] = deepcopy(v)
	return result 

def recordFailedBusPublication(records):
	if len(records):
		canop_file           =       "%s/cache/tmp/canopsis.json" % (base)
		tmp_content = getJsonToArray(canop_file)
	        if tmp_content == False:
        	        tmp_content = []

		for curr_record in records:
			curr_key =  curr_record.keys()[0]
			tmp_content.append(curr_record)

		f = open(canop_file,"w")

		json.dump(tmp_content,f)

def recordProcessedBusPublication(records):
	if len(records):		
                canop_file           =       "%s/cache/tmp/canopsis.json" % (base)
                f = open(canop_file,"w")
                json.dump(records,f)


def publishInCanopsis( resultats, settings, params ):
	print os.environ
	#you can add host, port, userid, password, virtualhost, exchange_name, read_conf_file, auto_connect, on_ready
	if ( params.has_key('amqp_host') ):
		amqp_host = params['amqp_host']
	else:
		amqp_host = "localhost"
	if ( params.has_key('amqp_port') ):
		amqp_port = params['amqp_port']
	else:
		amqp_port = 5672
	if ( params.has_key('amqp_userid') ):
		amqp_userid = params['amqp_userid']
	else:
		amqp_userid = "guest"
	if ( params.has_key('amqp_password') ):
		amqp_password = params['amqp_password']
	else:
		amqp_password = "guest"
	if ( params.has_key('amqp_virtualhost') ) :
		amqp_virtualhost = params['amqp_virtualhost']
	else:
		amqp_virtualhost = "canopsis"
	if ( params.has_key('amqp_exchange') ):
		amqp_exchange = params['amqp_exchange']
	else:
		amqp_exchange = "canopsis.events"
	amqp_bus = camqp(host=amqp_host, port=amqp_port, userid=amqp_userid, password=amqp_password, virtual_host=amqp_virtualhost, exchange_name="canopsis.events" )
	record_fail = []
	num_del_record = 0
	if amqp_bus:
		# We treat the waiting publications
		canop_content = False
		try:
			canop_file           =       "%s/cache/tmp/canopsis.json" % (base)
			canop_content = getJsonToArray(canop_file)
		except:
			pass
		if canop_content:
			for i in range(len(canop_content)):
				record = canop_content[i]
				curr_key = record.keys()[0]
                        	inserted = True#amqp_bus.publish(json.dumps(record[curr_key]),curr_key)
                                if ( inserted ):
                                        print "INSERTED !"
					canop_content.pop(i)
					num_del_record += 1
                               	else:
                                       	print "NOT INSERTED!"
		
		if num_del_record > 0:
			recordProcessedBusPublication(canop_content)		

		# We now send the new publications
		newresultat = {}
		for source,query in resultats.items():
			for q_name,records in query.items():
				newdata = [] 
				for record in records['data']:
					newrecord = {}
					tmplevel = {}
					remove = [] 
					for field, value in record.items():
						if 'metric.' in field:
							if  record.has_key( field.split('.')[1] ):
								print field.split('.')[0]
								print record[field.split('.')[1]]
								print field.split('.')[2]
								record[ field.split('.')[0]+"."+record[field.split('.')[1]]+"."+field.split('.')[2] ] = value
								remove.append(field)

					for field in remove:
						del record[field]

					for field, value in record.items():
						if "." in field:
							tmplevel = combine( tmplevel,  build_arbo( field.split('.'), value) )
						else:
							newrecord[field] = value
						
					metric = []
					if ( tmplevel.has_key('metric') ):
						for metricname, value in tmplevel['metric'].items():
							value["metric"] = metricname
							if ( 'value' in value.keys() ):
								metric.append(value)
						newrecord['perf_data_array'] = metric
						del (tmplevel['metric'])
					if ( len(tmplevel.keys()) > 0 ):
						newrecord = combine(newrecord, tmplevel)
					newdata.append(newrecord)
					inserted = True  #amqp_bus.publish(json.dumps(newrecord), formatRoutingKey(record) )
					print "insert record : "+formatRoutingKey(record)
					if ( inserted ):
						print json.dumps(newrecord, indent=4 )
						print "INSERTED !"
					else:
						record_fail.append({formatRoutingKey(record) : newrecord})
						print "NOT INSERTED!"
				records['data'] =  newdata
	else:
		# We report the records to a "pile" file to be executed when published
                newresultat = {}
                for source,tableau in resultats.items():
                        for records in tableau:
                                newdata = []
                                for record in records['data']:
                                        newrecord = {}
                                        tmplevel = {}
					remove = []
					for field, value in record.items():
						if 'metric.' in field:
							if  record.has_key( field.split('.')[1] ):
								record[ str(field.split('.')[0])+"."+str(record[field.split('.')[1]])+"."+str(field.split('.')[2]) ] = value
								remove.append(field)

					for field in remove:
						del record[field]

                                        for field, value in record.items():
                                                if "." in field:
                                                        tmplevel = combine( tmplevel,  build_arbo( field.split('.'), value) )
                                                else:
                                                        newrecord[field] = value

                                        metric = []
                                        if ( tmplevel.has_key('metric') ):
                                                for metricname, value in tmplevel['metric'].items():
                                                        value["metric"] = metricname
                                                        if ( 'value' in value.keys() ):
                                                                metric.append(value)
                                                newrecord['perf_data_array'] = metric
                                                del (tmplevel['metric'])
                                        if ( len(tmplevel.keys()) > 0 ):
                                                newrecord = combine(newrecord, tmplevel)

                                        newdata.append(newrecord)
                                        
					# We directly go to the temp file
					record_fail.append({formatRoutingKey(record) : newrecord})
                                        print "NOT INSERTED!"
                                records['data'] =  newdata

	recordFailedBusPublication(record_fail)

	return newdata

def updateTable ( resultats, settings, params ):
	instance = None
	if ( params.has_key('instance') ):
		instance = params['instance']
	table = None 
	if ( params.has_key('table') ):
		table = params['table']
	fieldid = None
	if ( params.has_key('fieldid') ):
		fieldid = params['fieldid']
	idstk = None
	if ( params.has_key('idstk') ):
		idstk = params['idstk'] 

	source		= 	"%s/resources/Database/%s.json" % (base,instance)
	settings 	= 	getJsonToArray(source)
	db		= 	bibsql.sqldb(conf=settings)	
	for record in resultats:
		fieldidvalue = record[fieldid]
		dtsync = datetime.now()
		str1 = "UPDATE %s set dtsync=NOW() where %s=%s and idstk=%s" % (table, fieldid, str(fieldidvalue), str(idstk) ) #UPDATE "+table+" set dtsync='"+dtsync+"' where "+fieldid+"="+fieldidvalue
		#r = db.update(str1)
		#db.cmd('COMMIT')

	return resultats
				

	
