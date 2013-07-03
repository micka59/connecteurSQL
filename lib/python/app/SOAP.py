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
sys.path.append( os.environ['HOME_CONNECTEUR']+"/lib/python2.7/site-packages")
sys.path.append( os.environ['HOME_CONNECTEUR']+"/lib/python2.7/site-packages/suds-0.4-py2.7.egg")
#import settings
import xml.etree.cElementTree as et
from suds.client import Client
from suds.plugin import MessagePlugin
from suds.transport.http import HttpAuthenticated
from controller_library import candles

import logging

from Base import Base
from XML import XML


class UnicodeFilter(MessagePlugin):
    def received(self, context):
        decoded = context.reply.decode('utf-8', errors='ignore')
        reencoded = decoded.encode('utf-8')
        context.reply = reencoded


class SOAP (Base):

	def __init__(self):
		pass

	def convert_to_json(self):
		data = False
		url  = False
		#try:
		params = { }
		if self.file.has_key('serv'):
			params['service'] = self.file['serv']

                if self.file.has_key('port'):
                        params['port'] = self.file['port']

			
                if self.file.has_key('url'):
                        params['location'] = self.file['url']
	
		if self.file.has_key('httpAuth'):
			if ( self.file['httpAuth'].has_key('username') and self.file['httpAuth'].has_key('password') ):
				params['transport'] = HttpAuthenticated( **self.file['httpAuth'] )	

                if self.file.has_key('proxy') and self.file['proxy']:
			proxies = {}
	                for method,prox in self.file['proxy'].iteritems():
        	                if url and url.startswith('http')==False:
                	                proxies[method] = "%s://%s" % (method,prox)
                        	else:
                                	proxies[method] = prox
			params['proxy'] = proxies

		if ( self.file.has_key('wsdl' ) ):
			wsdl = self.file['wsdl']
 
		params['plugins'] = [UnicodeFilter()]
		
		client = Client(wsdl, **params)

		if self.file.has_key('username') and self.file.has_key('password'):
			token 		=	client.factory.create('AuthToken')
			token.username	=	self.file['username']
			token.password	=	self.file['password']
			client.set_options(soap_headers=token)

		#result = client.service.GetWeather('Strasbourg','France')
		if self.file.has_key('function') and self.file['function']:
			if self.file.has_key('params') and self.file['params']:
				result = getattr(client.service, self.file['function'])(**self.file['params'])
			else:
				result = getattr(client.service, self.file['function'])
		if ( type(result) == list ):	
			try:
				f = ''.join(result)
				if self.file.has_key('encode_origin'):
					try:
						f = f.encode(self.file['encode_origin'],'strict')
					except:
						self.message.addError('018', 'Encoding failed', 2)
				else :
					encode = 'utf-8'
					try:
						f = f.encode(encode)
	                        	except:
        	                        	self.message.addError('018', 'Encoding failed', 2)
			
				tmp_file = "tmp_soap.xml"
				candles.writeInFile(f,tmp_file)
				tree = et.parse(tmp_file)
				root = tree.getroot()

				data = []
				tmp = {}
				for elt in root:
					tmp[elt.tag] = elt.text
				data.append(tmp)
				os.remove(tmp_file)
			except:
				self.message.addError('012', 'Response treatment failure', 2)
		else :
			data = []
			data.append( { 'result': result } ) 


        	return data


        # returns the datas from the source
        def getDataSOAP(self):
                results = { }
                for file in self.conf['list_ws']:
                        self.file = file
                        try:
                                self.query_name = file['name']
                        except Exception as e:
                                self.message.addError('022',  "No name found for the file query : %s" % e, 2)
                        if ( file.has_key('mapping') ):
                                self.current_mapping = file['mapping']
                        else:
                                self.current_mapping = {}
			#try:
	                self.result['data'] = self.convert_to_json()

                        self.content = ""
                        self.treatData_generic()
                        results[self.query_name] = self.result
                        self.result = {}
                                #self.current_mapping = {}
                        self.current_file = {}
                        #except:
                        #       self.message.addError('012', 'Impossible to find file in configuration', 2)
                        self.current_mapping = { }
                self.data = results


	def init(self):
		if self.conf.has_key('name'):
			self.name = self.conf['name']
		self.getData_generic()

        # returns the datas from the source
        def getData(self):
                self.getDataSOAP()

