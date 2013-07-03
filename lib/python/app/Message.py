#!/opt/connecteurSQL/bin/python2.7
#################################################
#						#
#		MOTOR JSON			#
# or how to return datas from multipe sources	#
# in order to register them through one format	#
#						#
#################################################
from datetime import datetime
import logging
class Message:
	def __init__(self, logfile):
		self.logfile = logfile +"log-"+datetime.now().strftime("%Y%m%d")
		self.errors  = [] 
		self.messages= []
		self.logger = logging.getLogger('EXT_Datasource_Connector')
		self.logger.setLevel(logging.DEBUG)
		ch = logging.StreamHandler()
		ch.setLevel(logging.DEBUG)
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		ch.setFormatter(formatter)
		if not self.logger.handlers:
			self.logger.addHandler(ch)
	# if level == 1, fatal error die !
	def addError(self, errorCode, errorDesc, level):
		error = { 'code': errorCode, 'description': errorDesc } 
		self.errors.append(error)
		
		if level == 1:
			self.logger.critical( "(%s) %s" % ( errorCode, errorDesc ) )
			os._exit()
		else:
			self.logger.warning ( "(%s) %s" %( errorCode, errorDesc ) )
	def addMessage(self, content ) :
		message = { 'content' : content }
		self.messages.append( message ) 
		self.logger.info( content )	
		
