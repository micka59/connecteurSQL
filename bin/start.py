#!/opt/connecteurSQL/bin/python2.7
import sys
import os
#We import library path
#we find cur dir
if not os.environ.has_key('HOME_CONNECTEUR') or os.environ['HOME_CONNECTEUR'] == '':
        dir_path = os.path.dirname(os.path.abspath(__file__))
        home_connecteur = "%s/.." % dir_path
        os.environ['HOME_CONNECTEUR'] = home_connecteur


sys.path.append(os.environ['HOME_CONNECTEUR']+"/lib/python")
sys.path.append(os.environ['HOME_CONNECTEUR']+"/lib/python2.7")

import simplejson as json

dir_path = os.path.dirname(os.path.abspath(__file__))
f = open("%s/../etc/conf.json" % ( dir_path ) ,'r').read()
conf = json.loads( f )

if ( conf.has_key('resources_dir') ):
	os.environ['HOME_CONNECTEUR_RESOURCE_DIR'] = conf['resources_dir'] 
if ( conf.has_key('cache_dir') ):
	os.environ['HOME_CONNECTEUR_CACHE_DIR'] = conf['cache_dir'] 
if ( conf.has_key('log_dir') ):
	os.environ['HOME_CONNECTEUR_LOG_DIR'] = conf['log_dir'] 

from controller_library import motor

if len ( sys.argv ) > 2:
	"""  We have more than one argument """
	result = motor.start_car ( sys.argv[1], sys.argv)
elif len( sys.argv ) == 2:
	""" We have one argument => source """
	result = motor.start_car( sys.argv[1] ) 
elif len( sys.argv ) == 1 :
	""" We have no argument, we return the list of available sources """
	result = motor.start_car() 

