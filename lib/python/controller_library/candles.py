#!/opt/connecteurSQL/bin/python2.7

# System Libs
import sys
import os

# We define our vars
global base
base = os.environ['HOME_CONNECTEUR']

global cache_dir
if (os.environ.has_key( 'HOME_CONNECTEUR_CACHE_DIR' ) ):
	cache_dir = os.environ['HOME_CONNECTEUR_CACHE_DIR'] 
else:
	cache_dir = ("%s/cache" % (base) ) 

global resource_dir
if ( os.environ.has_key('HOME_CONNECTEUR_RESOURCE_DIR') ) :
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
sys.path.append( "%s/postTreatments" % ( resource_dir ) )

import errno
from xml.dom import minidom
from copy import deepcopy
import simplejson as json
import urllib2

# Lib for Messages
from app.Message import Message

# Conf Message
global message
message = Message( log_dir )

# Write content in file
def writeInFile(data,filename):
	try:
        	with open(filename, 'w') as cacheFile:
                	cacheFile.write(data)
		return True
        except Exception as e:
                message.addError('007', "Impossible to write in file (%s) : %s" % ( filename, e), 2)
		return False


# Retrieve a JSON file and return a formatted array
def getJsonToArray(file=False,string=False):
        """
        Convert JSON File to Python Array
        """
	if file:
        	# We store the content of the JSON in the variable f
	        f = open(file,'r').read()
        	try:
	        # We transform the JSON into a library
        	        data = json.loads(f)
	        except Exception as e:
        	        message.addError( "003", "An error occured while loading file (%s) : %s" % (file, e), 2)
                	return False
	elif string:
		try:
			data = json.loads(string)
                except Exception as e:
                        message.addError( "003", "An error occured while loading string (%s) : %s" % (string, e), 2)
                        return False


        return data

# Retrieve file using proxy
def getDynamicFile(url,proxy=False):
	"""
	Open a distant file w/ or w/o Proxy settings
	"""
	headers={'User-agent' : 'Mozilla/5.0'}
	if proxy:
		proxies = {}
		for method,prox in proxy.iteritems():
			if url.startswith('http')==False:
				proxies[method] = "%s://%s" % (method,prox)
			else:
				proxies[method] = prox
		proxy_support = urllib2.ProxyHandler(proxies)
		opener = urllib2.build_opener(proxy_support, urllib2.HTTPHandler(debuglevel=1))
		urllib2.install_opener(opener)
	
	req = urllib2.Request(url, None, headers)
	html = urllib2.urlopen(req).read()
	return html

# Retrieve file content
def getFileContent(url,encode=False,proxy=False):
	"""
	Returns the content of a file and tries to return it as unicode
	"""
	if proxy==False or url.startswith('http')==False:
		content = open(url,'r').read()
	else:
		content = getDynamicFile(url,proxy)
	if encode:
		content.decode(encode)
	else:
        	tmp_content = try_unicode(content,'strict')
	        if tmp_content:
        	        content = tmp_content

	return content

def try_unicode(string, errors='strict'):
	"""
	Tries to convert a string into unicode
	"""
	encoding_guess_list = ["utf-8","ascii","cp1250", "latin1", "iso-8859-2","iso-8859-1","utf-16"]
	if isinstance(string, unicode):
		return string
	assert isinstance(string, str), repr(string)
	for enc in encoding_guess_list:
		try:
			return string.decode(enc, errors)
		except UnicodeError, exc:
			continue


def dict2element(root,structure,doc,elt_val):
	"""
	Gets a dictionary like structure and converts its
	content into xml elements. After that appends
	resulted elements to root element. If root element
	is a string object creates a new elements with the
	given string and use that element as root.

	This function returns a xml element object.

	"""
    	# if root is a string make it a element
	if isinstance(root,str):
        	root = doc.createElement(root)
    	if isinstance(structure, list):
		for tmp in structure:
			parent = doc.createElement(elt_val)
                	for key,value in tmp.iteritems():
                        	el = doc.createElement(str(key))
	                        if isinstance(value, (dict,tuple)):
        		                dict2element(el,value,doc,elt_val)
                	        else:
                        	        #el.appendChild(doc.createTextNode(str(value) if value is not None  else ''))
                                        el.appendChild(doc.createCDATASection(str(value) if value is not None  else ''))
                                	parent.appendChild(el)
			root.appendChild(parent)

    	else:
        	for key,value in structure.iteritems():
                	el = doc.createElement(str(key))
                	if isinstance(value, (dict,tuple)):
	                    	dict2element(el,value,doc,elt_val)
                	else:
        	            	el.appendChild(doc.createTextNode(str(value) if value is not None  else ''))
                		root.appendChild(el)

    	return root

def dict2xml(structure,tostring=False,elt_val='element'):
    	"""
    	Gets a dict like object as a structure and returns a corresponding minidom
    	document object.

    	If str is needed instead of minidom, tostring parameter can be used

    	Restrictions:
    	Structure must only have one root.
    	Structure must consist of str or dict objects (other types will
    	converted into string)

    	Sample structure object would be
    	{'root':{'elementwithtextnode':'text content','innerelements':{'innerinnerelements':'inner element content'}}}
    	result for this structure would be
    	'<?xml version="1.0" ?>
    	<root>
      		<innerelements><innerinnerelements>inner element content</innerinnerelements></innerelements>
      	<elementwithtextnode>text content</elementwithtextnode>
    	</root>'
    	"""
    	# This is main function call. which will return a document
    	assert len(structure) == 1, 'Structure must have only one root element'

    	root_element_name, value = next(structure.iteritems())
    	impl = minidom.getDOMImplementation()
    	doc = impl.createDocument(None,str(root_element_name),None)
    	dict2element(doc.documentElement,value,doc,elt_val)
    	#print doc.toxml() if tostring else doc
	return doc.toxml() if tostring else doc


def make_sure_path_exists(path):
    	"""
    	Check if dir exist and if not creates it
    	"""
    	try:
        	os.makedirs(path)
    	except OSError as exception:
        	if exception.errno != errno.EEXIST:
            		raise

def combine(a, b):
        """
	recursively merges dict's. not just simple a['key'] = b['key'], if
           both a and bhave a key who's value is a dict then dict_merge is called
           on both values and the result stored in the returned dictionary.
	"""
        if not isinstance(b, dict):
                return b
        result = deepcopy(a)
        for k, v in b.iteritems():
                if k in result and isinstance(result[k], dict):
                        result[k] = combine(result[k], v)
                else:
                        result[k] = deepcopy(v)
        return result

def getTimestampFile(file):
	"""
	Retrieve the timestamp included in the file
	Model : filename = source + _ + timestamp
	"""
        filename = file.split('.')
        ts = filename[0].split('_')
	try:
		return int(ts[-1])
	except:
	        return 0

def is_ascii(s):
	"""
	Return boolean on if string is ascii
	"""
	return all(ord(c) < 128 for c in s)
def dataDecodeEncode(data,decode,encode):
	"""
		Fonction recursive d'encode et decode data
	"""
	if ( isinstance(data, list)):
		table = []
		for elt in data:
			table.append( dataDecodeEncode(elt,decode,encode) )
		return table
	elif ( isinstance( data, dict)):
		new_dict = { }
		for key,val in data.iteritems():
			new_dict[key] = dataDecodeEncode(val,decode,encode)
		return new_dict
	elif ( isinstance ( data, basestring)) :
		if ( is_ascii(data) or decode == 'ascii' ) :
			decode = 'ascii'
			return data.encode(encode)
		else:
			return data.decode(decode).encode(encode)
	else:
		return data
