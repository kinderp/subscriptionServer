from flask import Flask, request, Response
from sys import argv, exit
from datetime import datetime
from math import trunc
from time import sleep
import os
import atexit
import string
import signal
import json
import pdb
import time
import requests
import subprocess

from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


# This function is registered to be called upon termination
def all_done():
    os.unlink(pidfile)

# Default arguments
port = 7777
host='0.0.0.0'
server_url = '/accumulate'
verbose = 0
created_table = 'no'
list_sub_id = {}
list_resource_id = {}

# Arguments from command line
if len(argv) > 2:
    port = int(argv[1])
    server_url = argv[2]

if len(argv) > 3:
    if argv[3] == 'on':
        print 'verbose mode is on'
        verbose = 1
    else:
        host = argv[3]

if len(argv) > 4:
    if argv[4] == 'on':
        print 'verbose mode is on'
        verbose = 1

pid = str(os.getpid())
pidfile = "/tmp/accumulator." + str(port) + ".pid"

#
# If an accumulator process is already running, it is killed.
# First using SIGTERM, then SIGINT and finally SIGKILL
# The exception handling is needed as this process dies in case
# a kill is issued on a non-running process ...
#
if os.path.isfile(pidfile):
    oldpid = file(pidfile, 'r').read()
    opid   = string.atoi(oldpid)
    print "PID file %s already exists, killing the process %s" % (pidfile, oldpid)

    try: 
        oldstderr = sys.stderr
        sys.stderr = open("/dev/null", "w")
        os.kill(opid, signal.SIGTERM);
        sleep(0.1)
        os.kill(opid, signal.SIGINT);
        sleep(0.1)
        os.kill(opid, signal.SIGKILL);
        sys.stderr = oldstderr
    except:
        print "Process %d killed" % opid


#
# Creating the pidfile of the currently running process
#
file(pidfile, 'w').write(pid)

#
# Making the function all_done being executed on exit of this process.
# all_done removes the pidfile
#
atexit.register(all_done)


app = Flask(__name__)

@app.route(server_url, methods=['GET', 'POST', 'PUT', 'DELETE'])
def record():

    global ac, t0, times, created_table
    s = ''
    send_continue = False

    #pdb.set_trace()
    #get the timestamp for the observations
    timestamp = time.time()*1000
    
    int_timestamp = int(timestamp)
    str_timestamp = str(int_timestamp)
    tokens = str_timestamp.split('.')
    timestamp = tokens[0]

    # First notification? Then, set reference datetime. Otherwise, add the
    # timedelta to the list
    if (t0 == ''):
        t0 = datetime.now()
        times.append(0)
    else:
        delta = datetime.now() - t0
        # Python 2.7 could use delta.total_seconds(), but we use this formula
        # for backward compatibility with Python 2.6
        t = (delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 10**6) / 10**6
        times.append(trunc(round(t)))
        #times.append(t)

    # Store verb and URL
    s += request.method + ' ' + request.url + '\n'

    # Store headers
    for h in request.headers.keys():
        s += h + ': ' + request.headers[h] + '\n'
        if ((h == 'Expect') and (request.headers[h] == '100-continue')):
            send_continue = True

    # Store payload
    if ((request.data is not None) and (len(request.data) != 0)):
        s += '\n'
        s += request.data

    #pdb.set_trace()
    payload = request.data
    payload_obj = json.loads(payload)

    sub_id = payload_obj['subscriptionId']
    entity_type = payload_obj['contextResponses'][0]['contextElement']['type']
    entity_id = payload_obj['contextResponses'][0]['contextElement']['id']
    is_pattern = payload_obj['contextResponses'][0]['contextElement']['isPattern']
    att_type = payload_obj['contextResponses'][0]['contextElement']['attributes'][0]['type']
    att_name = payload_obj['contextResponses'][0]['contextElement']['attributes'][0]['name']
    att_value = payload_obj['contextResponses'][0]['contextElement']['attributes'][0]['value']

    print sub_id
    print entity_type
    print entity_id
    print att_type
    print att_name
    print att_value    


    dir_on_cosmos = entity_id + '_' + entity_type
    name_file = dir_on_cosmos + ".txt"
    name_file_ckan = dir_on_cosmos + ".csv"
    name_table_hive = 'gioakbombaci_' + dir_on_cosmos

    #controllo se e' la prima sottoscrizione
    check = sub_id in list_sub_id
    #pdb.set_trace()
    if (check == True):
	#il sub_id e' nel dizionario, non e' la prima sottoscrizione, tabella gia' creata
	created_table = 'yes'
    else:
	#e' la prima sottoscrizione
	created_table = 'no'
	list_sub_id[sub_id] = timestamp
        #creo l'intestazione per il file csv
	create_header = "echo timestamp," + att_type + " >> " + name_file_ckan
	os.system(create_header)

    create_file = "echo " + timestamp + "," + att_value + " >> " + name_file
    create_file_ckan = "echo " + timestamp + "," + att_value + " >> " + name_file_ckan

    send_file_step1 = 'curl -i -X PUT "http://cosmos.lab.fi-ware.org:14000/webhdfs/v1/user/gioakbombaci/observations/' + dir_on_cosmos + '/' + name_file + '?op=CREATE&user.name=gioakbombaci"'
    send_file_step2 =  'curl -i -X PUT -T ' + name_file + ' --header "content-type: application/octet-stream" "http://cosmos.lab.fi-ware.org:14000/webhdfs/v1/user/gioakbombaci/observations/' + dir_on_cosmos + '/' + name_file + '?op=CREATE&user.name=gioakbombaci&data=true"'
    os.system(create_file)
    os.system(create_file_ckan)

    if (created_table == 'no'):
	# se e' la prima sottoscrizione
	# -1- creo la dir
	# -2- invio il file (new or append)
	# -3- creo la tabella

	#ckan
	# -1- creo il file
	# -2- salvo il resource id che mi server per l'aggiornamento del file alla successiva sottoscrizione
	
	created_table = 'yes'
	
	create_file_ckan = "curl -H'Authorization: 30f4d771-ef48-4050-a35b-de965247f8aa' 'https://data.lab.fiware.org/api/action/resource_create' --form upload=@" + name_file_ckan + " --form package_id=provadataset --form name=" + name_file_ckan + " --form format=csv"
	
	out_file = timestamp + ".out"
	out_curl = create_file_ckan + " > " + out_file
	os.system(out_curl)
	output = ""
	with open(out_file, "r") as out_file_curl:
		output=out_file_curl.read().replace('\n', '')
        response_ckan_create = json.loads(output)
        resource_id = response_ckan_create['result']['id']
	list_resource_id[sub_id] = resource_id
	print resource_id

    	create_dir = 'curl -i -X PUT "http://cosmos.lab.fi-ware.org:14000/webhdfs/v1/user/gioakbombaci/observations/' + dir_on_cosmos + '?op=MKDIRS&user.name=gioakbombaci"'
	
	#inizio codice hive
	os.system(create_dir)
	os.system(send_file_step1)
	os.system(send_file_step2)

	#creo la tabella
	try:
    		transport = TSocket.TSocket('130.206.80.46', 10000)
    		transport = TTransport.TBufferedTransport(transport)
    		protocol = TBinaryProtocol.TBinaryProtocol(transport)

    		client = ThriftHive.Client(protocol)
    		transport.open()
		#pdb.set_trace()
    		query = 'create external table ' + name_table_hive + " (time string, observed_property string) row format delimited fields terminated by ',' location '/user/gioakbombaci/observations/" + dir_on_cosmos + "'"
    		client.execute(query)
    		transport.close()
	except Thrift.TException, tx:
		print '%s' % (tx.message)
    else:
	#se non e' la prima sottoscrizione
	# -1- cancello il file
	# -2- reinvio il file
	delete_file = 'curl -i -X DELETE "http://cosmos.lab.fi-ware.org:14000/webhdfs/v1/user/gioakbombaci/observations/' + dir_on_cosmos + '/' + name_file + '?op=DELETE&user.name=gioakbombaci"'
    	os.system(delete_file)
	os.system(send_file_step1)
	os.system(send_file_step2)

	#ckan
	#ricarico il file
	#os.system(upload_file_ckan)
	#pdb.set_trace()
	id_to_change = list_resource_id[sub_id]
	print id_to_change
	update_ckan = "curl -H'Authorization: 30f4d771-ef48-4050-a35b-de965247f8aa' 'https://data.lab.fiware.org/api/action/resource_update' --form upload=@" + name_file_ckan+ " --form name="+ name_file_ckan + " --form id=" + id_to_change + " --form format=csv"
	os.system(update_ckan)
    # Separator
    s += '=======================================\n'

    # Accumulate
    ac += s

    if verbose:
        print s

    if send_continue:
        return Response(status=100)
    else:
        return Response(status=200)


@app.route('/dump', methods=['GET'])
def dump():
    return ac


@app.route('/times', methods=['GET'])
def times():
    return ', '.join(map(str,times)) + '\n'


@app.route('/number', methods=['GET'])
def number():
    return str(len(times)) + '\n'


@app.route('/reset', methods=['POST'])
def reset():
    global ac, t0, times
    ac = ''
    t0 = ''
    times = []
    return Response(status=200)


@app.route('/pid', methods=['GET'])
def getPid():
    return str(os.getpid())

# This is the accumulation string
ac = ''
t0 = ''
times = []

if __name__ == '__main__':
    # Note that using debug=True breaks the the procedure to write the PID into a file. In particular
    # makes the calle os.path.isfile(pidfile) return True, even if the file doesn't exist. Thus,
    # use debug=True below with care :)
    app.run(host=host, port=port)
