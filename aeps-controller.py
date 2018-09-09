#import context  # Ensures paho is in PYTHONPATH
import paho.mqtt.client as mqtt
import json
import time
import ssl
def process_data(payload):
    Pgrid_ref = 600.0
    payload = json.loads(payload)
    Pinv_rms = payload['Iinv_rms']*120.0
    Pload_rms = payload['Iload_rms']*120.0
    msg_counter = payload['msg_counter']
    #print Pinv_rms, Pload_rms
    if (Pload_rms - Pinv_rms > (Pgrid_ref)):
	Pinv_calc = Pload_rms - Pgrid_ref
	if (abs(Pinv_calc - Pinv_rms) > 0):
    		Iinv_ref = Pinv_calc/85
    		reply = json.dumps({'Iinv_ref': Iinv_ref, 'msg_counter': msg_counter})
    else:
	reply = None
    return reply

def on_connect(mqttc, obj, flags, rc):
    print("rc: "+str(rc))

def on_disconnect(client, userdata,rc=0):
    mqttc.loop_stop()

def on_message(mqttc, obj, msg):
    global tic, toc, counter
    if tic != 0:
	toc = (toc*counter + 1000*(time.time() - tic))/(counter+1)
	counter += 1
    	print toc
    tic = time.time()
    reply = process_data(msg.payload)
    if reply:
	#mqttc.publish("SST-Project/controller-data", reply, 0)
	mqttc.publish("/ragashe@ncsu.edu/SST-Project/controller-data", reply, 0)
    #print "Iinv_rms = ", json.loads(msg.payload)['Iinv_rms'], "Iload_rms = ", json.loads(msg.payload)['Iload_rms']

def on_publish(mqttc, obj, mid):
    pass
    #print("mid: "+str(mid))

counter = 0
# If you want to use a specific client id, use
# mqttc = mqtt.Client("client-id")
# but note that the client id must be unique on the broker. Leaving the client
# id parameter empty will generate a random id for you.

mqttc = mqtt.Client(transport="websockets")
#mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_publish = on_publish
mqttc.on_connect = on_connect
mqttc.on_disconnect = on_disconnect
tic = 0
toc = 0
counter = 1
#mqttc.username_pw_set("gxkjmtxx","EGf-IydwhIMe")
#mqttc.username_pw_set("jnszdtnn","SozLR4e9Ndhv")
mqttc.username_pw_set("ragashe@ncsu.edu","29e8ccce")
#mqttc.username_pw_set("ragashe@ncsu.edu","")
mqttc.ws_set_options(path="/mqtt", headers=None)
mqttc.tls_set("ca-certificates.crt")

#mqttc.connect("m14.cloudmqtt.com",38286)
#mqttc.connect("m10.cloudmqtt.com",31144)
#mqttc.connect("test.mosquitto.org",8080)
#mqttc.connect("broker.hivemq.com",8000)
mqttc.connect("mqtt.dioty.co",8880)
#mqttc.connect("maqiatto.com",8883)

mqttc.subscribe("/ragashe@ncsu.edu/SST-Project/critical-measurements", 0)
#mqttc.subscribe("ragashe@ncsu.edu/SST-Project/critical-measurements", 0)

mqttc.loop_forever()
