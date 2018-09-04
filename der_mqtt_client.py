import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish
import json
import der_mqtt_sunspec_tunnel as tunnel
from threading import Thread
import time

def on_message(mqttc, obj, msg):
	global tic, toc, counter
	if tic != 0:
		toc = (toc*counter + 1000*(time.time() - tic))/(counter+1)
		counter += 1
		#print toc
	tic = time.time() 
        msg.payload = json.loads(msg.payload)
	Iinv_ref_value = msg.payload['Iinv_ref']
	msg_counter = msg.payload['msg_counter']
	tunnel.add_to_sunspec_write_queue({'id':'Iinv_ref','value':Iinv_ref_value, 'msg_counter': msg_counter})
        #print msg.payload['Iinv_ref']

def on_disconnect(client, userdata,rc=0):
    mqttc.loop_stop()

def publish_data(payload):
	payload = json.dumps(payload)
	mqttc.publish("/ragashe@ncsu.edu/SST-Project/critical-measurements", payload, 0)
	#mqttc.publish("SST-Project/critical-measurements", payload, 0)

def check_publish_queue():
	tic = 0
	while True:
		if tunnel.mqtt_publish_queue:
			#print 1000*(time.time() - tic)
                	publish_data(tunnel.mqtt_publish_queue[-1])
                	tunnel.mqtt_publish_queue = []
			tic = time.time()

def publish_thread_start():
	t = Thread(target=check_publish_queue)
	t.start()

tic = 0
toc = 0
counter = 1

mqttc = mqtt.Client(transport="websockets")
mqttc.on_message = on_message
mqttc.username_pw_set("ragashe@ncsu.edu","29e8ccce")
#mqttc.tls_set("ca-certificate.crt")
#mqttc.connect("broker.hivemq.com", 8000)
#mqttc.connect("test.mosquitto.org", 8080)
mqttc.connect("mqtt.dioty.co", 8080)

#mqttc.subscribe("SST-Project/controller-data")
mqttc.subscribe("/ragashe@ncsu.edu/SST-Project/controller-data")

mqttc.loop_start()

'''
while True:
	if tunnel.mqtt_publish_queue:
		publish_data(tunnel.mqtt_publish_queue[0])
		del tunnel.mqtt_publish_queue[0]

print "Test Print"
'''
