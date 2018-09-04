#import

def init():
	global sunspec_write_queue
	global mqtt_publish_queue
	sunspec_write_queue = []
	mqtt_publish_queue = []

def add_to_mqtt_publish_queue(data):
	mqtt_publish_queue.append(data)

def add_to_sunspec_write_queue(data):
	sunspec_write_queue.append(data)
