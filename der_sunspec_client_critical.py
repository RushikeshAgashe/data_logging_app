import sunspec.core.client as client
import argparse
import sys
import os
from xml.dom import minidom
from collections import OrderedDict
import sqlite3
from datetime import datetime, date
from struct import pack
import time
import der_mqtt_client as mqtt_client
import der_mqtt_sunspec_tunnel as tunnel

class Model(object):
	def __init__(self,model_id, index):
		self.index = index
		self.id = model_id
		self.len    = None
		self.name   = ''
		self.offset = None
		self.points = []
		self.xml_input_filepath = ''
		self.xmldoc = None
		self.data_points_obj = None
		self.db_tablename = ''

class Point(object):
	def __init__(self, xml_point):
		self.xml_point = xml_point
		self.id = ''
		self.offset = ''
		self.len  = ''
		self.type = ''
		self.base_value = ''
		self.sf = ''
		self.units = ''
		self.access = ''
		self.mandatory = ''

def get_attr_value(xml_element, id):
	try:
		value = xml_element.attributes[id].value.encode('ascii')
	except KeyError:
		value = ''
	return value

def calc_model_offsets(models):
	for model in models:
		if 1 == int(model.id):
			model.offset = SUNS_ID_SIZE
		else:
			model.offset = models[model.index - 1].offset + int(models[model.index -1].len) + MODEL_HEADER_SIZE
		
def point_init(point):
	point.id = get_attr_value(point.xml_point, 'id')
	point.offset = get_attr_value(point.xml_point, 'offset')
	point.len = get_attr_value(point.xml_point, 'len')
	point.type = get_attr_value(point.xml_point, 'type')
	point.sf = get_attr_value(point.xml_point, 'sf')
	point.units = get_attr_value(point.xml_point, 'units')
	point.mandatory = get_attr_value(point.xml_point, 'mandatory')
	point.access = get_attr_value(point.xml_point, 'access')

def model_init(model):
	model.db_tablename = 'smdx_data_' + '{0:05}'.format(model.id)
	model.xml_input_filepath = dirpath + 'smdx_' + '{0:05}'.format(model.id) + '.xml'
	model.xmldoc = minidom.parse(model.xml_input_filepath)
	
	xml_model = model.xmldoc.getElementsByTagName("model")[0]
	model.len = get_attr_value(xml_model, 'len')
	model.name = get_attr_value(xml_model, 'name')

	xml_points = model.xmldoc.getElementsByTagName("model")[0].getElementsByTagName("point")
	for xml_point in xml_points:
		model.points.append(Point(xml_point))
	for point in model.points:
		point_init(point)
		point_id_map[point.id] = point
		model_id_map[point.id] = model

def get_conditioned_write_value(element):
	point = point_id_map[element['id']]
	model = model_id_map[element['id']]
	value = element['value']
	if point.sf:
		value = int(float(value)/(10**int(model.data_points_obj[point.id].value_sf)))
	else:
		value = int(value)

	if point.type == 'uint16':
		if value > 0xFFFF:
			raise ValueError()
	elif point.type == 'int16':
		if value < -0x8000 or value > 0x7FFF:
			raise ValueError() 
	elif point.type == 'uint32':
		if value > 0xFFFFFFFF:
			raise ValueError() 
	elif point.type == 'int32':
		if value < -0x80000000 or value > 0x7FFFFFFF:
			raise ValueError() 
	else:
		pass

	element['value'] = value
	return element
	#tunnel.sunspec_write_queue.append(element[0])

def get_formatted_write_value(value, point):
	if point.type in ['uint16','int16']:
		value = pack('>h', value)
	else:
		value = pack('>l', value)
	
	return value

def write_values(d, models):
	global tic, toc
	global counter
	while tunnel.sunspec_write_queue:
		#toc += 1000*(time.time() - tic)
		if tic != 0:
			toc = (toc*counter + 1000*(time.time() - tic))/(counter+1)
			counter += 1
			print toc
		#tic = time.time()
		write_value = tunnel.sunspec_write_queue[-1]
		tic = counter_dict[write_value['msg_counter']]
		#print write_value
		#print 1000*(time.time() - counter_dict[write_value['msg_counter']])
		write_value = get_conditioned_write_value(write_value)
		point = point_id_map[write_value['id']]
		model = model_id_map[write_value['id']]
		modbus_address = model.offset + MODEL_HEADER_SIZE + int(point.offset)
		write_value = write_value['value']
		write_value = get_formatted_write_value(write_value, point)
		d.device.write(modbus_address,write_value)
		tunnel.sunspec_write_queue = []

def publish_critical_data(d,models):
	global msg_counter, counter_dict
	for model in models:
		for point in model.points:
			if point.id in ['Iinv_rms', 'Iload_rms']:
				if point_attr_dict['type'] not in ['pad','sunssf']:
                                	point_attr_dict['base_value'] = model.data_points_obj[point.id].value_base
                                if point.sf:
                                        point_attr_dict['sf'] = model.data_points_obj[point.id].value_sf
                                if point.id == 'Iinv_rms':
                                        Iinv_rms = point_attr_dict['base_value']*pow(10,point_attr_dict['sf'])
                                if point.id == 'Iload_rms':
                                        Iload_rms = point_attr_dict['base_value']*pow(10,point_attr_dict['sf'])
                                        payload = {'Iinv_rms': Iinv_rms, 'Iload_rms': Iload_rms,'msg_counter': msg_counter}
					counter_dict[msg_counter] = time.time()
					msg_counter += 1
                                        tunnel.add_to_mqtt_publish_queue(payload)


def populate_database(d, models):
	#timestamp = datetime.now()
	timestamp = 10
	Pref_grid = 600.0
	for model in models:
		for point in model.points:
			if point.id in ['Iinv_rms','Iload_rms']:
				if point_attr_dict['type'] not in ['pad','sunssf']:
					point_attr_dict['base_value'] = model.data_points_obj[point.id].value_base
				if point.sf:
					point_attr_dict['sf'] = model.data_points_obj[point.id].value_sf
				if point.id == 'Iinv_rms':
					Iinv_rms = point_attr_dict['base_value']*pow(10,point_attr_dict['sf'])
					#Pinv = Iinv_rms*120.0
				if point.id == 'Iload_rms':
					Iload_rms = point_attr_dict['base_value']*pow(10,point_attr_dict['sf'])
					#Pload = Iload_rms*120.0
					payload = {'Iinv_rms': Iinv_rms, 'Iload_rms': Iload_rms}
					mqtt_client.publish_data(payload)
					#add_to_write_queue([{'id':'Iinv_ref','value':iinv_reference_value}])	
					'''if (Pload - Pinv > Pref_grid):
						Pinv_calc = Pload - Pref_grid
						if (abs(Pinv_calc - Pinv)> 0):
							iinv_reference_value = Pinv_calc/84.85;
							#print Pload, Pinv_calc, Pinv
							add_to_write_queue([{'id':'Iinv_ref','value':iinv_reference_value}])
					#print Iload
					'''	#print timestamp
				#point_attr_dict['access'] = point.access
				#data_entry(model.db_tablename,point_attr_dict)

def run(timestamp,server_address,server_port,protocol='TCP', slave_id=1,timeout=0.1):
	global runFlag
	global db_timestamp
	global device_name
	try:
		device_name = os.environ["DEVICE_NAME"]+'_'
	except KeyError:
		pass
	db_timestamp = timestamp
	try:
		d = client.SunSpecClientDevice(client.TCP ,slave_id=slave_id,ipaddr=server_address,ipport=server_port,timeout=timeout)
		d.read()
	except:
		raise IOError("Modbus Read Error!")
	models = []
	model_ids = d.device.models.keys()
	for index,model_id in enumerate(model_ids):
		models.append(Model(model_id,index))
	for model in models:
		model_init(model)		
		model.data_points_obj = d.device.models.values()[model.index][0].points
		#create_table(model.db_tablename,point_attr_dict)
	calc_model_offsets(models)
	runFlag = True
	while runFlag == True:
		write_values(d,models)
		try:
			#tic = time.time()
			d.sst_inverter.read()
			#toc = time.time()
			#print 1000*(toc-tic)
		except:
			continue
		
		publish_critical_data(d, models)
		#time.sleep(0.1)
		if DEBUG == True:
			print d
		else:
			pass
	d.close()
	return

def stop():
	global runFlag
	runFlag = False
	return

point_id_map = {}
model_id_map = {}
MODEL_HEADER_SIZE = 2
SUNS_ID_SIZE = 2
runFlag = True
DEBUG = False
device_name = 'BBBK_unknown'
try:
	device_name = os.environ["DEVICE_NAME"]+'_'
except KeyError:
	device_name = 'BBBK_unknown_'
db_path  = '../sunspec_database/'
db_timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
dirpath = '../pysunspec-clone/sunspec/models/smdx/'
point_attr_dict = OrderedDict()
point_attr_dict ['timestamp'] 	= 'TIMESTAMP'
point_attr_dict ['id'] 			= 'TEXT'
point_attr_dict ['base_value'] 	= 'TEXT'
point_attr_dict ['sf'] 			= 'TEXT'
point_attr_dict ['units']	 	= 'TEXT'
point_attr_dict ['type'] 		= 'TEXT'
point_attr_dict ['access'] 		= 'TEXT'
tic = 0
toc = 0
counter = 1
msg_counter = 1
counter_dict = {}
if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='SunSpec MODBUS Polling Client')
        parser.add_argument('--protocol', nargs='?', default='TCP', help='Protocol for MODBUS communication')
        parser.add_argument('--slave_id', nargs='?',type = int, default=1, help='MODBUS Slave ID')
        parser.add_argument('--server_address', nargs='?', default="10.76.56.2", help='MODBUS TCP Server IP Address')
        parser.add_argument('--server_port', nargs='?', type=int, default=8899, help='MODBUS TCP Server Port')

	args = parser.parse_args()
	print args
	if args.protocol == 'TCP':
	    protocol = client.TCP
	else:
	    protocol = client.RTU
	run(timestamp=db_timestamp,server_port=args.server_port, server_address=args.server_address, protocol=args.protocol, slave_id=args.slave_id)
