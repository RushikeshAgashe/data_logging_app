import sunspec.core.client as client
from subprocess import call
import time
import argparse
import sys
import os
from xml.dom import minidom
from collections import OrderedDict
import sqlite3
from datetime import datetime, date
from struct import pack


def create_table(table_name, point_type_dict):
	point_type_list = []
	for key,value in point_type_dict.iteritems():
		point_type_list.append([key,point_type_dict[key]])
	table_columns_and_type_str = [" ".join(item) for item in point_type_list]
	table_columns_and_type_str = str(tuple(table_columns_and_type_str)).replace("'","")
	
	conn = sqlite3.connect('../sunspec_database/BBBK_'+db_timestamp+'.db')
	c = conn.cursor()
	c.execute("CREATE TABLE IF NOT EXISTS "+table_name+table_columns_and_type_str)
	try:
		conn.commit()
	except:
		pass
	c.close()
	conn.close()

def data_entry(table_name, point_value_dict):
	
	point_value_list = []
	for key,value in point_value_dict.iteritems():
		point_value_list.append([key, point_value_dict[key]])
	table_columns_str = [item[0] for item in point_value_list]
	table_columns_str = str(tuple(table_columns_str)).replace("'","")
	data = [item[1] for item in point_value_list]
	data = tuple(data)
	conn = sqlite3.connect('../sunspec_database/BBBK_'+db_timestamp+'.db')
	c = conn.cursor()
	sql_table_values_str = []
	for i in range(0,len(table_columns_str.split(','))):
		sql_table_values_str.append('?')
	sql_table_values_str = str(tuple(sql_table_values_str)).replace("'","")
	c.execute("INSERT INTO "+table_name+table_columns_str+" VALUES"+sql_table_values_str,data)
	conn.commit()
	c.close()
	conn.close()
	
def clear_screen():
	if sys.platform in ['win32','win64']:
        	os.system("cls")
    	else:
        	os.system("clear")

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
		self.db_points = None
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
	# model.offset = get_model_offset(model)

	xml_points = model.xmldoc.getElementsByTagName("model")[0].getElementsByTagName("point")
	for xml_point in xml_points:
		model.points.append(Point(xml_point))
	for point in model.points:
		point_init(point)

def add_to_write_queue(element):
	write_queue.append(element[0])

def get_write_value(value, point):
	datatype = point.xml_point.attributes['type'].value
	try:
		sf = point.xml_point.attributes['sf'].value
	except:
		sf = None
	
	if datatype in ['uint16','int16']:
		value = int(value)
		if sf != None:
			value = value*1000
		value = pack('>h', value)

	elif datatype in ['uint32', 'int32']:
		value = int(value)
		if sf != None:
			value = value*1000
		value = pack('>l', value)
	else:
		pass
	return value

def write_values(d, models):
	while write_queue:
		for model in models:
			for point in model.points:
				if point.xml_point.attributes['id'].value == write_queue[0]['id']:
					modbus_address = int(point.xml_point.attributes['offset'].value) + 68 + 4
					print modbus_address
					print write_queue[0]['value']
					write_value = get_write_value(write_queue[0]['value'], point)
					d.device.write(modbus_address,write_value)
		del write_queue[0]

def populate_database(d, models):
	timestamp = datetime.now()
	for model in models:
		data_points = d.device.models.values()[model.index][0].points
		for point in model.points:
			point_attr_dict = OrderedDict()
			point_attr_dict['timestamp'] = timestamp
			point_attr_dict['id'] = point.id
			point_attr_dict['units'] = point.units
			point_attr_dict['type'] = point.type
			if point_attr_dict['type'] not in ['pad','sunssf']:
				point_attr_dict['base_value'] = data_points[point_attr_dict['id']].value_base
			if point.sf:
				point_attr_dict['sf'] = data_points[point_attr_dict['id']].value_sf
			if not point.access:
				point.access = 'r'
			point_attr_dict['access'] = point.access
			data_entry(model.db_tablename,point_attr_dict)


def run(timestamp,port, protocol='RTU', slave_id=1, baudrate='9600'):
	global runFlag
	global db_timestamp
	db_timestamp = timestamp
	d = client.SunSpecClientDevice(client.RTU,slave_id=slave_id,name=port,baudrate=baudrate)
	d.read()
	models = []
	model_ids = d.device.models.keys()
	for index,model_id in enumerate(model_ids):
		models.append(Model(model_id,index))
	for model in models:
		model_init(model)		
		create_table(model.db_tablename,point_attr_dict)

	runFlag = True
	while runFlag == True:
		write_values(d,models)
		d.read()
		populate_database(d, models)
		if __name__ == "__main__":
			clear_screen()
			print d
		elif DEBUG == True:
			print d
		else:
			pass
		
	d.close()
	return

def stop():
	global runFlag
	runFlag = False
	while runFlag == False:
		pass

runFlag = True
DEBUG = False
db_timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
write_queue = []
dirpath = '../pysunspec-clone/sunspec/models/smdx/'
point_attr_dict = OrderedDict()
point_attr_dict ['timestamp'] 	= 'TIMESTAMP'
point_attr_dict ['id'] 			= 'TEXT'
point_attr_dict ['base_value'] 	= 'TEXT'
point_attr_dict ['sf'] 			= 'TEXT'
point_attr_dict ['units']	 	= 'TEXT'
point_attr_dict ['type'] 		= 'TEXT'
point_attr_dict ['access'] 		= 'TEXT'

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='SunSpec MODBUS Polling Client')
	parser.add_argument('--port',required=True, help='Serial COM Port')
	parser.add_argument('--protocol', nargs='?', default='RTU', help='Protocol for MODBUS communication')
	parser.add_argument('--slave_id', nargs='?',type = int, default=1, help='MODBUS Slave ID')
	parser.add_argument('--baudrate', nargs='?', type=int, default=9600, help='MODBUS Serial COM Baudrate')
	args = parser.parse_args()

	if args.protocol == 'TCP':
	    protocol = client.TCP
	else:
	    protocol = client.RTU
	run(timestamp=db_timestamp,port=args.port, protocol=args.protocol, slave_id=args.slave_id, baudrate=args.baudrate)
