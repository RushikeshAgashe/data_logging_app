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


class ModelReader():
	def __init__(self, input_filepath='', output_filepath=''):
		self.input_filepath = input_filepath
		self.output_filepath = output_filepath

def create_table(table_name, point_type_dict):
	point_type_list = []
	for key,value in point_type_dict.iteritems():
		point_type_list.append([key,point_type_dict[key]])
	table_columns_and_type_str = [" ".join(item) for item in point_type_list]
	table_columns_and_type_str = str(tuple(table_columns_and_type_str)).replace("'","")
	
	conn = sqlite3.connect('../sunspec_database/BEAGLEBONE_BLACK_1.db')
	#conn = sqlite3.connect('../sunspec_database/sunspec_'+db_timestamp+'_BEAGLEBONE_BLACK_1.db')
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
	conn = sqlite3.connect('../sunspec_database/BEAGLEBONE_BLACK_1.db')
	#conn = sqlite3.connect('../sunspec_database/sunspec_'+db_timestamp+'_BEAGLEBONE_BLACK_1.db')
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

def stop():
	global runFlag
	runFlag = False
	while runFlag == False:
		pass
	
def run(timestamp,port, protocol='RTU', slave_id='1', baudrate='9600'):
	global runFlag
	global db_timestamp
	db_timestamp = timestamp
	d = client.SunSpecClientDevice(client.RTU,slave_id=slave_id,name=port,baudrate=baudrate)
	d.read()
	model_ids = d.device.models.keys()
	dirpath = '../pysunspec-clone/sunspec/models/smdx/'
	reader_blocks = {}
	point_attr_dict = OrderedDict()
	point_attr_dict ['timestamp'] 	= 'TIMESTAMP'
	point_attr_dict ['id'] 		= 'TEXT'
	point_attr_dict ['base_value'] 	= 'TEXT'
	point_attr_dict ['sf'] 		= 'TEXT'
	point_attr_dict ['units'] 	= 'TEXT'
	point_attr_dict ['type'] 	= 'TEXT'
	point_attr_dict ['access'] 	= 'TEXT'
		
	for model_id in model_ids:
		reader_blocks[str(model_id)] = ModelReader()
		rb = reader_blocks[str(model_id)]	
		rb.input_filepath = dirpath + 'smdx_' + '{0:05}'.format(model_id) + '.xml'
		rb.output_filepath = 'smdx_data_' + '{0:05}'.format(model_id) + '.xml' 
		xmldoc = minidom.parse(rb.input_filepath)
		points = xmldoc.getElementsByTagName("model")[0].getElementsByTagName("point")
		table_name = 'smdx_data_' + '{0:05}'.format(model_id)
		create_table(table_name,point_attr_dict)

	runFlag = True
	while runFlag == True:
		d.read()
		model_ids = d.device.models.keys()
		if __name__ == "__main__":
			clear_screen()
		itr = 0
		timestamp = datetime.now()
		for model_id in model_ids:
			rb = reader_blocks[str(model_id)]
			xmldoc = minidom.parse(rb.input_filepath)
			table_name = 'smdx_data_' + '{0:05}'.format(model_id)
			points = xmldoc.getElementsByTagName("model")[0].getElementsByTagName("point")
			data_points = d.device.models.values()[itr][0].points
			for point in points:
				point_attr_dict = OrderedDict()
				point_attr_dict['timestamp'] = timestamp
				point_attr_dict['id'] = point.attributes['id'].value.encode('ascii')
				point_attr_dict['type'] = point.attributes['type'].value.encode('ascii')
				if point_attr_dict['type'] not in ['pad','sunssf']:
					point_attr_dict['base_value'] = data_points[point_attr_dict['id']].value_base
				try:
					point_attr_dict['sf'] = data_points[point_attr_dict['id']].value_sf
				except KeyError:
					pass
				try:
					point_attr_dict['units'] = point.attributes['units'].value.encode('ascii')
				except KeyError:
					pass
				try:
					point_attr_dict['access'] = point.attributes['access'].value.encode('ascii')
				except KeyError:
					point_attr_dict['access'] = 'r'
				data_entry(table_name,point_attr_dict)
			itr = itr + 1
		if __name__ == "__main__" or DEBUG==True:
			print d
	d.close()
	return
runFlag = True
DEBUG = False
db_timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
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
