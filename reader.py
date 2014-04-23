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
	c = conn.cursor()
	timestamp =  datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
	c.execute("CREATE TABLE IF NOT EXISTS "+table_name+table_columns_and_type_str)
	conn.commit()
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
	
def main():
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
	print args

	d = client.SunSpecClientDevice(client.RTU,slave_id=args.slave_id,name=args.port,baudrate=args.baudrate)
	d.read()
	model_ids = d.device.models.keys()
	dirpath = 'sunspec/models/smdx/'
	reader_blocks = {}
	point_type_list = []
	for model_id in model_ids:
		point_type_dict = OrderedDict()
		point_type_dict['data_timestamp'] = 'TIMESTAMP'
		reader_blocks[str(model_id)] = ModelReader()
		rb = reader_blocks[str(model_id)]	
		rb.input_filepath = dirpath + 'smdx_' + '{0:05}'.format(model_id) + '.xml'
		rb.output_filepath = 'smdx_data_' + '{0:05}'.format(model_id) + '.xml' 
		xmldoc = minidom.parse(rb.input_filepath)
		points = xmldoc.getElementsByTagName("model")[0].getElementsByTagName("point")
		for point in points:
			key = point.attributes['id'].value.encode('ascii')
			value = point.attributes['type'].value.encode('ascii')
			point_type_dict[key] = value
			if value  == 'string':
				point_type_dict[key] = 'TEXT'
			elif value in ['uint16', 'int16', 'uint32', 'int32']:
				point_type_dict[key] = 'REAL'
			elif value in ['sunssf','pad']:
				del point_type_dict[key]
				continue
			point_type_list.append([key ,point_type_dict[key]])	
		table_name = 'smdx_data_' + '{0:05}'.format(model_id)
		create_table(table_name,point_type_dict)

	while True:
		d.read()
		model_ids = d.device.models.keys()
		clear_screen()
		itr = 0
		timestamp = datetime.now()
		for model_id in model_ids:
			point_value_dict = OrderedDict()
			point_value_dict['data_timestamp'] = timestamp
			rb = reader_blocks[str(model_id)]
			xmldoc = minidom.parse(rb.input_filepath)
			points = xmldoc.getElementsByTagName("model")[0].getElementsByTagName("point")
			data_points = d.device.models.values()[itr][0].points
			for point in points:
				point_type =  point.attributes['type'].value.encode('ascii')
				point_id = point.attributes['id'].value.encode('ascii')
				if point_type in ['pad','sunssf']:
					point.appendChild(xmldoc.createTextNode(''))
				else:
					if data_points[point_id].value_sf is not None:
						scaling_factor = 10**data_points[point_id].value_sf
						point_value = scaling_factor*data_points[point_id].value_base
					else:
						point_value = data_points[point_id].value_base
					
					point_value = '--' if point_value is None else point_value
					point_value_dict[point_id] = point_value
			#		point.appendChild(xmldoc.createTextNode(str(point_value)))
			#output_xmlfile = open(rb.output_filepath, "w+")
			#xmldoc.writexml(output_xmlfile)
			#output_xmlfile.close()
			itr = itr + 1
			table_name = 'smdx_data_' + '{0:05}'.format(model_id)
			data_entry(table_name,point_value_dict)
		print d
	    
if __name__ == "__main__":
	main()
