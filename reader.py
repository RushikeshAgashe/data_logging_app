import sunspec.core.client as client
from subprocess import call
import time
import argparse
import sys
import os

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

while True:
    d.read()
    if sys.platform in ['win32','win64']:
        os.system("cls")
    else:
        os.system("clear")
    print d
    time.sleep(1)
    

