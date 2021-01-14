# test for WeatherSense SwitchDoc Labs Weather Sensors
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------------------------------------------
from os import EX_DATAERR
import sys
from subprocess import PIPE, Popen, STDOUT
from threading  import Thread
import json
import datetime
import paho.mqtt.publish as publish


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
#   A few helper functions...

def nowStr():
    return( datetime.datetime.now().strftime( '%Y-%m-%d %H:%M:%S'))

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
#stripped = lambda s: "".join(i for i in s if 31 < ord(i) < 127)

def tempFtoC(temp_F):
    return (temp_F - 32) * (5/9.0)

# For data details see https://shop.switchdoc.com/products/wireless-weatherrack2

# Data Sample
# {"time" : "2020-07-09 10:54:16", "model" : "SwitchDoc Labs F007TH Thermo-Hygrometer", "device" : 233, "modelnumber" : 5, "channel" : 3, "battery" : "OK", "temperature_F" : 72.100, "humidity" : 45, "mic" : "CRC"}

# Raw Data Description
# time: Time of Message Reception
# model: SwitchDoc Labs F007TH Thermo-Hygrometer
# device: Serial Number of the sensor - changed on powerup but can be used to discriminate from other similar sensors in the area
# modelnumber: 
# channel: 
# battery: "OK" if battery good, "x" if battery is getting low
# temperature_F: temperature in F
# humidity: Relative Humidity in %.
# mic: "CRC" ???
def parseF016TH(sLine):
    data = json.loads(sLine)
    data['temperature'] = tempFtoC(data.get('temperature_F'))
    data.pop('temperature_F', None)
    return data

# Data Sample
# {"time" : "2020-11-22 06:40:15", "model" : "SwitchDoc Labs FT020T AIO", "device" : 12, "id" : 0, "batterylow" : 0, "avewindspeed" : 2, "gustwindspeed" : 3, "winddirection" : 18, "cumulativerain" : 180, "temperature" : 1011, "humidity" : 27, "light" : 1432, "uv" : 4, "mic" : "CRC"}

# Raw Data Description
# time: Time of Message Reception
# model: SwitchDoc Labs FT020T AIO
# device: Serial Number of the sensor - changed on powerup but can be used to discriminate from other similar sensors in the area
# batterylow: 0 if battery good, 1 if battery is getting low
# avewindspeed: Average Wind Speed in m/s *10
# gustwindspeed: Last Gust Speed in m/s *10
# winddirection: Wind Direction in degrees from 0-359.
# cumulativerain: Total rain since last reset or power off. in mm.*10
# temperature: outside temperature in F with 400 offset and *10 T = (value-400)/10.0
# humidity: Relative Humidity in %.
# light: Visible Sunlight in lux.
# uv: UV Index * 10
# "mic": "CRC" ???
def parseFT020T(sLine):
    data = json.loads(sLine)
    data['avewindspeed'] = data.get('avewindspeed')/10.0
    data['gustwindspeed'] = data.get('gustwindspeed')/10.0
    data['cumulativerain'] = data.get('cumulativerain')/10.0
    data['temperature'] = tempFtoC((data.get('temperature')-400)/10.0)
    data['uv'] = data.get('uv')/10.0
    return data

def run():

  # ---------------------------------------------------------------------------------------------------------------------------------------------------------------
  # 146 = FT-020T WeatherRack2, #147 = F016TH SDL Temperature/Humidity Sensor
  print("Starting Wireless Read")
  #cmd = [ '/usr/local/bin/rtl_433', '-vv',  '-q', '-F', 'json', '-R', '146', '-R', '147']
  cmd = [ '/usr/local/bin/rtl_433', '-q', '-F', 'json', '-R', '146', '-R', '147']


  #   We're using a queue to capture output as it occurs
  try:
      from Queue import Queue, Empty
  except ImportError:
      from queue import Queue, Empty  # python 3.x
  ON_POSIX = 'posix' in sys.builtin_module_names

  def enqueue_output(src, out, queue):
      for line in iter(out.readline, b''):
          queue.put(( src, line))
      out.close()

  #   Create our sub-process...
  #   Note that we need to either ignore output from STDERR or merge it with STDOUT due to a limitation/bug somewhere under the covers of "subprocess"
  #   > this took awhile to figure out a reliable approach for handling it...
  p = Popen( cmd, stdout=PIPE, stderr=STDOUT, bufsize=1, close_fds=ON_POSIX)
  q = Queue()

  t = Thread(target=enqueue_output, args=('stdout', p.stdout, q))

  t.daemon = True # thread dies with the program
  t.start()

  # ---------------------------------------------------------------------------------------------------------------------------------------------------------------

  pulse = 0
  while True:
      #   Other processing can occur here as needed...
      #sys.stdout.write('Made it to processing step. \n')

      try:
          src, line = q.get(timeout = 1)
          #print(line.decode())
      except Empty:
          pulse += 1
      else: # got line
          pulse -= 1
          sLine = line.decode()
          data = dict()
          topic = ""
          # print(sLine)
          #   See if the data is something we need to act on...
          if (( sLine.find('F007TH') != -1) or ( sLine.find('F016TH') != -1)):
              sys.stdout.write('WeatherSense Indoor T/H F016TH Found' + '\n')
              data = parseF016TH(sLine)
              topic = '/'.join(['weathersense', 'indoorth', str(data.get('channel'))])
          if (( sLine.find('FT0300') != -1) or ( sLine.find('FT020T') != -1)):
              sys.stdout.write('WeatherSense WeatherRack2 FT020T found' + '\n')
              data = parseFT020T(sLine)
              topic = '/'.join(['weathersense', 'weatherrack2', str(data.get('device'))])
          if topic:
              sys.stdout.write(json.dumps(data) + '\n')
              publish.single(topic.lower(), json.dumps(data), hostname='192.168.1.53')

      sys.stdout.flush()
