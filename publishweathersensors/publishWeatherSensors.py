from apscheduler.schedulers.background import BackgroundScheduler
from os import EX_DATAERR
import sys
from subprocess import PIPE, Popen, STDOUT
from threading import Thread
import json
import datetime
import redis
import paho.mqtt.publish as publish
# import dataclasses
from pydantic.dataclasses import dataclass
from pydantic import (
  BaseModel,
  confloat,
  conint,
  validator,
  ValidationError
)

# cfg = config_from_json('config.json', read_from_file=True)

r = redis.Redis(
  host='127.0.0.1',
  port=6379)

@dataclass
class RainfallTotal:
  key: str
  zero: float = 0.0
  total: float = 0.0
  cumulative: float = 0.0

  def set_redis(self):
    r.set(self.key, self.zero)

  def get_redis(self):
    self.zero = r.get(self.key)

  def reset(self): 
    self.zero = self.cumulative
    self.set_redis()

  def update(self, cumulative: float) -> float:
    self.cumulative = cumulative
    self.total = cumulative - self.zero
    return self.total

  def get_total(self) -> float:
    return self.total

  def __post_init__(self):
    try:
      self.get_redis()
    except:
      self.zero = 0.0

daily_rainfall = RainfallTotal('zero_daily')
# monthly_rainfall = RainfallTotal('zero_monthly')
# annual_rainfall = RainfallTotal('zero_annual')

# daily_rainfall = 0.0
# monthly_rainfall = 0.0
# annual_rainfall = 0.0

def daily_rainfall_reset():
  daily_rainfall.reset()

# def monthly_rainfall_reset():
#   monthly_rainfall.reset()

# def annual_rainfall_reset():
#   annual_rainfall.reset()

# scheduler = BackgroundScheduler(daemon=True)
# scheduler.add_job(func=daily_rainfall_reset, trigger='cron', hour='9')
# scheduler.add_job(func=monthly_rainfall_reset, trigger='cron',
#                   year='*', month='*', day=1)
# scheduler.add_job(func=annual_rainfall_reset, trigger='cron',
#                   year='*', day=1)
# scheduler.start()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
#   A few helper functions...

def nowStr() -> str:
  return(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
#stripped = lambda s: "".join(i for i in s if 31 < ord(i) < 127)

def tempFtoC(temp_F: int) -> float:
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

class reportF016TH(BaseModel):
  time: str
  model: str
  device: int
  modelnumber: int
  channel: int
  battery: str
  temperature_F: float
  humidity: conint(ge=0, le=100)
  mic: str

class reportIndoorSensor(BaseModel):
  time: str
  model: str
  device: int
  modelnumber: int
  channel: int
  battery: str
  temperature: float
  humidity: conint(ge=0, le=100)

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
# @dataclass

class reportFT020T(BaseModel):
  time: str
  model: str
  device: conint(ge=0)
  id: conint(ge=0)
  batterylow: conint(ge=0, le=1)
  avewindspeed: conint(ge=0)
  gustwindspeed: conint(ge=0)
  winddirection: conint(ge=0, le=359)
  cumulativerain: conint(ge=0)
  temperature: confloat(ge=0, le=2000)
  humidity: conint(ge=0, le=100)
  light: conint(ge=0)
  uv: conint(ge=0)
  mic: str

  @validator('avewindspeed', always=True)
  def avewindspeed_correction(cls, v):
    return v/10.0

  @validator('gustwindspeed', always=True)
  def gustwindspeed_correction(cls, v):
    return v/10.0

  @validator('cumulativerain', always=True)
  def cumulativerain_correction(cls, v):
    return v/10.0

  @validator('temperature', always=True)
  def temperature_correction(cls, v):
    return tempFtoC((v-400)/10.0)

  @validator('uv', always=True)
  def uv_correction(cls, v):
    return v/10.0

class reportWeatherSensor(BaseModel):
  report: reportFT020T
  dailyrainfall: confloat(ge=0)
  # monthlyrainfall: confloat(ge=0)
  # annualrainfall: confloat(ge=0)

def run():

  # try:
  #   report = reportF016TH(time="2020-07-09 10:54:16",
  #                         model="SwitchDoc Labs F007TH Thermo-Hygrometer",
  #                         device=233,
  #                         modelnumber=5,
  #                         channel=3,
  #                         battery="OK",
  #                         temperature_F=72.100,
  #                         humidity=45,
  #                         mic="CRC")
  #   print("report = " + report.json())
  #   reportis = reportIndoorSensor(time=report.time,
  #                                  model=report.model,
  #                                  device=report.device,
  #                                  modelnumber=report.modelnumber,
  #                                  channel=report.channel,
  #                                  battery=report.battery,
  #                                  temperature=tempFtoC(report.temperature_F),
  #                                  humidity=report.humidity)
  #   print("reportis = " + reportis.json())
  # except ValidationError as err:
  #   print(err.json())

  # try:
  #   report = reportFT020T(time="2020-11-22 06:40:15",
  #                         model="SwitchDoc Labs FT020T AIO",
  #                         device=12,
  #                         id=0,
  #                         batterylow=0,
  #                         avewindspeed=2,
  #                         gustwindspeed=3,
  #                         winddirection=18,
  #                         cumulativerain=190,
  #                         temperature=1011,
  #                         humidity=27,
  #                         light=1432,
  #                         uv=4,
  #                         mic="CRC")
  #   print("report = " + report.json())
  #   reportws = reportWeatherSensor(report=report,
  #   dailyrainfall = daily_rainfall.update(report.cumulativerain),
  #   monthlyrainfall = monthly_rainfall.update(report.cumulativerain),
  #   annualrainfall = annual_rainfall.update(report.cumulativerain)
  #   )
  #   print("reportws = " + reportws.json())
  # except ValidationError as err:
  #   print(err.json())
    
  # ---------------------------------------------------------------------------------------------------------------------------------------------------------------
  
  # 146 = FT-020T WeatherRack2, #147 = F016TH SDL Temperature/Humidity Sensor
  print("Starting Wireless Read")
  #cmd = [ '/usr/local/bin/rtl_433', '-vv',  '-q', '-F', 'json', '-R', '146', '-R', '147']
  cmd = ['/usr/local/bin/rtl_433', '-q', '-F', 'json', '-R', '146', '-R', '147']

  #   We're using a queue to capture output as it occurs
  try:
    from Queue import Queue, Empty
  except ImportError:
    from queue import Queue, Empty  # python 3.x
  ON_POSIX = 'posix' in sys.builtin_module_names

  def enqueue_output(src, out, queue):
    for line in iter(out.readline, b''):
      queue.put((src, line))
    out.close()

  #   Create our sub-process...
  #   Note that we need to either ignore output from STDERR or merge it with STDOUT due to a limitation/bug somewhere under the covers of "subprocess"
  #   > this took awhile to figure out a reliable approach for handling it...
  p = Popen(cmd, stdout=PIPE, stderr=STDOUT, bufsize=1, close_fds=ON_POSIX)
  q = Queue()

  t = Thread(target=enqueue_output, args=('stdout', p.stdout, q))

  t.daemon = True  # thread dies with the program
  t.start()

  # ---------------------------------------------------------------------------------------------------------------------------------------------------------------

  pulse = 0
  while True:
    #Other processing can occur here as needed...
    #sys.stdout.write('Made it to processing step. \n')

    try:
      src, line = q.get(timeout=1)
      # print(line.decode())
    except Empty:
      pulse += 1
    else:  # got line
      pulse -= 1
      sLine = line.decode()
      data = dict()
      topic = ""
      # print(sLine)
      #   See if the data is something we need to act on...
      if ((sLine.find('F007TH') != -1) or (sLine.find('F016TH') != -1)):
        sys.stdout.write('WeatherSense Indoor T/H F016TH Found' + '\n')
        # data = parseF016TH(sLine)
        # schema = desert.schema(reportF016TH, meta={"unknown": EXCLUDE})
        try:
          sys.stdout.write(sLine + '\n')
          report = reportF016TH.parse_raw(sLine)
          data = json.loads(reportIndoorSensor(time=report.time,
                                                model=report.model,
                                                device=report.device,
                                                modelnumber=report.modelnumber,
                                                channel=report.channel,
                                                battery=report.battery,
                                                temperature=tempFtoC(
                                                    report.temperature_F),
                                                humidity=report.humidity
                                                ).json())
          topic = '/'.join(['weathersense', 'indoorth',
                            str(data.get('channel'))])
        except ValidationError as err:
          print(str(err))
      if ((sLine.find('FT0300') != -1) or (sLine.find('FT020T') != -1)):
        sys.stdout.write('WeatherSense WeatherRack2 FT020T found' + '\n')
        # data = parseFT020T(sLine)
        # schema = desert.schema(reportFT020T, meta={"unknown": EXCLUDE})
        try:
          sys.stdout.write(sLine + '\n')
          report = reportFT020T.parse_raw(sLine)
          data = json.loads(reportWeatherSensor(report=report,
                                                dailyrainfall=daily_rainfall.update(
                                                    report.cumulativerain)
                                                ).json())
          topic = '/'.join(['weathersense', 'weatherrack2', str(data.get('device'))])
        except ValidationError as err:
          print(str(err))
      if topic:
        sys.stdout.write(json.dumps(data) + '\n')
        # publish.single(topic.lower(), json.dumps(
        #     data), hostname=cfg.broker_ip)
        publish.single(topic.lower(), json.dumps(data), hostname='192.168.1.53')

    sys.stdout.flush()

                                                # monthlyrainfall=monthly_rainfall.update(
                                                #     report.cumulativerain),
                                                # annualrainfall=annual_rainfall.update(
                                                #     report.cumulativerain)
