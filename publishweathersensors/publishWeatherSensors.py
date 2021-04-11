# test for WeatherSense SwitchDoc Labs Weather Sensors
# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
# --------------------------------------------------------------------------------------------------------------------------------------------------------------
from os import EX_DATAERR
import sys
from subprocess import PIPE, Popen, STDOUT
from threading import Thread
import json
import datetime
import paho.mqtt.publish as publish
import dataclasses
from dataclasses import dataclass
from marshmallow import EXCLUDE, fields, ValidationError, validate
import desert

from apscheduler.schedulers.background import BackgroundScheduler


@dataclass
class RainfallTotal:
    zero: float = 0.0
    total: float = 0.0
    cumulative: float = 0.0

    def reset(self): 
        self.zero = self.cumulative

    def update(self, cumulative: float) -> float:
        self.cumulative = cumulative
        self.total = cumulative - self.zero
        return self.total
    
    def get_total(self) -> float:
        return self.total


daily_rainfall = RainfallTotal()
monthly_rainfall = RainfallTotal()
annual_rainfall = RainfallTotal()

def daily_rainfall_reset():
  daily_rainfall.reset()

def monthly_rainfall_reset():
  monthly_rainfall.reset()

def annual_rainfall_reset():
  annual_rainfall.reset()

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(func=daily_rainfall_reset, trigger='cron', hour='9')
scheduler.add_job(func=monthly_rainfall_reset, trigger='cron',
                  year='*', month='*', day=1)
scheduler.add_job(func=annual_rainfall_reset, trigger='cron',
                  year='*', day=1)
scheduler.start()

# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
#   A few helper functions...

def nowStr() -> str:
    return(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


# ---------------------------------------------------------------------------------------------------------------------------------------------------------------
#stripped = lambda s: "".join(i for i in s if 31 < ord(i) < 127)

def tempFtoC(temp_F: int) -> float:
    return (temp_F - 32) * (5/9.0)



@dataclass
class reportIndoorSensor:
    time: str
    model: str
    device: int
    modelnumber: int
    channel: int
    battery: str
    temperature: float
    humidity: float

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

@dataclass
class reportF016TH:
    time: str
    model: str
    device: int
    modelnumber: int
    channel: int
    battery: str
    temperature_F: float
    mic: str
    humidity: int = dataclasses.field(metadata=desert.metadata(
        fields.Int(
            required=True,
            validate=validate.Range(min=0, max=100, error="Humidity must be between 0% and 100%"))
    ))

    def to_reportIndoorSensor(self):
        return reportIndoorSensor(
            self.time,
            self.model,
            self.device,
            self.modelnumber,
            self.channel,
            self.battery,
            tempFtoC(self.temperature_F),
            self.humidity
        )

# def parseF016TH(sLine):
#     data =
#     data['temperature'] = tempFtoC(data.get('temperature_F'))
#     data.pop('temperature_F', None)
#     return data


@dataclass
class reportWeatherSensor:
    time: str
    model: str
    device: int
    avewindspeed: int
    gustwindspeed: int
    dailyrainfall: float
    monthlyrainfall: float
    annualrainfall: float
    temperature: float
    light: int
    uv: int
    batterylow: float
    winddirection: int
    humidity: float


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
@dataclass
class reportFT020T:
    time: str
    model: str
    device: int
    avewindspeed: int
    gustwindspeed: int
    cumulativerain: int
    temperature: float
    light: int
    uv: int
    mic: str
    batterylow: int = dataclasses.field(metadata=desert.metadata(
            fields.Int(
                required=True,
                validate=validate.Range(min=0, max=1, error="Battery state must be 0 or 1"))
    ))
    winddirection: float = dataclasses.field(metadata=desert.metadata(
            fields.Float(
                required=True,
                validate=validate.Range(min=0, max=359, error="Wind direction must be a value between 0 and 359"))
    ))
    humidity: int = dataclasses.field(metadata=desert.metadata(
            fields.Int(
                required=True,
                validate=validate.Range(min=0, max=100, error="Humidity must be between 0% and 100%"))
    ))

    def __post_init__(self):
        """
        Update the numeric fields to return correct values
        """
        self.avewindspeed =  self.avewindspeed/10.0
        self.gustwindspeed = self.gustwindspeed/10.0
        self.cumulativerain = self.cumulativerain/10.0
        self.temperature = tempFtoC((self.temperature-400)/10.0)
        self.uv = self.uv/10.0

    def to_reportWeatherSensor(self):
        return reportWeatherSensor(
            self.time,
            self.model,
            self.device,
            self.avewindspeed,
            self.gustwindspeed,
            daily_rainfall.update(self.cumulativerain),
            monthly_rainfall.update(self.cumulativerain),
            annual_rainfall.update(self.cumulativerain),
            self.temperature,
            self.light,
            self.uv,
            self.batterylow,
            self.winddirection,
            self.humidity
        )


def run():

    # ---------------------------------------------------------------------------------------------------------------------------------------------------------------
    # 146 = FT-020T WeatherRack2, #147 = F016TH SDL Temperature/Humidity Sensor
    print("Starting Wireless Read")
    #cmd = [ '/usr/local/bin/rtl_433', '-vv',  '-q', '-F', 'json', '-R', '146', '-R', '147']
    cmd = ['/usr/local/bin/rtl_433', '-q',
           '-F', 'json', '-R', '146', '-R', '147']

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
        #   Other processing can occur here as needed...
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
                schema = desert.schema(reportF016TH, meta={"unknown": EXCLUDE})
                try:
                    sys.stdout.write(sLine + '\n')
                    data = schema.load(json.loads(sLine)).to_reportIndoorSensor()
                    topic = '/'.join(['weathersense', 'indoorth',
                                     str(data.channel)])
                except ValidationError as err:
                    print(err.messages)
                    print(err.valid_data)
            if ((sLine.find('FT0300') != -1) or (sLine.find('FT020T') != -1)):
                sys.stdout.write(
                    'WeatherSense WeatherRack2 FT020T found' + '\n')
                # data = parseFT020T(sLine)
                schema = desert.schema(reportFT020T, meta={"unknown": EXCLUDE})
                try:
                    sys.stdout.write(sLine + '\n')
                    data = schema.load(json.loads(
                        sLine)).to_reportWeatherSensor()
                    topic = '/'.join(['weathersense', 'weatherrack2',
                                 str(data.get('device'))])
                except ValidationError as err:
                    print(err.messages)
                    print(err.valid_data)
            if topic:
                sys.stdout.write(json.dumps(data) + '\n')
                publish.single(topic.lower(), json.dumps(
                    data, cls=EnhancedJSONEncoder), hostname='192.168.1.53')

        sys.stdout.flush()
