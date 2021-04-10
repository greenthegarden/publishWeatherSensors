from publishweathersensors import __version__
from publishweathersensors.publishWeatherSensors import tempFtoC, reportF016TH
import pytest
import desert
from marshmallow import EXCLUDE, ValidationError

def test_version():
    assert __version__ == '0.1.0'

def test_tempFtoC():
    assert tempFtoC(100) == pytest.approx(37.7778, 0.01)

def test_reportF016TH():
    test_data = {"time" : "2020-07-09 10:54:16", "new": 56, "model" : "SwitchDoc Labs F007TH Thermo-Hygrometer", "device" : "233", "modelnumber" : 5, "channel" : 3, "battery" : "OK", "temperature_F" : 72.100, "humidity" : 45, "mic" : "CRC"}
    schema = desert.schema(reportF016TH) #, meta={"unknown": EXCLUDE})
#     # data = schema.load(json.loads(sLine))
#     # assert data
    # dumped = desert.validate_and_dump(schema)
#     defs = dumped["definitions"]
#     assert "OuterSchema" in defs
    try:
      data = schema.load(test_data)
      print(data)
    except ValidationError as err:
      print(err.messages)
      print(err.valid_data)
    assert False
