from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='172.16.1.30:9095',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

message = {
    "InstrumentName": "Machine001",
    "LocationName": "LabA",
    "centerId": 1,
    "LogDate": "2026-01-01T15:00:00",
    "BuildVersion": "1.0.2",
    "BuildDate": "2026-01-01",
    "IsSync": 0
}

producer.send("dbo.AppVersionLog", value=message)
producer.flush()
