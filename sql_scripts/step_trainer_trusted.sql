CREATE EXTERNAL TABLE IF NOT EXISTS stedi_project_db.step_trainer_trusted (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject FLOAT,
    x FLOAT,
    y FLOAT,
    z FLOAT,
    timestamp BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://jmk-stedi-lakehouse-project/step_trainer_trusted/'
TBLPROPERTIES ('has_encrypted_data'='false');
