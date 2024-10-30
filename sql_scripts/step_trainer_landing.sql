CREATE EXTERNAL TABLE IF NOT EXISTS stedi_project_db.step_trainer_landing (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://jmk-stedi-lakehouse-project/step_trainer_landing/'
TBLPROPERTIES ('has_encrypted_data'='false');
