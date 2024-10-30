CREATE EXTERNAL TABLE IF NOT EXISTS stedi_project_db.machine_learning_curated (
    sensorReadingTime BIGINT,
    serialNumber STRING,
    distanceFromObject FLOAT,
    x FLOAT,
    y FLOAT,
    z FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://jmk-stedi-lakehouse-project/machine_learning_curated/'
TBLPROPERTIES ('has_encrypted_data'='false');
