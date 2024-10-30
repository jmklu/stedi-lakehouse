CREATE EXTERNAL TABLE IF NOT EXISTS stedi_project_db.accelerometer_trusted (
    timestamp BIGINT,
    user STRING,
    x FLOAT,
    y FLOAT,
    z FLOAT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://jmk-stedi-lakehouse-project/accelerometer_trusted/'
TBLPROPERTIES ('has_encrypted_data'='false');
