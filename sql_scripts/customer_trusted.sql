CREATE EXTERNAL TABLE IF NOT EXISTS stedi_project_db.customer_trusted (
    serialnumber STRING,
    sharewithpublicasofdate BIGINT,
    birthday DATE,
    registrationdate BIGINT,
    sharewithresearchasofdate BIGINT,
    customername STRING,
    email STRING,
    lastupdatedate BIGINT,
    phone STRING,
    sharewithfriendsasofdate BIGINT
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('ignore.malformed.json' = 'true')
LOCATION 's3://jmk-stedi-lakehouse-project/customer_trusted/'
TBLPROPERTIES ('has_encrypted_data'='false');
