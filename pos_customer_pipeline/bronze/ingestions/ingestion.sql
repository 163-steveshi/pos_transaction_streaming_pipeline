use catalog dbs_external_another;
use schema bronze;
-- create anexternal location if needed
create or refresh streaming live table raw_customers_sltl
--sets a table property (metadata) on the table for later organizting
tblproperties('quality'='bronze')
as 
select *,  current_timestamp() AS ingestion_time from cloud_files(
    's3://spark-read-study-bucket/pos_transaction_customer/',
    'csv',
    map('header','true', 'cloudFile.useS3NativeFileSystem','false', 'cloudFile.schemalocation','s3://spark-read-study-bucket/pos_transaction_customer/schema')
);
