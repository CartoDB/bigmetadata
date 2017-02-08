DROP EXTENSION IF EXISTS cartodb CASCADE;
DROP EXTENSION IF EXISTS cdb_geocoder CASCADE;
DROP EXTENSION IF EXISTS plproxy CASCADE;
DROP EXTENSION IF EXISTS cdb_dataservices_server CASCADE;
DROP EXTENSION IF EXISTS cdb_dataservices_client CASCADE;
DROP SCHEMA IF EXISTS cdb_dataservices_client CASCADE;
DROP USER IF EXISTS publicuser;
CREATE USER publicuser;
CREATE EXTENSION cartodb;
CREATE EXTENSION cdb_geocoder;
CREATE EXTENSION plproxy;
CREATE EXTENSION cdb_dataservices_server;
CREATE EXTENSION cdb_dataservices_client;
INSERT INTO cartodb.cdb_conf VALUES ('logger_conf','{}');
INSERT INTO cartodb.cdb_conf VALUES ('redis_metadata_config',
  '{"timeout":1000,"redis_db":0,"redis_host": "localhost", "redis_port": 6379}');
INSERT INTO cartodb.cdb_conf VALUES ('redis_metrics_config',
  '{"timeout":1000,"redis_db":0,"redis_host": "localhost", "redis_port": 6379}');
INSERT INTO cartodb.cdb_conf VALUES ('heremaps_conf',
  '{"geocoder":{"app_id":"","app_code":"","geocoder_cost_per_hit":""},"isolines":{"app_id":"","app_code":""}}');
INSERT INTO cartodb.cdb_conf VALUES ('mapzen_conf',
  '{"matrix":{"api_key":"","monthly_quota":""},"routing":{"api_key":"","monthly_quota":""},"geocoder":{"api_key":"","monthly_quota":""}}');
INSERT INTO cartodb.cdb_conf VALUES ('data_observatory_conf',
  '{"period_end_date":"2100/1/1","connection":{"whitelist":[],"staging":"dbname=gis host=127.0.0.1 port=5432 user=docker password=docker","production":"dbname=gis host=127.0.0.1 port=5432 user=docker password=docker"}}');
INSERT INTO cartodb.cdb_conf VALUES ('user_config', '{"is_organization": false, "entity_name": "publicuser"}');
INSERT INTO cartodb.cdb_conf VALUES ('geocoder_server_config', '{"connection_str": "dbname=gis host=127.0.0.1 port=5432 user=docker password=docker"}');
