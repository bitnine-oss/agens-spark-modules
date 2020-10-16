%hive
use es_test
;

drop table if exists `artists`
;

-- https://www.elastic.co/guide/en/elasticsearch/hadoop/current/hive.html#_writing_data_to_elasticsearch_2
CREATE EXTERNAL TABLE if not exists `artists` (
    `id`    BIGINT,
    name    STRING,
    links   STRUCT< url:STRING, picture:STRING >
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
    'es.resource'='hive_artists',
    'es.nodes'='minmac:29200',
    'es.net.http.auth.user'='elastic',
    'es.net.http.auth.pass'='bitnine',
    'es.mapping.id'='id'
    'es.index.auto.create'='true'
)
;

desc `artists`
;

INSERT OVERWRITE TABLE `artists`
SELECT NULL, 'art01', named_struct('url', 'http://art01.com', 'picture', 'art01.png')
;

select * from `artists`
;

CREATE EXTERNAL TABLE if not exists `pictures` (
    `id`    BIGINT,
    name    STRING,
    links   ARRAY<STRING>
)
STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
    'es.resource'='hive_pictures',
    'es.nodes'='minmac:29200',
    'es.net.http.auth.user'='elastic',
    'es.net.http.auth.pass'='bitnine',
    'es.mapping.id'='id',
    'es.index.auto.create'='true'
)
;

INSERT OVERWRITE TABLE `pictures`
select 1001, 'sun-raise', Array('a','b','c','d')
;

select * from `pictures`
;


