%hive
use es_test
;

drop table if exists `modern_vertex`
;

CREATE EXTERNAL TABLE if not exists `modern_vertex` (
  `timestamp` TIMESTAMP,    -- default: current_timestamp()
  `id` STRING,
  `datasource` STRING,
  `label` STRING,
  `properties` ARRAY<STRUCT<`key`:STRING,`type`:STRING,`value`:STRING>>
) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
  'es.nodes'='minmac:29200',            -- <IP>:<Port>
  'es.nodes.wan.only'='true',           -- if true, just use one node (performance down)
  'es.net.http.auth.user'='elastic',
  'es.net.http.auth.pass'='bitnine',
  'es.index.auto.create'='true',        -- prevent modification of index
  'es.write.operation'='upsert',        -- if exists same `ID`, then overwrite
  'es.resource'='agensvertex',
  'es.query'='?q=datasource:modern',
  'es.read.field.as.array.include'='properties',    -- https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-field-info
  'es.mapping.id'='id',
  'es.mapping.names'='timestamp:timestamp, datasource:datasource, label:label, id:id, properties:properties'
)
;

select * from modern_vertex
;

-- explode array of property
select id, label, property.key as p_key, property.type as p_type, property.value as p_value
from modern_vertex LATERAL VIEW explode(properties) propTable AS property
;

-- `order` operator make execution time long!!
select id, label, cast(property.value as int) as age_int
from (
    select label, id, property from modern_vertex LATERAL VIEW explode(properties) propTable AS property
) as tempTable
where property.key = 'age'
order by age_int desc
;

-- NAMED_STRUCT
select id, label, NAMED_STRUCT('key',property.key, 'type',property.type, 'value',property.value) as new_property
from (
    select id, label, property
    from modern_vertex LATERAL VIEW explode(properties) propTable AS property
) as tempTable
;

-- collect_set : group by to array
select id, label, collect_set(new_property) as arr_properties
from (
    select id, label, NAMED_STRUCT('key',property.key, 'type',property.type, 'value',property.value) as new_property
    from (
        select id, label, property
        from modern_vertex LATERAL VIEW explode(properties) propTable AS property
    ) as temp1
) as temp2
group by id, label
;

-- do not define column orders => (`timestamp`, id, datasource, label, properties)
-- when using `values` keyword => Unable to create temp file for insert values Expression of type TOK_FUNCTION not supported in insert/values
-- **ref https://cnpnote.tistory.com/entry/HADOOP-%EB%B0%B0%EC%97%B4-%EB%B3%B5%ED%95%A9-%EC%9C%A0%ED%98%95-%EC%97%B4%EC%97%90-%EA%B0%92-%EC%82%BD%EC%9E%85-%ED%95%98%EC%9D%B4%EB%B8%8C
insert overwrite table modern_vertex
values(
    current_timestamp(), 'modern_101', 'modern', 'person',
    array(named_struct('key','name','type','java.lang.String','value','Thomas'))
)
;

-- **NOTE: `timestamp` date 에 format 을 지정하면 MapRedTask 에서 오류 발생!
-- ==> ES mapping 설정시 date 에 format 제거하면 잘 됨

-- Execution Error, return code 2 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask
-- insert overwrite table modern_vertex
insert into table modern_vertex
select current_timestamp(), 'modern_101', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Thomas'))
;

