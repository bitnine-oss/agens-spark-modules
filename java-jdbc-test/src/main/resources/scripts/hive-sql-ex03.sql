%hive
use es_test
;

drop table if exists `agens_vertex2`
;

CREATE EXTERNAL TABLE if not exists `agens_vertex2` (
  `timestamp` TIMESTAMP,    -- default: current_timestamp()
  `datasource` STRING,
  `deleted` CHAR(1),        -- UPPERCASE: Y or N
  `id` STRING,
  `label` STRING,
  `properties` ARRAY<STRUCT<`key`:STRING,`type`:STRING,`value`:STRING>>
) STORED BY 'org.elasticsearch.hadoop.hive.EsStorageHandler'
TBLPROPERTIES(
  'es.resource'='agensvertex2',
  'es.nodes'='minmac:29200',            -- <IP>:<Port>
  'es.nodes.wan.only'='true',           -- if true, just use one node (performance down)
  'es.net.http.auth.user'='elastic',
  'es.net.http.auth.pass'='bitnine',
  'es.index.auto.create'='false',       -- prevent modification of index
  'es.write.operation'='upsert',        -- if exists same `ID`, then overwrite
  'es.query'='?q=datasource:modern AND deleted:N',  -- multiple: '%20AND%20'
  'es.read.field.as.array.include'='properties',        -- https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-field-info
  'es.mapping.id'='id'
  -- ,'es.mapping.names'='timestamp:timestamp, datasource:datasource, deleted:deleted, label:label, id:id, properties:properties'
)
;

-- hive-sql-ex02 참조
insert overwrite table `agens_vertex2`
select `timestamp`, datasource, 'N' as deleted, id, label, properties from `modern_vertex`
;

insert OVERWRITE TABLE `agens_vertex2`
select current_timestamp(), 'N', 'modern_101', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Agens'))
;

insert OVERWRITE TABLE `agens_vertex2`
select current_timestamp(), 'N', 'modern_102', 'modern', 'person', array(named_struct('key','name','type','java.lang.String','value','Graph'))
;

select * from `agens_vertex2`
;

-- update modern_test2 set properties = array(struct<>,..) where id = 'modern_101'
-- ==>
insert OVERWRITE TABLE `modern_test2`
select current_timestamp(), 'modern', 'N', 'modern_101', 'person', array(named_struct('key','name','type','java.lang.String','value','Thomas'),named_struct('key','age','type','java.lang.Integer','value','44'))
;

select * from `agens_vertex2`
;

-- delete from modern_test2 where id = 'modern_102'
-- ==>
insert OVERWRITE TABLE `agens_vertex2`
select `timestamp`, datasource, 'Y' as deleted, id, label, properties from agens_vertex2 where id in ('modern_102')
;

select * from `agens_vertex2`
;

-- ** FAILED
-- SemanticException [Error 10146]: Cannot truncate non-managed table agens_vertex2.
truncate table `agens_vertex2`
;
