#!/bin/bash

ES_URL="minmac:29200"
ES_IDX="agensvertex2"
ES_USER="elastic"
ES_PSWD="bitnine"

echo "\n"
echo "** 0) delete index : ${ES_URL}/${ES_IDX}"
curl -X DELETE "${ES_URL}/${ES_IDX}" -u $ES_USER:$ES_PSWD
sleep 0.5

echo "\n"
echo "** 1) create index : ${ES_URL}/${ES_IDX}"
curl -X PUT "${ES_URL}/${ES_IDX}"  -u $ES_USER:$ES_PSWD -H 'Content-Type: application/json' -d'
{}
'
sleep 0.5

echo "\n"
echo "** 2) put mapping : ${ES_URL}/${ES_IDX}"
curl -X PUT "${ES_URL}/${ES_IDX}/_mapping" -u $ES_USER:$ES_PSWD -H 'Content-Type: application/json' -d'
{
  "dynamic": false,
  "properties":{
    "timestamp"  : { "type": "date" },
    "datasource" : { "type": "keyword", "ignore_above": 256 },
    "deleted"    : { "type": "keyword", "ignore_above": 2   },
    "id"         : { "type": "keyword", "ignore_above": 256 },
    "label"      : { "type": "keyword", "ignore_above": 256 },
    "properties" : {
      "type" : "nested",
      "properties": {
        "key"    : { "type": "keyword", "ignore_above": 256 },
        "type"   : { "type": "keyword", "ignore_above": 256 },
        "value"  : { "type": "text", "fields":{ "keyword": {"type":"keyword", "ignore_above": 256} } }
      }
    }
  }
}
'
sleep 0.5

echo "\n"
echo "** 3) check index : ${ES_URL}"
curl -X GET "${ES_URL}/${ES_IDX}?pretty=true" -u $ES_USER:$ES_PSWD

echo "\n"
echo "..done, Good-bye\n"