package net.bitnine.agens.spark.elastic;

import net.bitnine.agens.spark.AgensConf;
import net.bitnine.agens.spark.AgensJavaHelper;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import scala.Tuple3;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;


public final class AgensJavaElastic {

    private static final Logger LOG = Logger.getLogger(AgensJavaElastic.class);

    private final AgensConf conf;
    private RestHighLevelClient client = null;

    public AgensJavaElastic(AgensConf conf){
        assert( conf != null && conf.host() != null && conf.port() != null );
        this.conf = conf;
    }

    private RestHighLevelClient open(){
        RestClientBuilder builder = RestClient.builder(new HttpHost(conf.host(), Integer.parseInt(conf.port())));

        if( conf.user() != null && conf.password() != null ){
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(conf.user(), conf.password()));
            // login : HttpClientConfigCallback()
            builder.setHttpClientConfigCallback(httpClientBuilder ->
                    httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }

        return new RestHighLevelClient(builder);
    }

    private void close(){
        if( client != null ){
            try {
                client.close();
            } catch(Exception e){
                // An empty catch block
            }
            client = null;
        }
    }

    public scala.collection.immutable.Map<String, Long> datasourcesToScala(String index){
        return AgensJavaHelper.toScalaMap(datasources(index));
    }

    public Map<String,Long> datasources(String index){
        if( client == null ) client = open();

        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.terms("datasources")
                        .field("datasource").order(BucketOrder.key(true)));

        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);

        Map<String, Long> result = new HashMap<>();
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // response
            Aggregations aggregations = searchResponse.getAggregations();
            Terms labels = aggregations.get("datasources");
            labels.getBuckets().forEach(b -> result.put(b.getKeyAsString(), b.getDocCount()));
        }
        catch (Exception ex) {
            // An empty catch block
        }
        finally { close(); }

        return result;
    }

    public scala.collection.immutable.Map<String, Long> labelsToScala(String index, String datasource){
        return AgensJavaHelper.toScalaMap(labels(index, datasource));
    }

    public Map<String, Long> labels(String index, String datasource) {
        if( client == null ) client = open();

        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.boolQuery()
                .must(termQuery("datasource", datasource)))
                .aggregation(AggregationBuilders.terms("labels")
                        .field("label").order(BucketOrder.key(true)));

        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);

        Map<String, Long> result = new HashMap<>();
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // response
            Aggregations aggregations = searchResponse.getAggregations();
            Terms labels = aggregations.get("labels");
            for (Terms.Bucket b : labels.getBuckets()) {
                result.put(b.getKeyAsString(), b.getDocCount());
            }
        }
        catch (Exception ex){
            // An empty catch block
        }
        finally { close(); }

        return result;
    }

    public scala.collection.immutable.Map<String, Long> keysToScala(String index, String datasource, String label){
        return AgensJavaHelper.toScalaMap(keys(index, datasource, label));
    }

    public Map<String, Long> keys(String index, String datasource, String label) {
        if( client == null ) client = open();

        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // searchSourceBuilder.size(0);

        searchSourceBuilder
            .query(QueryBuilders.boolQuery()
                .must(termQuery("datasource", datasource))
                .filter(termQuery("label", label))
            )
            .aggregation(AggregationBuilders.nested("propAgg", "properties")
                .subAggregation(AggregationBuilders.terms("keyAgg").field("properties.key"))
            );
        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);

        Map<String, Long> result = new HashMap<>();
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // response
            Aggregations aggregations = searchResponse.getAggregations();
            Nested agg = aggregations.get("propAgg");
            Terms keys = agg.getAggregations().get("keyAgg");
            keys.getBuckets().forEach(b -> result.put(b.getKeyAsString(), b.getDocCount()));
        }
        catch (Exception ex){
            // An empty catch block
        }
        finally { close(); }

        return result;
    }

    public scala.collection.immutable.Map<String,Tuple3<String,Long,Boolean>> keytypesToScala(String index, String datasource, String label){
        return AgensJavaHelper.toScalaMap(keytypes(index, datasource, label));
    }

    public Map<String,Tuple3<String,Long,Boolean>> keytypes(String index, String datasource, String label) {
        if( client == null ) client = open();

        // query : aggregation
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // searchSourceBuilder.size(0);

        searchSourceBuilder
                .query(QueryBuilders.boolQuery()
                        .must(termQuery("datasource", datasource))
                        .filter(termQuery("label", label))
                )
                .aggregation(AggregationBuilders.nested("propAgg", "properties")
                        .subAggregation(AggregationBuilders.terms("keyAgg").field("properties.key")
                                .subAggregation(AggregationBuilders.terms("typeAgg").field("properties.type")
                                        .order(BucketOrder.count(false)).size(1)
                                )
                        )
                );
        // request
        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(searchSourceBuilder);

        Map<String, Tuple3<String,Long,Boolean>> result = new HashMap<>();
        try {
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // response
            Aggregations aggregations = searchResponse.getAggregations();
            Nested agg = aggregations.get("propAgg");
            Terms keyAgg = agg.getAggregations().get("keyAgg");
            keyAgg.getBuckets().forEach(b -> {
                // keys : doc count
                String keyValue = b.getKeyAsString();
                long docCount = b.getDocCount();

                // keys' Meta : type, count by agg, hasNull
                Terms typeAgg = b.getAggregations().get("typeAgg");
                String typeValue = typeAgg.getBuckets().get(0).getKeyAsString();
                long aggCount = typeAgg.getBuckets().get(0).getDocCount();
                result.put(keyValue, new Tuple3<>(typeValue, aggCount, docCount != aggCount));
            });
        }
        catch (Exception ex){
            // An empty catch block
        }
        finally { close(); }

        return result;
    }
}


/*
GET agensvertex/_search
{
  "size": 0,
  "query": {
    "bool": {
        "must": [
          {"term": {"datasource": "modern" }},
          {"term": {"label": "person" }}
        ]
    }
  },
  "aggs": {
    "props": {
      "nested": {
        "path": "properties"
      },
      "aggs": {
        "keys": {
          "terms": {
            "field": "properties.key"
          },
          "aggs": {
            "key_types": {
              "terms": {
                "field": "properties.type",
                "order": { "_count": "desc" },
                "size": 1
              }
            }
          }
        }
      }
    }
  }
}
 */