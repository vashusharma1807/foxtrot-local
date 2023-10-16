package com.flipkart.foxtrot.core.dao;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.common.TableFieldMapping;
import com.flipkart.foxtrot.core.querystore.SearchDatabaseConnection;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.google.common.base.Stopwatch;
import com.google.inject.Inject;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Singleton;
import lombok.val;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created By vashu.shivam on 15/09/23
 */

@Singleton
public class ElasticsearchStore implements SearchStore {

    private final ElasticsearchConnection elasticsearchConnection;
    private final ObjectMapper mapper;

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchStore.class);

    @Inject
    public ElasticsearchStore(ElasticsearchConnection elasticsearchConnection,
                              ObjectMapper mapper) {
        this.elasticsearchConnection = elasticsearchConnection;
        this.mapper = mapper;
    }


    public void saveCardinalityCache(String index,
                                     String table,
                                     TableFieldMapping tableFieldMapping) {
        try {
            this.elasticsearchConnection.getClient()
                    .index(new IndexRequest(index).id(table)
                                    .timeout(new TimeValue(2, TimeUnit.SECONDS))
                                    .source(mapper.writeValueAsBytes(tableFieldMapping), XContentType.JSON),
                            RequestOptions.DEFAULT);
        } catch (Exception e) {
            logger.error("Error in saving cardinality cache: " + e.getMessage(), e);
        }
    }

    public SearchDatabaseConnection getConnection() {
        return elasticsearchConnection;
    }

    @Override
    public void save(String indexName,
                     String id,
                     int timeOutInSeconds,
                     byte[] source,
                     String xContentType) throws IOException {
        elasticsearchConnection.getClient()
                .index(new IndexRequest(indexName).id(id)
                        .timeout(new TimeValue(timeOutInSeconds, TimeUnit.SECONDS))
                        .source(source, XContentType.fromMediaType(xContentType)), RequestOptions.DEFAULT);
    }

    @Override
    public <T extends Serializable> List<T> getSearchHitSourceAsString(Class<T> type,
                                                                       String indexName,
                                                                       String docType,
                                                                       int maxSize) throws IOException {
        List<T> searchHitSources = new ArrayList<>();
        SearchResponse response = elasticsearchConnection.getClient()
                .search(new SearchRequest(indexName).types(docType)
                        .indicesOptions(Utils.indicesOptions())
                        .source(new SearchSourceBuilder().size(maxSize)), RequestOptions.DEFAULT);
        for (SearchHit hit : com.collections.CollectionUtils.nullAndEmptySafeValueList(response.getHits()
                .getHits())) {
            searchHitSources.add(mapper.readValue(hit.getSourceAsString(), type));
        }
        return searchHitSources;
    }

    @Override
    public List<String> getAggregationSearchResults(String index,
                                                    String type,
                                                    String aggregationTerm,
                                                    String aggregationField) throws IOException {
        SearchResponse searchResponse = elasticsearchConnection.getClient()
                .search(new SearchRequest(index).types(type)
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(new SearchSourceBuilder().aggregation(AggregationBuilders.terms(aggregationTerm)
                                .field(aggregationField)
                                .size(1000))), RequestOptions.DEFAULT);

        Terms agg = searchResponse.getAggregations()
                .get(aggregationTerm);
        List<String> results = new ArrayList<>();
        for (Terms.Bucket entry : agg.getBuckets()) {
            results.add(entry.getKeyAsString());
        }
        return results;
    }

    @Override
    public <T extends Serializable> List<T> getQuerySearchHitSourceAsString(Class<T> type,
                                                                            String indexName,
                                                                            String docType,
                                                                            String queryField,
                                                                            String queryFieldValue,
                                                                            String sortBy,
                                                                            int from,
                                                                            int maxSize) throws IOException {
        SearchHits searchHits = elasticsearchConnection.getClient()
                .search(new SearchRequest(indexName).types(docType)
                        .searchType(SearchType.QUERY_THEN_FETCH)
                        .source(new SearchSourceBuilder().query(QueryBuilders.termQuery(queryField, queryFieldValue))
                                .sort(SortBuilders.fieldSort(sortBy)
                                        .order(SortOrder.DESC))
                                .from(from)
                                .size(maxSize)), RequestOptions.DEFAULT)
                .getHits();

        List<T> searchHitSources = new ArrayList<>();

        for (SearchHit searchHit : CollectionUtils.nullAndEmptySafeValueList(searchHits.getHits())) {
            searchHitSources.add(mapper.readValue(searchHit.getSourceAsString(), type));
        }
        return searchHitSources;
    }

    @Override
    public void optimize(int batchSize,
                         int segmentsToOptimize) throws IOException {

        val indexes = elasticsearchConnection.getClient()
                .indices()
                .get(new GetIndexRequest("*"), RequestOptions.DEFAULT)
                .getIndices();
        val candidateIndices = Arrays.stream(indexes)
                .filter(index -> {
                    String table = ElasticsearchUtils.getTableNameFromIndex(index);
                    if (Strings.isNullOrEmpty(table)) {
                        return false;
                    }
                    String currentIndex = ElasticsearchUtils.getCurrentIndex(table, System.currentTimeMillis());
                    String nextDayIndex = ElasticsearchUtils.getCurrentIndex(table,
                            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1));
                    return !index.equals(currentIndex) && !index.equals(nextDayIndex);
                })
                .collect(Collectors.toSet());
        List<List<String>> batchOfIndicesToOptimize = CollectionUtils.partition(candidateIndices, batchSize);
        for (List<String> indices : batchOfIndicesToOptimize) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            elasticsearchConnection.getClient()
                    .indices()
                    .forcemerge(new ForceMergeRequest(indices.toArray(new String[0])).maxNumSegments(segmentsToOptimize)
                            .flush(true)
                            .onlyExpungeDeletes(false), RequestOptions.DEFAULT);
            logger.info("No of indexes optimized : {}", indices.size());
            MetricUtil.getInstance()
                    .registerActionSuccess("indexesOptimized", CollectionUtils.mkString(indices, ","),
                            stopwatch.elapsed(TimeUnit.MILLISECONDS));
        }
    }

}
