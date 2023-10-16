package com.flipkart.foxtrot.core.dao;

import com.collections.CollectionUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.querystore.SearchDatabaseConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchUtils;
import com.flipkart.foxtrot.core.util.MetricUtil;
import com.google.common.base.Stopwatch;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.val;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created By vashu.shivam on 15/09/23
 */
@Singleton
public class OpensearchStore implements SearchStore {

    private final OpensearchConnection openSearchConnection;
    private final ObjectMapper mapper;

    private static final Logger logger = LoggerFactory.getLogger(OpensearchStore.class);

    @Inject
    public OpensearchStore(OpensearchConnection openSearchConnection,
                           ObjectMapper mapper) {
        this.openSearchConnection = openSearchConnection;
        this.mapper = mapper;
    }

    @Override
    public SearchDatabaseConnection getConnection() {
        return openSearchConnection;
    }

    @Override
    public void save(String indexName,
                     String id,
                     int timeOutInSeconds,
                     byte[] source,
                     String xContentType) throws IOException {
        openSearchConnection.getClient()
                .index(new IndexRequest(indexName).id(id)
                        .timeout(new TimeValue(timeOutInSeconds, TimeUnit.SECONDS))
                        .source(source, XContentType.fromMediaType(xContentType)), RequestOptions.DEFAULT);
    }

    @Override
    public <T extends Serializable> List<T> getSearchHitSourceAsString(final Class<T> type,
                                                                       String indexName,
                                                                       String docType,
                                                                       int maxSize) throws IOException {
        List<T> searchHitSources = new ArrayList<>();
        SearchResponse response = openSearchConnection.getClient()
                .search(new SearchRequest(indexName).indicesOptions(IndicesOptions.lenientExpandOpen())
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
        SearchResponse searchResponse = openSearchConnection.getClient()
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
        SearchHits searchHits = openSearchConnection.getClient()
                .search(new SearchRequest(indexName).searchType(SearchType.QUERY_THEN_FETCH)
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
        val indexes = openSearchConnection.getClient()
                .indices()
                .get(new GetIndexRequest("*"), RequestOptions.DEFAULT)
                .getIndices();
        val candidateIndices = Arrays.stream(indexes)
                .filter(index -> {
                    String table = OpensearchUtils.getTableNameFromIndex(index);
                    if (Strings.isNullOrEmpty(table)) {
                        return false;
                    }
                    String currentIndex = OpensearchUtils.getCurrentIndex(table, System.currentTimeMillis());
                    String nextDayIndex = OpensearchUtils.getCurrentIndex(table,
                            System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1));
                    return !index.equals(currentIndex) && !index.equals(nextDayIndex);
                })
                .collect(Collectors.toSet());
        List<List<String>> batchOfIndicesToOptimize = CollectionUtils.partition(candidateIndices, batchSize);
        for (List<String> indices : batchOfIndicesToOptimize) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            openSearchConnection.getClient()
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
