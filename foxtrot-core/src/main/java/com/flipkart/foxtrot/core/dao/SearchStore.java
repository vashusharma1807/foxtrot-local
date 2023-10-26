package com.flipkart.foxtrot.core.dao;

import com.flipkart.foxtrot.common.ActionRequest;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.core.common.SearchActionRequest;
import com.flipkart.foxtrot.core.common.SearchActionResponse;
import com.flipkart.foxtrot.core.querystore.SearchDatabaseConnection;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Created By vashu.shivam on 15/09/23
 */
public interface SearchStore {

     SearchDatabaseConnection getConnection();

     void save(String indexName,
               String id,
               int timeOutInSeconds,
               byte[] source,
               String xContentType) throws IOException;

     <T extends Serializable> List<T> getSearchHitSourceAsString(final Class<T> type,
                                                                 String indexName,
                                                                 String docType,
                                                                 int maxSize) throws IOException;

     List<String> getAggregationSearchResults(String index,
                                              String type,
                                              String aggregationTerm,
                                              String aggregationField) throws IOException;

     <T extends Serializable> List<T> getQuerySearchHitSourceAsString(final Class<T> type,
                                                                      String indexName,
                                                                      String docType,
                                                                      String queryField,
                                                                      String queryFieldValue,
                                                                      String sortBy,
                                                                      int from,
                                                                      int maxSize) throws IOException;

     void optimize(int batchSize,
                   int segmentsToOptimize) throws IOException;

     SearchActionRequest createSearchRequest(CountRequest countRequest,
                                             List<Filter> extraFilters,
                                             int precisionThreshold);

     SearchActionResponse getSearchResults(SearchActionRequest request) throws IOException;

     //ActionResponse getResponse();

//     @Override
//     public ActionResponse getResponse(org.elasticsearch.action.ActionResponse response, CountRequest parameter) {
//          if (parameter.isDistinct()) {
//               Aggregations aggregations = ((SearchResponse) response).getAggregations();
//               Cardinality cardinality = aggregations.get(Utils.sanitizeFieldForAggregation(parameter.getField()));
//               if (cardinality == null) {
//                    return new CountResponse(0);
//               }
//               else {
//                    return new CountResponse(cardinality.getValue());
//               }
//          }
//          else {
//               return new CountResponse(((SearchResponse) response).getHits()
//                       .getTotalHits());
//          }
//
//     }
}
