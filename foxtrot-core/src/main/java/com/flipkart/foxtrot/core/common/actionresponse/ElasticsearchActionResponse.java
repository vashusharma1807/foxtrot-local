package com.flipkart.foxtrot.core.common.actionresponse;

import com.flipkart.foxtrot.core.common.SearchActionResponse;
import com.flipkart.foxtrot.core.querystore.actions.Utils;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality;

/**
 * Created By vashu.shivam on 23/10/23
 */
public abstract class ElasticsearchActionResponse extends ActionResponse implements SearchActionResponse {

    @Override
    public long getHitsForCountRequest(boolean isDistinct,
                                       String fieldName) {
        if (isDistinct) {
            Aggregations aggregations = ((SearchResponse)((ActionResponse) this)).getAggregations();
            Cardinality cardinality = aggregations.get(Utils.sanitizeFieldForAggregation(fieldName));
            if (cardinality == null) {
                return 0;
            } else {
                return cardinality.getValue();
            }
        } else {
            return ((SearchResponse)((ActionResponse) this)).getHits()
                    .getTotalHits();
        }
    }

}
