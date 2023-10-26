package com.flipkart.foxtrot.core.querystore.actions;

import com.flipkart.foxtrot.common.ActionResponse;
import com.flipkart.foxtrot.common.count.CountRequest;
import com.flipkart.foxtrot.common.count.CountResponse;
import com.flipkart.foxtrot.common.query.Filter;
import com.flipkart.foxtrot.common.query.general.ExistsFilter;
import com.flipkart.foxtrot.common.util.CollectionUtils;
import com.flipkart.foxtrot.common.visitor.CountPrecisionThresholdVisitorAdapter;
import com.flipkart.foxtrot.core.common.Action;
import com.flipkart.foxtrot.core.common.SearchActionRequest;
import com.flipkart.foxtrot.core.common.SearchActionResponse;
import com.flipkart.foxtrot.core.config.SearchDatabaseTuningConfig;
import com.flipkart.foxtrot.core.exception.FoxtrotExceptions;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsLoader;
import com.flipkart.foxtrot.core.querystore.actions.spi.AnalyticsProvider;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by rishabh.goyal on 02/11/14.
 */
//TODO - Whole Querystore
@AnalyticsProvider(opcode = "count", request = CountRequest.class, response = CountResponse.class, cacheable = true)
public class CountAction extends Action<CountRequest> {

    private final SearchDatabaseTuningConfig searchDatabaseTuningConfig;

    public CountAction(CountRequest parameter, AnalyticsLoader analyticsLoader) {
        super(parameter, analyticsLoader);
        this.searchDatabaseTuningConfig = analyticsLoader.getSearchDatabaseTuningConfig();
    }


    @Override
    public void preprocess() {
        getParameter().setTable(ElasticsearchUtils.getValidTableName(getParameter().getTable()));
        // Null field implies complete doc count
        if (getParameter().getField() != null) {
            Filter existsFilter = new ExistsFilter(getParameter().getField());
            if (!getParameter().getFilters().contains(existsFilter)) {
                getParameter().getFilters()
                        .add(new ExistsFilter(getParameter().getField()));
            }
        }
    }

    @Override
    public String getMetricKey() {
        return getParameter().getTable();
    }

    @Override
    public String getRequestCacheKey() {
        preprocess();
        long filterHashKey = 0L;
        CountRequest request = getParameter();
        if (null != request.getFilters()) {
            for (Filter filter : request.getFilters()) {
                filterHashKey += 31 * filter.hashCode();
            }
        }

        filterHashKey += 31 * (request.isDistinct()
                               ? "TRUE".hashCode()
                               : "FALSE".hashCode());
        filterHashKey += 31 * (request.getField() != null
                               ? request.getField()
                                       .hashCode()
                               : "COLUMN".hashCode());
        return String.format("count-%s-%d", request.getTable(), filterHashKey);
    }

    @Override
    public void validateImpl(CountRequest parameter) {
        List<String> validationErrors = new ArrayList<>();
        if (CollectionUtils.isNullOrEmpty(parameter.getTable())) {
            validationErrors.add("table name cannot be null or empty");
        }
        if (parameter.isDistinct() && CollectionUtils.isNullOrEmpty(parameter.getField())) {
            validationErrors.add("field name cannot be null or empty");
        }
        if (!CollectionUtils.isNullOrEmpty(validationErrors)) {
            throw FoxtrotExceptions.createMalformedQueryException(parameter, validationErrors);
        }
    }

    @Override
    public ActionResponse execute(CountRequest parameter) {
        SearchActionRequest request = getRequestBuilder(parameter, Collections.emptyList());

        try {
            SearchActionResponse response = getSearchStore().getSearchResults(request);
            return getResponse(response, parameter);
        }
        catch (IOException e) {
            throw FoxtrotExceptions.createQueryExecutionException(parameter, e);
        }
    }

    @Override
    public SearchActionRequest getRequestBuilder(CountRequest parameter,
                                                 List<Filter> extraFilters) {
        try {
            return getSearchStore().createSearchRequest(parameter, extraFilters, parameter.accept(
                    new CountPrecisionThresholdVisitorAdapter(searchDatabaseTuningConfig.getPrecisionThreshold())));
        } catch (Exception e) {
            throw FoxtrotExceptions.queryCreationException(parameter, e);
        }
    }

    @Override
    public ActionResponse getResponse(SearchActionResponse response,
                                      CountRequest parameter) {

        return new CountResponse(response.getHitsForCountRequest(parameter.isDistinct(), parameter.getField()));
    }
}
