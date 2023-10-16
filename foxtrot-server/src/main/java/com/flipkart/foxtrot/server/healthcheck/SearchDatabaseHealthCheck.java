package com.flipkart.foxtrot.server.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.flipkart.foxtrot.server.SearchDatabaseType;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.google.inject.Inject;

/**
 * Created By vashu.shivam on 15/10/23
 */
public class SearchDatabaseHealthCheck extends HealthCheck {

    final SearchDatabaseType databaseType;
    final ElasticsearchConnection elasticsearchConnection;
    final OpensearchConnection opensearchConnection;

    @Inject
    public SearchDatabaseHealthCheck(final FoxtrotServerConfiguration configuration,
                                     ElasticsearchConnection elasticsearchConnection,
                                     OpensearchConnection opensearchConnection) {
        this.databaseType = configuration.getSearchDatabaseType();
        this.elasticsearchConnection = elasticsearchConnection;
        this.opensearchConnection = opensearchConnection;
    }

    @Override
    protected Result check() throws Exception {
        return databaseType.equals(SearchDatabaseType.ELASTICSEARCH)
               ? new ElasticSearchHealthCheck(elasticsearchConnection).check()
               : new OpenSearchHealthCheck(opensearchConnection).check();
    }
}
