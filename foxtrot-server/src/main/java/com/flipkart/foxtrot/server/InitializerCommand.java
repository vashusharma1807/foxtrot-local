/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.flipkart.foxtrot.server;

import com.flipkart.foxtrot.core.datastore.impl.hbase.HBaseUtil;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchUtils;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConfig;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchUtils;
import com.flipkart.foxtrot.core.table.impl.TableElasticsearchMapStore;
import com.flipkart.foxtrot.core.table.impl.TableOpensearchMapStore;
import com.flipkart.foxtrot.server.config.FoxtrotServerConfiguration;
import com.flipkart.foxtrot.server.console.impl.ElasticsearchConsolePersistence;
import com.flipkart.foxtrot.server.console.impl.OpensearchConsolePersistence;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreElasticsearchServiceImpl;
import com.flipkart.foxtrot.sql.fqlstore.FqlStoreOpensearchServiceImpl;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitializerCommand extends ConfiguredCommand<FoxtrotServerConfiguration> {

    private static final Logger logger = LoggerFactory.getLogger(InitializerCommand.class.getSimpleName());

    public InitializerCommand() {
        super("initialize", "Initialize elasticsearch and hbase");
    }

    @Override
    protected void run(Bootstrap<FoxtrotServerConfiguration> bootstrap,
                       Namespace namespace,
                       FoxtrotServerConfiguration configuration) throws Exception {
        if (configuration.getSearchDatabaseType()
                .equals(SearchDatabaseType.ELASTICSEARCH)) {
            initialiseElasticsearch(configuration);
        } else if (configuration.getSearchDatabaseType()
                .equals(SearchDatabaseType.OPENSEARCH)) {
            initialiseOpensearch(configuration);
        }
    }

    private void initialiseOpensearch(FoxtrotServerConfiguration configuration) throws Exception {
        OpensearchConfig opensearchConfig = configuration.getOpensearchConfig();
        OpensearchConnection opensearchConnection = new OpensearchConnection(opensearchConfig);
        opensearchConnection.start();

        try {
            org.opensearch.action.admin.cluster.health.ClusterHealthResponse clusterHealth = opensearchConnection.getClient()
                    .cluster()
                    .health(new org.opensearch.action.admin.cluster.health.ClusterHealthRequest(),
                            org.opensearch.client.RequestOptions.DEFAULT);
            int numDataNodes = clusterHealth.getNumberOfDataNodes();
            int numReplicas = (numDataNodes < 2)
                              ? 0
                              : 1;

            logger.info("# data nodes: {}, Setting replica count to: {}", numDataNodes, numReplicas);

            createOSMetaIndex(opensearchConnection, OpensearchConsolePersistence.INDEX, numReplicas);
            createOSMetaIndex(opensearchConnection, OpensearchConsolePersistence.INDEX_V2, numReplicas);
            createOSMetaIndex(opensearchConnection, TableOpensearchMapStore.TABLE_META_INDEX, numReplicas);
            createOSMetaIndex(opensearchConnection, OpensearchConsolePersistence.INDEX_HISTORY, numReplicas);
            createOSMetaIndex(opensearchConnection, FqlStoreOpensearchServiceImpl.FQL_STORE_INDEX, numReplicas);
            createOSMetaIndex(opensearchConnection, "user-meta", numReplicas);
            createOSMetaIndex(opensearchConnection, "tokens", numReplicas);

            logger.info("Creating mapping");
            org.opensearch.client.indices.PutIndexTemplateRequest putIndexTemplateRequest = OpensearchUtils.getClusterTemplateMapping();
            org.opensearch.action.support.master.AcknowledgedResponse response = opensearchConnection.getClient()
                    .indices()
                    .putTemplate(putIndexTemplateRequest, org.opensearch.client.RequestOptions.DEFAULT);
            logger.info("Created mapping: {}", response.isAcknowledged());
        } finally {
            opensearchConnection.stop();
        }
        logger.info("Creating hbase table");
        HBaseUtil.createTable(configuration.getHbase(), configuration.getHbase()
                .getTableName());
        logger.info("Initialization complete...");

    }

    private void initialiseElasticsearch(FoxtrotServerConfiguration configuration) throws Exception {
        ElasticsearchConfig esConfig = configuration.getElasticsearch();
        ElasticsearchConnection connection = new ElasticsearchConnection(esConfig);
        connection.start();

        try {
            ClusterHealthResponse clusterHealth = connection.getClient()
                    .cluster()
                    .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
            int numDataNodes = clusterHealth.getNumberOfDataNodes();
            int numReplicas = (numDataNodes < 2)
                              ? 0
                              : 1;

            logger.info("# data nodes: {}, Setting replica count to: {}", numDataNodes, numReplicas);

            createESMetaIndex(connection, ElasticsearchConsolePersistence.INDEX, numReplicas);
            createESMetaIndex(connection, ElasticsearchConsolePersistence.INDEX_V2, numReplicas);
            createESMetaIndex(connection, TableElasticsearchMapStore.TABLE_META_INDEX, numReplicas);
            createESMetaIndex(connection, ElasticsearchConsolePersistence.INDEX_HISTORY, numReplicas);
            createESMetaIndex(connection, FqlStoreElasticsearchServiceImpl.FQL_STORE_INDEX, numReplicas);
            createESMetaIndex(connection, "user-meta", numReplicas);
            createESMetaIndex(connection, "tokens", numReplicas);

            logger.info("Creating mapping");
            PutIndexTemplateRequest putIndexTemplateRequest = ElasticsearchUtils.getClusterTemplateMapping();
            AcknowledgedResponse response = connection.getClient()
                    .indices()
                    .putTemplate(putIndexTemplateRequest, RequestOptions.DEFAULT);
            logger.info("Created mapping: {}", response.isAcknowledged());
        } finally {
            connection.stop();
        }
        logger.info("Creating hbase table");
        HBaseUtil.createTable(configuration.getHbase(), configuration.getHbase()
                .getTableName());
        logger.info("Initialization complete...");
    }

    private void createESMetaIndex(final ElasticsearchConnection connection,
                                   final String indexName,
                                   int replicaCount) {
        try {
            if (connection.getClient()
                    .indices()
                    .exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
                logger.info("Index {} already exists. Nothing to do.", indexName);
                return;
            }

            logger.info("'{}' creation started", indexName);
            Settings settings = Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", replicaCount)
                    .build();
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName)
                    .settings(settings);
            CreateIndexResponse response = connection.getClient()
                    .indices()
                    .create(createIndexRequest, RequestOptions.DEFAULT);
            logger.info("'{}' creation acknowledged: {}", indexName, response.isAcknowledged());
            if(!response.isAcknowledged()) {
                logger.error("Index {} could not be created.", indexName);
            }
        } catch (Exception e) {
            if (null != e.getCause()) {
                logger.error("Index {} could not be created: {}", indexName, e.getCause()
                        .getLocalizedMessage());
            } else {
                logger.error("Index {} could not be created: {}", indexName, e.getLocalizedMessage());
            }
        }
    }

    private void createOSMetaIndex(final OpensearchConnection connection,
                                   final String indexName,
                                   int replicaCount) {
        try {
            if (connection.getClient()
                    .indices()
                    .exists(new org.opensearch.client.indices.GetIndexRequest(indexName),
                            org.opensearch.client.RequestOptions.DEFAULT)) {
                logger.info("Index {} already exists. Nothing to do.", indexName);
                return;
            }

            logger.info("'{}' creation started", indexName);
            org.opensearch.common.settings.Settings settings = org.opensearch.common.settings.Settings.builder()
                    .put("number_of_shards", 1)
                    .put("number_of_replicas", replicaCount)
                    .build();
            org.opensearch.client.indices.CreateIndexRequest createIndexRequest = new org.opensearch.client.indices.CreateIndexRequest(
                    indexName).settings(settings);
            org.opensearch.client.indices.CreateIndexResponse response = connection.getClient()
                    .indices()
                    .create(createIndexRequest, org.opensearch.client.RequestOptions.DEFAULT);
            logger.info("'{}' creation acknowledged: {}", indexName, response.isAcknowledged());
            if (!response.isAcknowledged()) {
                logger.error("Index {} could not be created.", indexName);
            }
        } catch (Exception e) {
            if (null != e.getCause()) {
                logger.error("Index {} could not be created: {}", indexName, e.getCause()
                        .getLocalizedMessage());
            } else {
                logger.error("Index {} could not be created: {}", indexName, e.getLocalizedMessage());
            }
        }
    }


}
