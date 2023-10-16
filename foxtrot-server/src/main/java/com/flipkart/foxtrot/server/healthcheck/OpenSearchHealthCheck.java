/**
 * Copyright 2014 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.flipkart.foxtrot.server.healthcheck;

import com.codahale.metrics.health.HealthCheck;
import com.flipkart.foxtrot.core.querystore.impl.OpensearchConnection;
import com.google.inject.Inject;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.client.RequestOptions;

/**
 * Created by rishabh.goyal on 15/05/14.
 */

public class OpenSearchHealthCheck extends HealthCheck {

    private OpensearchConnection opensearchConnection;

    @Inject
    public OpenSearchHealthCheck(OpensearchConnection opensearchConnection) {
        this.opensearchConnection = opensearchConnection;
    }

    @Override
    protected Result check() throws Exception {
        ClusterHealthResponse response = opensearchConnection.getClient()
                .cluster()
                .health(new ClusterHealthRequest(), RequestOptions.DEFAULT);
        return (response.getStatus()
                .name()
                .equalsIgnoreCase("GREEN") || response.getStatus()
                .name()
                .equalsIgnoreCase("YELLOW"))
               ? Result.healthy()
               : Result.unhealthy("Cluster unhealthy");
    }
}
