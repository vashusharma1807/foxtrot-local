package com.flipkart.foxtrot.core.querystore;

import com.flipkart.foxtrot.core.querystore.impl.ElasticsearchConfig;
import io.dropwizard.lifecycle.Managed;

/**
 * Created By vashu.shivam on 15/09/23
 */
public interface SearchDatabaseConnection extends Managed {

     long getGetQueryTimeout();

}
