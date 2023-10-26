package com.flipkart.foxtrot.core.common.actionrequest;

import com.flipkart.foxtrot.core.common.SearchActionRequest;
import org.elasticsearch.action.ActionRequest;

/**
 * Created By vashu.shivam on 23/10/23
 */
public abstract class ElasticsearchActionRequest extends ActionRequest implements SearchActionRequest {

}
