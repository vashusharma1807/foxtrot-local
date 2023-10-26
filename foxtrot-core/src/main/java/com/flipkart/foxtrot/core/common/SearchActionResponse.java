package com.flipkart.foxtrot.core.common;

/**
 * Created By vashu.shivam on 23/10/23
 */
public interface SearchActionResponse {

    long getHitsForCountRequest(boolean isDistinct,
                                String fieldName);
}
