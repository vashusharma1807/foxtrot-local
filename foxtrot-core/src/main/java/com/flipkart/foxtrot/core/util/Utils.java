package com.flipkart.foxtrot.core.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Created By vashu.shivam on 14/10/23
 */
@NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
public class Utils {

    public static Map<String, Object> toMap(ObjectMapper mapper,
                                            Object value) {
        return mapper.convertValue(value, new TypeReference<Map<String, Object>>() {
        });
    }

}
