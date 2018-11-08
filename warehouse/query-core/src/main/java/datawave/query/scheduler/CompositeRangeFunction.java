package datawave.query.scheduler;

import com.google.common.base.Function;
import datawave.webservice.query.configuration.QueryData;

import javax.annotation.Nullable;

public class CompositeRangeFunction implements Function<QueryData,QueryData> {
    
    @Nullable
    @Override
    public QueryData apply(@Nullable QueryData input) {
        return input;
    }
}
