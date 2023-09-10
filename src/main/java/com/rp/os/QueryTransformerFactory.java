package com.rp.os;

import co.elastic.clients.elasticsearch._types.query_dsl.Query;

public class QueryTransformerFactory {

    private QueryTransformerFactory() {
        throw new IllegalStateException();
    }

    public static <T> QueryTransformer<?> getQueryTransformer(String input, String output) {

        if ("KQL".equalsIgnoreCase(input) && "ElasticSearch".equalsIgnoreCase(output)) {
            return new KqlToEsTransfomer();
        } else {
            throw new UnsupportedOperationException("Unimplemented transformation!");
        }

    }

}
