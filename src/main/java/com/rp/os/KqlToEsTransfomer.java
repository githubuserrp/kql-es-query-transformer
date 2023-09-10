package com.rp.os;

import com.fasterxml.jackson.databind.JsonNode;
import com.rp.os.logicaloperators.LogicalOperator;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.MultiMatchQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.TextQueryType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KqlToEsTransfomer implements QueryTransformer<JsonNode> {
    Logger log = LoggerFactory.getLogger(KqlToEsTransfomer.class);
    private static final String REGEX_FOR_SPLITING_KQL_QUERY = "[\\h-]";
    private static final String SPACE = " ";
    private static final String EMPTY = "";
    private static final String DUMMY_TOKEN = "KqlToEsTransfomer_DUMMY_TOKEN";

    // @Override
    public JsonNode transform(String query) {

        log.info("Received this string for transformation:{}", query);

        frameESQuery(getTokensAsFIFOCollection(query));

        return null;
    }

    public Deque<String> getTokensAsFIFOCollection(String input) {
        String[] tokens = input.split(REGEX_FOR_SPLITING_KQL_QUERY);
        return new LinkedList<>(Arrays.asList(tokens));
    }

    public List<String> getTokensAsList(String input) {
        return Arrays.asList(input.split(REGEX_FOR_SPLITING_KQL_QUERY));

    }

    public void frameESQuery(Deque<String> tokens) {

        if (log.isDebugEnabled())
            log.debug("frameESQuery Method received input tokens:{}", tokens);

        buildQuery(tokens, getFirstLogicalOperatorOrderOfPrecedence());

    }

    Query buildQuery(Deque<String> tokens, LogicalOperator delimiter) {

        if (tokens.isEmpty()) {
            return null;
        }

        log.debug("Inside buildQuery. Input: {}, Looking for delimiter: {}", tokens, delimiter);

        List<String> tempHolder = new ArrayList<>();
        List<Query> multiMatchQueries = new ArrayList<>();

        // DUMMY TOKEN is added to handle residue. The last part of the query.
        tokens.add(DUMMY_TOKEN);
        boolean isDelimiterPresent = false;
        while (tokens.peekFirst() != null) {
            String token = tokens.poll();
            if (delimiter.name().equalsIgnoreCase(token.toUpperCase()) || token.equals(DUMMY_TOKEN)) {

                if (!token.equals(DUMMY_TOKEN)) {
                    isDelimiterPresent = true;
                    log.debug("Found Logical Operator:{} ", delimiter);

                } else {
                    log.debug("Found DUMMY_TOKEN:{}", token);
                }
                log.debug("Token collected so far:{}", tempHolder);
                Query b = buildChildQuery(delimiter, token, tempHolder);
                if (null != b)
                    multiMatchQueries.add(b);

                tempHolder.clear();

            } else {
                tempHolder.add(token);
                log.debug("Inside ELSE {}", tempHolder);

            }
        }

        if (isDelimiterPresent)
            return buildBoolQuery(delimiter, multiMatchQueries)._toQuery();
        else
            return multiMatchQueries.get(0);

    }

    public static void main(String[] args) {
        KqlToEsTransfomer kt = new KqlToEsTransfomer();
        kt.transform("A a OR B b AND C c OR NOT D and e E and f F OR G g");
        BoolQuery.Builder b = new BoolQuery.Builder();
        List<String> a = new ArrayList<>();
        a.add("HelloWold");
        MultiMatchQuery m = kt.buildMultiMatchQuery(a);
        b.mustNot(m._toQuery());
        kt.log.debug("Serialized BoolQuery{}", b.build());

    }

    private Query buildChildQuery(LogicalOperator delimiter, String token, List<String> tempHolder) {
        Query b = null;
        if (isNextLogicalOperatorPresent(delimiter)) {
            LogicalOperator temp = getNextLogicalOperatorOrderOfPrecedence(delimiter);
            log.debug("Next Token: {}", temp);

            b = buildQuery(new LinkedList<>(tempHolder),
                    temp);

        } else {
            if (!LogicalOperator.isUnaryOperator(token))
                b = buildMultiMatchQuery(tempHolder)._toQuery();
        }
        return b;
    }

    private boolean isNextLogicalOperatorPresent(LogicalOperator input) {

        return (getNextLogicalOperatorOrderOfPrecedence(input) != null);

    }

    private LogicalOperator getNextLogicalOperatorOrderOfPrecedence(LogicalOperator input) {
        switch (input) {
            case OR:
                return LogicalOperator.AND;
            case AND:
                return LogicalOperator.NOT;
            case NOT:
                return null;
        }
        return null;
    }

    private BoolQuery buildBoolQuery(LogicalOperator operator, List<Query> childQueries) {
        switch (operator) {
            case OR:
                return buildBoolQueryForOrOperator(childQueries);
            case AND:
                return buildBoolQueryForAndOperator(childQueries);
            case NOT:
                return buildBoolQueryForNotOperator(childQueries);
            default:
                throw new UnsupportedOperationException();
        }
    }

    private BoolQuery buildBoolQueryForOrOperator(List<Query> childQueries) {
        log.debug("Inside buildOrBoolQuery");

        BoolQuery boolQuery = BoolQuery.of((BoolQuery.Builder bqb) -> bqb.should(childQueries));

        log.debug("Serialized OR Bool Query: {}", boolQuery);
        return boolQuery;

    }

    private BoolQuery buildBoolQueryForAndOperator(List<Query> childQueries) {

        BoolQuery boolQuery = BoolQuery.of((BoolQuery.Builder bqb) -> bqb.must(childQueries));

        log.debug("Serialized AND Bool Query: {}", boolQuery);
        return boolQuery;

    }

    private BoolQuery buildBoolQueryForNotOperator(List<Query> childQueries) {

        BoolQuery boolQuery = BoolQuery.of((BoolQuery.Builder bqb) -> bqb.mustNot(childQueries));

        log.debug("Serialized NOT Bool Query: {}", boolQuery);
        return boolQuery;

    }

    private LogicalOperator getFirstLogicalOperatorOrderOfPrecedence() {
        return LogicalOperator.OR;
    }

    public MultiMatchQuery buildMultiMatchQuery(List<String> tokens) {
        if (tokens.isEmpty())
            return null;
        String queryString = getString(tokens);
        return buildMultiMatchQuery(queryString);

    }

    public MultiMatchQuery buildMultiMatchQuery(String s) {
        log.debug("Inside buildMultiMatchQuery. input:{}", s);
        MultiMatchQuery m = MultiMatchQuery.of((MultiMatchQuery.Builder mmb) -> {
            log.info("inline function execution");
            mmb.lenient(true);
            mmb.type(TextQueryType.BestFields);
            mmb.query(s);
            return mmb;
        });

        log.debug("Serialized Multi Match Query: {}", m);
        return m;

    }

    public String getString(List<String> tokens) {
        if (tokens.isEmpty()) {
            log.debug("Received empty tokens");
            return EMPTY;
        }
        StringBuilder sb = new StringBuilder();
        for (String token : tokens) {
            sb.append(token);
            sb.append(SPACE);
        }
        return sb.deleteCharAt(sb.length() - 1).toString();

    }

}
