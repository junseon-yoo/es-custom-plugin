package io.scinapse.elasticsearch.export;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActionListener;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.rest.RestRequest.Method.POST;

public class RestBulkIdExportAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "bulk_id_export";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/{index}/_export_ids"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String field = request.param("field", "paper_id");

        if (request.hasContent() == false) {
            throw new IllegalArgumentException("request body is required with a [query] field");
        }

        QueryBuilder queryBuilder = parseQuery(request);

        BulkIdExportRequest exportRequest = new BulkIdExportRequest(
            new String[]{index}, queryBuilder, field
        );

        return channel -> client.execute(BulkIdExportAction.INSTANCE, exportRequest,
            new RestActionListener<>(channel) {
                @Override
                protected void processResponse(BulkIdExportResponse response) throws Exception {
                    BytesReference body = response.getIdsBytes();

                    RestResponse restResponse = new RestResponse(
                        RestStatus.OK,
                        "text/plain; charset=UTF-8",
                        body
                    );
                    restResponse.addHeader("X-Total-Count", String.valueOf(response.getTotalIds()));
                    restResponse.addHeader("X-Took-Ms", String.valueOf(response.getTookMs()));
                    restResponse.addHeader("X-Total-Shards", String.valueOf(response.getTotalShards()));
                    restResponse.addHeader("X-Successful-Shards", String.valueOf(response.getSuccessfulShards()));
                    restResponse.addHeader("X-Failed-Shards", String.valueOf(response.getFailedShards()));
                    channel.sendResponse(restResponse);
                }
            }
        );
    }

    private QueryBuilder parseQuery(RestRequest request) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            SearchSourceBuilder searchSource = new SearchSourceBuilder();
            searchSource.parseXContent(parser, true, f -> true);
            QueryBuilder queryBuilder = searchSource.query();
            if (queryBuilder == null) {
                throw new IllegalArgumentException("request body must contain a [query] field");
            }
            return queryBuilder;
        }
    }
}
