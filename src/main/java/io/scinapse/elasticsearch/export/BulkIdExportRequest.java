package io.scinapse.elasticsearch.export;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class BulkIdExportRequest extends BroadcastRequest<BulkIdExportRequest> {

    private QueryBuilder query;
    private String field = "paper_id";

    public BulkIdExportRequest(String[] indices, QueryBuilder query, String field) {
        super(indices);
        this.query = query;
        this.field = field;
    }

    public BulkIdExportRequest(StreamInput in) throws IOException {
        super(in);
        this.query = in.readNamedWriteable(QueryBuilder.class);
        this.field = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeNamedWriteable(query);
        out.writeString(field);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = super.validate();
        if (query == null) {
            validationException = addValidationError("query is required", validationException);
        }
        if (field == null || field.isEmpty()) {
            validationException = addValidationError("field name is required", validationException);
        }
        return validationException;
    }

    public QueryBuilder query() {
        return query;
    }

    public String field() {
        return field;
    }
}
