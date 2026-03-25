package io.scinapse.elasticsearch.export;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.List;

public class BulkIdExportResponse extends BaseBroadcastResponse {

    private final BytesReference idsBytes;
    private final long totalIds;
    private final long tookMs;

    public BulkIdExportResponse(
            int totalShards, int successfulShards, int failedShards,
            List<DefaultShardOperationFailedException> shardFailures,
            BytesReference idsBytes, long totalIds, long tookMs
    ) {
        super(totalShards, successfulShards, failedShards, shardFailures);
        this.idsBytes = idsBytes;
        this.totalIds = totalIds;
        this.tookMs = tookMs;
    }

    public BulkIdExportResponse(StreamInput in) throws IOException {
        super(in);
        this.idsBytes = in.readBytesReference();
        this.totalIds = in.readVLong();
        this.tookMs = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(idsBytes);
        out.writeVLong(totalIds);
        out.writeVLong(tookMs);
    }

    public BytesReference getIdsBytes() {
        return idsBytes;
    }

    public long getTotalIds() {
        return totalIds;
    }

    public long getTookMs() {
        return tookMs;
    }
}
