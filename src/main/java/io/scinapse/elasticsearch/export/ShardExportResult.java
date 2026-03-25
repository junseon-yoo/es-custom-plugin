package io.scinapse.elasticsearch.export;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Per-shard result containing document IDs as newline-delimited raw bytes.
 * Avoids String object overhead — IDs stay as bytes from doc_values to wire.
 */
public class ShardExportResult implements Writeable {

    private final BytesReference idsBytes;
    private final long count;

    public ShardExportResult(BytesReference idsBytes, long count) {
        this.idsBytes = idsBytes;
        this.count = count;
    }

    public ShardExportResult(StreamInput in) throws IOException {
        this.idsBytes = in.readBytesReference();
        this.count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBytesReference(idsBytes);
        out.writeVLong(count);
    }

    public BytesReference idsBytes() {
        return idsBytes;
    }

    public long count() {
        return count;
    }
}
