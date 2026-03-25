package io.scinapse.elasticsearch.export;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.function.BooleanSupplier;

/**
 * Lucene Collector that reads document IDs from doc_values directly to bytes.
 * - Writes BytesRef → BytesStreamOutput without String conversion
 * - Checks cancellation every 8192 docs
 * - Tracks memory via circuit breaker
 */
public class IdCollector extends SimpleCollector {

    private static final int CANCEL_CHECK_INTERVAL = 8192;

    private final String field;
    private final BytesStreamOutput out;
    private final BooleanSupplier isCancelled;
    private final CircuitBreaker breaker;
    private SortedDocValues docValues;
    private long count;
    private long trackedBytes;

    public IdCollector(String field, BooleanSupplier isCancelled, CircuitBreaker breaker) {
        this.field = field;
        this.out = new BytesStreamOutput();
        this.isCancelled = isCancelled;
        this.breaker = breaker;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        SortedDocValues dv = context.reader().getSortedDocValues(field);
        if (dv == null) {
            throw new IllegalStateException(
                "field [" + field + "] has no doc_values in segment [" + context.reader() + "]. " +
                "Ensure the field is mapped as keyword with doc_values enabled."
            );
        }
        this.docValues = dv;
    }

    @Override
    public void collect(int doc) throws IOException {
        if (++count % CANCEL_CHECK_INTERVAL == 0 && isCancelled.getAsBoolean()) {
            throw new TaskCancelledException("bulk_id_export cancelled after " + count + " docs");
        }

        if (docValues.advanceExact(doc)) {
            BytesRef value = docValues.lookupOrd(docValues.ordValue());
            long bytesNeeded = value.length + 1L; // +1 for newline

            if (breaker != null) {
                try {
                    breaker.addEstimateBytesAndMaybeBreak(bytesNeeded, "bulk_id_export");
                } catch (CircuitBreakingException e) {
                    releaseBreaker();
                    throw e;
                }
                trackedBytes += bytesNeeded;
            }

            out.writeBytes(value.bytes, value.offset, value.length);
            out.writeByte((byte) '\n');
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public BytesStreamOutput output() {
        return out;
    }

    public long count() {
        return count;
    }

    /**
     * Release circuit breaker reservation. Call this after bytes are consumed.
     */
    public void releaseBreaker() {
        if (breaker != null && trackedBytes > 0) {
            breaker.addWithoutBreaking(-trackedBytes);
            trackedBytes = 0;
        }
    }
}
