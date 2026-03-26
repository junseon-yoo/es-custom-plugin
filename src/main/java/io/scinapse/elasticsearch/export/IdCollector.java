package io.scinapse.elasticsearch.export;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.tasks.TaskCancelledException;

import java.io.IOException;
import java.util.function.BooleanSupplier;

public class IdCollector extends SimpleCollector {

    private static final int CANCEL_CHECK_INTERVAL = 16384;
    private static final int BREAKER_CHECK_INTERVAL = 65536;

    private final String field;
    private final BytesStreamOutput out;
    private final BooleanSupplier isCancelled;
    private final CircuitBreaker breaker;

    private SortedDocValues sortedDv;
    private SortedSetDocValues sortedSetDv;
    private long count;
    private long trackedBytes;

    public IdCollector(String field, BooleanSupplier isCancelled, CircuitBreaker breaker) {
        this.field = field;
        this.out = new BytesStreamOutput(1024 * 1024);
        this.isCancelled = isCancelled;
        this.breaker = breaker;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        this.sortedDv = null;
        this.sortedSetDv = null;

        // Try SortedDocValues first (single-valued keyword)
        SortedDocValues sdv = context.reader().getSortedDocValues(field);
        if (sdv != null) {
            this.sortedDv = sdv;
            return;
        }

        // Try SortedSetDocValues (multi-valued keyword or ES keyword default)
        SortedSetDocValues ssdv = context.reader().getSortedSetDocValues(field);
        if (ssdv != null) {
            this.sortedSetDv = ssdv;
            return;
        }

        throw new IllegalStateException(
            "field [" + field + "] has no SortedDocValues or SortedSetDocValues. " +
            "Available fields: " + context.reader().getFieldInfos().size()
        );
    }

    @Override
    public void collect(int doc) throws IOException {
        count++;
        if (count % CANCEL_CHECK_INTERVAL == 0 && isCancelled.getAsBoolean()) {
            throw new TaskCancelledException("cancelled after " + count + " docs");
        }

        if (sortedDv != null) {
            if (sortedDv.advanceExact(doc)) {
                BytesRef value = sortedDv.lookupOrd(sortedDv.ordValue());
                out.writeBytes(value.bytes, value.offset, value.length);
                out.writeByte((byte) '\n');
            }
        } else if (sortedSetDv != null) {
            if (sortedSetDv.advanceExact(doc)) {
                long ord = sortedSetDv.nextOrd();
                if (ord != SortedSetDocValues.NO_MORE_ORDS) {
                    BytesRef value = sortedSetDv.lookupOrd(ord);
                    out.writeBytes(value.bytes, value.offset, value.length);
                    out.writeByte((byte) '\n');
                }
            }
        }

        if (breaker != null && count % BREAKER_CHECK_INTERVAL == 0) {
            long currentBytes = out.size();
            long delta = currentBytes - trackedBytes;
            if (delta > 0) {
                breaker.addEstimateBytesAndMaybeBreak(delta, "bulk_id_export");
                trackedBytes = currentBytes;
            }
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }

    public BytesStreamOutput output() { return out; }
    public long count() { return count; }

    public void releaseBreaker() {
        if (breaker != null && trackedBytes > 0) {
            breaker.addWithoutBreaking(-trackedBytes);
            trackedBytes = 0;
        }
    }
}
