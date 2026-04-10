package io.scinapse.elasticsearch.export;

import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Query;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.PlainShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.CompositeBytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TransportBulkIdExportAction extends TransportBroadcastByNodeAction<
        BulkIdExportRequest,
        BulkIdExportResponse,
        ShardExportResult> {

    private final IndicesService indicesService;
    private final CircuitBreakerService circuitBreakerService;

    @Inject
    public TransportBulkIdExportAction(
            ClusterService clusterService,
            TransportService transportService,
            IndicesService indicesService,
            ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver,
            CircuitBreakerService circuitBreakerService
    ) {
        super(
            BulkIdExportAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            BulkIdExportRequest::new,
            transportService.getThreadPool().executor(ThreadPool.Names.GENERIC)
        );
        this.indicesService = indicesService;
        this.circuitBreakerService = circuitBreakerService;
    }

    @Override
    protected ShardExportResult readShardResult(StreamInput in) throws IOException {
        return new ShardExportResult(in);
    }

    @Override
    protected ResponseFactory<BulkIdExportResponse, ShardExportResult> getResponseFactory(
            BulkIdExportRequest request, ClusterState clusterState
    ) {
        long startMs = System.currentTimeMillis();
        return (totalShards, successfulShards, failedShards, results, shardFailures) -> {
            long tookMs = System.currentTimeMillis() - startMs;

            long totalIds = 0;
            List<BytesReference> parts = new ArrayList<>(results.size());
            for (ShardExportResult result : results) {
                if (result.idsBytes().length() > 0) {
                    parts.add(result.idsBytes());
                }
                totalIds += result.count();
            }

            BytesReference combined = parts.isEmpty()
                ? BytesArray.EMPTY
                : CompositeBytesReference.of(parts.toArray(new BytesReference[0]));

            return new BulkIdExportResponse(
                totalShards, successfulShards, failedShards, shardFailures,
                combined, totalIds, tookMs
            );
        };
    }

    @Override
    protected BulkIdExportRequest readRequestFrom(StreamInput in) throws IOException {
        return new BulkIdExportRequest(in);
    }

    @Override
    protected void shardOperation(
            BulkIdExportRequest request,
            ShardRouting shardRouting,
            Task task,
            ActionListener<ShardExportResult> listener
    ) {
        // Early cancellation check — avoid acquiring searcher and rewriting
        // the query if the task has already been cancelled.
        if (task instanceof CancellableTask ct && ct.isCancelled()) {
            listener.onFailure(new TaskCancelledException(
                "bulk_id_export cancelled before shard [" + shardRouting.shardId() + "] execution"
            ));
            return;
        }

        try {
            IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
            IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());

            CircuitBreaker breaker = circuitBreakerService.getBreaker(CircuitBreaker.REQUEST);

            try (Engine.Searcher searcher = indexShard.acquireSearcher("bulk_id_export")) {
                SearchExecutionContext context = indexService.newSearchExecutionContext(
                    shardRouting.shardId().id(),
                    0,
                    searcher,
                    System::currentTimeMillis,
                    null,
                    Map.of()
                );

                QueryBuilder rewritten = Rewriteable.rewrite(request.query(), context);
                Query luceneQuery = rewritten.toQuery(context);

                var cancelCheck = (java.util.function.BooleanSupplier)
                    (() -> task instanceof CancellableTask ct && ct.isCancelled());

                var createdCollectors = java.util.Collections.synchronizedList(new ArrayList<IdCollector>());

                CollectorManager<IdCollector, ShardExportResult> manager = new CollectorManager<>() {
                    @Override
                    public IdCollector newCollector() {
                        IdCollector c = new IdCollector(request.field(), cancelCheck, breaker);
                        createdCollectors.add(c);
                        return c;
                    }

                    @Override
                    public ShardExportResult reduce(java.util.Collection<IdCollector> collectors) {
                        try {
                            var parts = new ArrayList<BytesReference>(collectors.size());
                            long total = 0;
                            for (IdCollector c : collectors) {
                                if (c.count() > 0) {
                                    parts.add(c.output().bytes());
                                }
                                total += c.count();
                            }
                            BytesReference merged = parts.isEmpty()
                                ? BytesArray.EMPTY
                                : CompositeBytesReference.of(parts.toArray(new BytesReference[0]));
                            return new ShardExportResult(merged, total);
                        } finally {
                            for (IdCollector c : collectors) {
                                c.releaseBreaker();
                            }
                        }
                    }
                };

                try {
                    ShardExportResult result = searcher.search(luceneQuery, manager);
                    listener.onResponse(result);
                } catch (Exception e) {
                    // Snapshot under the list's monitor before iterating.
                    // Collections.synchronizedList requires manual synchronization
                    // when iterating. In practice all collection threads have
                    // already stopped by the time search() throws, but taking
                    // a snapshot makes the memory model explicit.
                    List<IdCollector> snapshot;
                    synchronized (createdCollectors) {
                        snapshot = new ArrayList<>(createdCollectors);
                    }
                    for (IdCollector c : snapshot) {
                        c.releaseBreaker();
                    }
                    throw e;
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, BulkIdExportRequest request, String[] concreteIndices) {
        // IMPORTANT: select exactly one active copy per shard id.
        // routingTable().allShards() returns primary + all replicas, which would
        // cause each document to be exported once per copy (2x for 1-replica setup).
        //
        // Prefer active primary; fall back to an active replica if primary is
        // unavailable (e.g. during relocation or failover).
        // If a shard id has NO active copies, fail fast — silently skipping
        // would produce partial results indistinguishable from success.
        List<ShardRouting> selectedShards = new ArrayList<>();
        for (String indexName : concreteIndices) {
            IndexRoutingTable indexRouting = clusterState.routingTable().index(indexName);
            if (indexRouting == null) {
                continue;
            }
            for (int i = 0; i < indexRouting.size(); i++) {
                IndexShardRoutingTable shardRouting = indexRouting.shard(i);
                ShardRouting primary = shardRouting.primaryShard();
                if (primary != null && primary.active()) {
                    selectedShards.add(primary);
                    continue;
                }
                List<ShardRouting> active = shardRouting.activeShards();
                if (active.isEmpty() == false) {
                    selectedShards.add(active.get(0));
                    continue;
                }
                throw new NoShardAvailableActionException(
                    shardRouting.shardId(),
                    "no active shard copy available for [" + shardRouting.shardId()
                        + "] — bulk_id_export requires all shards to be available to avoid partial results"
                );
            }
        }
        return new PlainShardsIterator(selectedShards);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, BulkIdExportRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
            ClusterState state, BulkIdExportRequest request, String[] concreteIndices
    ) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, concreteIndices);
    }
}
