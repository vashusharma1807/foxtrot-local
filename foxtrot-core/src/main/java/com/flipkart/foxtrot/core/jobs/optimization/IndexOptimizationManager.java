package com.flipkart.foxtrot.core.jobs.optimization;

import com.flipkart.foxtrot.core.dao.SearchStore;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

/***
 Created by nitish.goyal on 11/09/18
 ***/
@Singleton
@Order(40)
public class IndexOptimizationManager extends BaseJobManager {

    private static final int BATCH_SIZE = 5;
    private static final int SEGMENTS_TO_OPTIMIZE_TO = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexOptimizationManager.class.getSimpleName());

    private final SearchStore searchStore;
    private final IndexOptimizationConfig indexOptimizationConfig;

    @Inject
    public IndexOptimizationManager(ScheduledExecutorService scheduledExecutorService,
                                    IndexOptimizationConfig indexOptimizationConfig,
                                    SearchStore searchStore,
                                    HazelcastConnection hazelcastConnection) {
        super(indexOptimizationConfig, scheduledExecutorService, hazelcastConnection);
        this.indexOptimizationConfig = indexOptimizationConfig;
        this.searchStore = searchStore;
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                searchStore.optimize(BATCH_SIZE, SEGMENTS_TO_OPTIMIZE_TO);
            } catch (IOException e) {
                LOGGER.error("Error getting index list", e);
            }
        }, new LockConfiguration(indexOptimizationConfig.getJobName(), lockAtMostUntil));
    }


    /*@Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                IndicesSegmentsRequest indicesSegmentsRequest = new IndicesSegmentsRequest();

                IndicesSegmentResponse indicesSegmentResponse = elasticSearchDatabaseConnection.getClient()
                        .admin()
                        .indices()
                        .forcemerge()
                        .segments(indicesSegmentsRequest)
                        .actionGet();
                Set<String> indicesToOptimize = Sets.newHashSet();

                Map<String, IndexSegments> segmentResponseIndices = indicesSegmentResponse.getIndices();
                for(Map.Entry<String, IndexSegments> entry : segmentResponseIndices.entrySet()) {
                    String index = entry.getKey();
                    extractIndicesToOptimizeForIndex(index, entry.getValue(), indicesToOptimize);
                }
                optimizeIndices(indicesToOptimize);
                LOGGER.info("No of indexes optimized : {}", indicesToOptimize.size());
            } catch (Exception e) {
                LOGGER.error("Error occurred while calling optimization API", e);
            }
        }, new LockConfiguration(esIndexOptimizationConfig.getJobName(), lockAtMostUntil));
    }

    private void optimizeIndices(Set<String> indicesToOptimize) {
        List<List<String>> batchOfIndicesToOptimize = CollectionUtils.partition(indicesToOptimize, BATCH_SIZE);
        for(List<String> indices : batchOfIndicesToOptimize) {
            Stopwatch stopwatch = Stopwatch.createStarted();
            elasticSearchDatabaseConnection.getClient()
                    .admin()
                    .indices()
                    .prepareForceMerge(indices.toArray(new String[0]))
                    .setMaxNumSegments(SEGMENTS_TO_OPTIMIZE_TO)
                    .setFlush(true)
                    .setOnlyExpungeDeletes(false)
                    .execute()
                    .actionGet();
            LOGGER.info("No of indexes optimized : {}", indices.size());
            MetricUtil.getInstance()
                    .registerActionSuccess("indexesOptimized", CollectionUtils.mkString(indices, ","),
                                           stopwatch.elapsed(TimeUnit.MILLISECONDS)
                                          );
        }
    }

    private void extractIndicesToOptimizeForIndex(String index, IndexSegments indexShardSegments, Set<String> indicesToOptimize) {

        String table = ElasticsearchUtils.getTableNameFromIndex(index);
        if(StringUtils.isEmpty(table)) {
            return;
        }
        String currentIndex = ElasticsearchUtils.getCurrentIndex(table, System.currentTimeMillis());
        String nextDayIndex = ElasticsearchUtils.getCurrentIndex(table, System.currentTimeMillis() + TimeUnit.DAYS.toMillis(1));
        if(index.equals(currentIndex) || index.equals(nextDayIndex)) {
            return;
        }
        Map<Integer, IndexShardSegments> indexShardSegmentsMap = indexShardSegments.getShards();
        for(Map.Entry<Integer, IndexShardSegments> indexShardSegmentsEntry : indexShardSegmentsMap.entrySet()) {
            List<Segment> segments = indexShardSegmentsEntry.getValue()
                    .iterator()
                    .next()
                    .getSegments();
            if(segments.size() > SEGMENTS_TO_OPTIMIZE_TO) {
                indicesToOptimize.add(index);
                break;
            }
        }
    }*/

}
