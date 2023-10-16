package com.flipkart.foxtrot.server.jobs.consolehistory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.foxtrot.core.dao.SearchStore;
import com.flipkart.foxtrot.core.jobs.BaseJobManager;
import com.flipkart.foxtrot.core.querystore.impl.HazelcastConnection;
import com.flipkart.foxtrot.server.console.ConsoleFetchException;
import com.flipkart.foxtrot.server.console.ConsolePersistence;
import com.flipkart.foxtrot.server.console.ConsoleV2;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import javax.inject.Inject;
import javax.inject.Singleton;
import net.javacrumbs.shedlock.core.LockConfiguration;
import net.javacrumbs.shedlock.core.LockingTaskExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.vyarus.dropwizard.guice.module.installer.order.Order;

/***
 Created by mudit.g on Dec, 2018
 ***/
@Singleton
@Order(45)
public class ConsoleHistoryManager extends BaseJobManager {

    private static final Logger logger = LoggerFactory.getLogger(ConsoleHistoryManager.class.getSimpleName());
    private static final String TYPE = "console_data";
    private static final String INDEX_V2 = "consoles_v2";
    private static final String INDEX_HISTORY = "consoles_history";
    private final SearchStore searchStore;
    private final ConsoleHistoryConfig consoleHistoryConfig;
    private final ObjectMapper mapper;
    private final ConsolePersistence consolePersistence;

    @Inject
    public ConsoleHistoryManager(ScheduledExecutorService scheduledExecutorService,
                                 ConsoleHistoryConfig consoleHistoryConfig,
                                 SearchStore searchStore,
                                 HazelcastConnection hazelcastConnection,
                                 ObjectMapper mapper,
                                 ConsolePersistence consolePersistence) {
        super(consoleHistoryConfig, scheduledExecutorService, hazelcastConnection);
        this.consoleHistoryConfig = consoleHistoryConfig;
        this.searchStore = searchStore;
        this.mapper = mapper;
        this.consolePersistence = consolePersistence;
    }

    @Override
    protected void runImpl(LockingTaskExecutor executor, Instant lockAtMostUntil) {
        executor.executeWithLock(() -> {
            try {
                List<String> results = searchStore.getAggregationSearchResults(INDEX_V2, TYPE, "names", "name.keyword");
                for (String result : results) {
                    deleteOldData(result);
                }
            } catch (Exception e) {
                logger.info("Failed to get aggregations and delete data for index history.", e);
            }

        }, new LockConfiguration(consoleHistoryConfig.getJobName(), lockAtMostUntil));
    }

    private void deleteOldData(final String name) {
        String updatedAt = "updatedAt";
        try {
            List<ConsoleV2> consoles = searchStore.getQuerySearchHitSourceAsString(ConsoleV2.class, INDEX_HISTORY, TYPE,
                    "name.keyword", name, updatedAt, 10, 9000);

            for (ConsoleV2 console : consoles) {
                consolePersistence.deleteOldVersion(console.getId());
            }
        } catch (Exception e) {
            throw new ConsoleFetchException(e);
        }
    }
}
