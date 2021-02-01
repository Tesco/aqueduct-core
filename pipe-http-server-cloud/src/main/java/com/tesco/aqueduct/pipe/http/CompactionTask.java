package com.tesco.aqueduct.pipe.http;

import com.tesco.aqueduct.pipe.logger.PipeLogger;
import com.tesco.aqueduct.pipe.storage.PostgresqlStorage;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micronaut.context.annotation.Context;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.scheduling.annotation.Scheduled;
import io.micronaut.scheduling.cron.CronExpression;
import org.slf4j.LoggerFactory;

@Context
@Requires(property = "persistence.compact.schedule.cron")
class CompactionTask {
    private static final PipeLogger LOG = new PipeLogger(LoggerFactory.getLogger(CompactionTask.class));
    private final PostgresqlStorage postgresqlStorage;
    private final LongTaskTimer longTaskTimer;

    public CompactionTask(
        final MeterRegistry registry,
        final PostgresqlStorage postgresqlStorage,
        @Property(name = "persistence.compact.schedule.cron") final String cronExpression
    ) {
        this.postgresqlStorage = postgresqlStorage;

        this.longTaskTimer = registry.more().longTaskTimer("persistence.compaction");
        isValid(cronExpression);
    }

    @Scheduled(cron = "${persistence.compact.schedule.cron}")
    void compaction() {
        longTaskTimer.record(() -> {
            LOG.info("compaction", "compaction started");
            postgresqlStorage.compactAndMaintain();
            LOG.info("compaction", "compaction finished");
        });
    }

    private void isValid(final String cronExpression) {
        CronExpression.create(cronExpression);
    }
}