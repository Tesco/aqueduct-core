package com.tesco.aqueduct.pipe.http;

import io.micronaut.configuration.metrics.management.endpoint.MetricsEndpoint;
import io.micronaut.core.bind.exceptions.UnsatisfiedArgumentException;
import io.micronaut.scheduling.annotation.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.Collections;
import java.util.SortedSet;

@Singleton
public class DumpMetrics {
    private static final Logger LOG
            = LoggerFactory.getLogger("metrics");

    private MetricsEndpoint metrics;

    @Inject
    public DumpMetrics(MetricsEndpoint metrics){
        this.metrics = metrics;
    }

    @Scheduled(fixedDelay = "1m")
    public void dumpMetrics() {
        final SortedSet<String> names = metrics.listNames().getNames();
        names.forEach(this::dumpMetric);
    }

    private void dumpMetric(final String metricName) {
        final MetricsEndpoint.MetricDetails details = metrics.getMetricDetails(metricName, null);
        dumpMetric(metricName, details);
        details.getAvailableTags()
            .forEach(tag -> dumpTag(metricName, tag));
    }

    private void dumpTag(final String metricName, @NotNull final MetricsEndpoint.AvailableTag tag) {
        try {
            tag.getValues().stream()
                .filter(v -> !v.contains("client_id="))
                .map(v -> tag.getTag() + ":" + v)
                .forEach(tagValue ->
                    dumpMetric(
                        metricName + ":" + tagValue,
                        metrics.getMetricDetails(metricName, Collections.singletonList(tagValue))
                    )
                );
        } catch (UnsatisfiedArgumentException e) {
            LOG.error("Dump Metrics, metric throwing exception is: " + metricName + " and tag " + tag.getTag());
            throw e;
        }
    }

    private void dumpMetric(final String metricName, final MetricsEndpoint.MetricDetails details) {
        details.getMeasurements().forEach(sample -> {
            MDC.put("statistic", sample.getStatistic().name());
            MDC.put("value", String.format("%.2f", sample.getValue()));
            MDC.put("env", "perf");
        });

        LOG.info(metricName);

        MDC.remove("statistic");
        MDC.remove("value");
        MDC.remove("env");
    }
}
