package com.tesco.aqueduct.registry.client;

import com.tesco.aqueduct.registry.utils.RegistryLogger;
import io.micronaut.http.client.HttpClientConfiguration;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.reactivex.Flowable.fromIterable;

public class ServiceList {
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(ServiceList.class));
    private final HttpClientConfiguration configuration;
    private List<PipeServiceInstance> services;
    private final PipeServiceInstance cloudInstance;
    private final File file;

    public ServiceList(
        final HttpClientConfiguration configuration,
        final PipeServiceInstance pipeServiceInstance,
        File file
    ) throws IOException {
        this.configuration = configuration;
        this.cloudInstance = pipeServiceInstance;
        services = new ArrayList<>();
        this.file = file;
        readUrls(file);
    }

    private void readUrls(File file) throws IOException {
        if (!file.exists()) {
            defaultToCloud();
            return;
        }
        Properties properties = new Properties();
        try (FileInputStream stream = new FileInputStream(file)) {
            properties.load(stream);
        }
        updateServices(readUrls(properties));
    }

    private List<URL> readUrls(Properties properties) {
        String urls = properties.getProperty("services", "");
        return Arrays.stream(urls.split(","))
            .map(this::toUrl)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    private URL toUrl(String m) {
        try {
            return new URL(m);
        } catch (MalformedURLException e) {
            LOG.error("toUrl", "malformed url " + m, e.getMessage());
        }
        return null;
    }

    public void update(final List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            defaultToCloud();
            return;
        }
        updateServices(urls);
        persistUrls(urls);
    }

    private void updateServices(final List<URL> urls) {
        if (urls == null || urls.isEmpty()) {
            defaultToCloud();
            return;
        }
        services = urls.stream()
            .map(this::getServiceInstance)
            .collect(Collectors.toList());
    }

    private void persistUrls(List<URL> urls) {
        Properties properties = new Properties();
        String urlStrings = urls.stream().map(Object::toString).collect(Collectors.joining(","));
        properties.setProperty("services", urlStrings);

        try (OutputStream outputStream = new FileOutputStream(file)) {
            try (OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
                properties.store(outputStreamWriter, null);
            }
        } catch (IOException exception) {
            LOG.error("persist", "Unable to persist service urls to properties file", exception);
            throw new UncheckedIOException(exception);
        }
    }

    private void defaultToCloud() {
        LOG.info("ServiceList.defaultToCloud", "Defaulting to follow the Cloud Pipe server.");
        List<PipeServiceInstance> justCloud = new ArrayList<>();
        justCloud.add(this.cloudInstance);
        this.services = justCloud;
    }

    private PipeServiceInstance getServiceInstance(final URL url) {
        return findPreviousInstance(url)
            .orElseGet(() -> new PipeServiceInstance(configuration, url));
    }

    private Optional<PipeServiceInstance> findPreviousInstance(final URL url) {
        try {
            // We have to use URIs for this comparison as URLs are converted to IPs under the hood, which causes issues
            // for local testing
            final URI uri = url.toURI();
            return services.stream()
                .filter(oldInstance -> uri.equals(oldInstance.getURI()))
                .findFirst();
        } catch (URISyntaxException exception) {
            LOG.error("pipe load balancer", "invalid URI", exception);
            return Optional.empty();
        }
    }

    public void checkState() {
        fromIterable(services)
            .flatMapCompletable(PipeServiceInstance::checkState)
            .blockingAwait();
    }

    public Stream<PipeServiceInstance> stream() {
        return services.stream();
    }
}
