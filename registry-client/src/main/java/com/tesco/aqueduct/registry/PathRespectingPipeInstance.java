package com.tesco.aqueduct.registry;

import io.micronaut.discovery.ServiceInstance;
import io.micronaut.http.client.DefaultHttpClient;
import io.micronaut.http.client.HttpClientConfiguration;
import io.micronaut.http.client.RxHttpClient;
import io.micronaut.http.uri.UriBuilder;
import io.reactivex.Completable;
import io.reactivex.Flowable;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class PathRespectingPipeInstance implements ServiceInstance {

    private final HttpClientConfiguration configuration;
    private final URL url;
    private boolean up;
    private static final RegistryLogger LOG = new RegistryLogger(LoggerFactory.getLogger(PathRespectingPipeInstance.class));

    @Inject
    public PathRespectingPipeInstance(final HttpClientConfiguration configuration, final URL url, final boolean up) {
        this.configuration = configuration;
        this.url = url;
        this.up = up;
    }

    public boolean isUp() {
        return up;
    }

    public void setUp(final boolean isServiceUp) {
        up = isServiceUp;
    }

    public URL getUrl() {
        return url;
    }

    @Override
    public String getId() {
        return "pipe";
    }

    @Override
    public URI getURI() {
        try {
            return url.toURI();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public URI resolve(URI relativeURI) {
        try {
            if (url.getPath() != null && !url.getPath().isEmpty()) {
                relativeURI = getUriWithBasePath(relativeURI);
            }
            return ServiceInstance.super.resolve(relativeURI);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("ServiceInstance URI is invalid: " + e.getMessage(), e);
        }
    }

    public Completable checkState() {
        final RxHttpClient client = new DefaultHttpClient(url, configuration);
        return checkState(client);
    }

    private Completable checkState(final RxHttpClient client) {
        final String statusUrl = generateStatusUrlFromBaseURI(getURI());
        return client.retrieve(statusUrl)
            // if got response, then it's a true
            .map(response -> true )
            // log result
            .doOnNext(b -> LOG.info("healthcheck.success", url.toString()))
            .doOnError(t -> LOG.error("healthcheck.failed", url.toString(), t.getMessage()))
            // change exception to "false"
            .onErrorResumeNext(Flowable.just(false))
            // set the status of the instance
            .doOnNext(this::setUp)
            // return as completable, close client and ignore any errors
            .ignoreElements() // returns completable
            .doOnComplete(client::close)
            .onErrorComplete();
    }

    private String generateStatusUrlFromBaseURI(final URI baseURI) {
        return UriBuilder.of(baseURI).path("/pipe/_status").build().toString();
    }

    private URI getUriWithBasePath(final URI relativeURI) throws URISyntaxException {
        final String path = Paths.get(url.getPath(), relativeURI.getPath()).toString();
        return new URI(
            relativeURI.getScheme(),
            relativeURI.getUserInfo(),
            relativeURI.getHost(),
            relativeURI.getPort(),
            path,
            relativeURI.getQuery(),
            relativeURI.getFragment()
        );
    }
}