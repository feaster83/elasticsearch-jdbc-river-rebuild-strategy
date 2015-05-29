/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.feaster83.elasticsearch.river.jdbc.strategy.rebuild;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.joda.time.DateTimeUtils;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.VersionType;
import org.xbib.elasticsearch.plugin.jdbc.client.Ingest;
import org.xbib.elasticsearch.plugin.jdbc.client.IngestFactory;
import org.xbib.elasticsearch.plugin.jdbc.client.Metric;
import org.xbib.elasticsearch.plugin.jdbc.util.ControlKeys;
import org.xbib.elasticsearch.plugin.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Simple river mouth implementation. This implementation uses bulk processing,
 * alias name housekeeping (with replica/refresh), and metrics. It understands
 * _version, _routing, _timestamp, _parent, and _ttl metadata.
 */
public class RebuildRiverMouth<RC extends SimpleRiverContext> implements RiverMouth<RC> {

    private final static ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.RebuildRiverMouth");

    protected RC context;

    protected IngestFactory ingestFactory;

    protected Ingest ingest;

    protected Metric metric;

    protected Settings indexSettings;

    protected Map<String, String> indexMappings;

    protected String index_prefix;

    protected String alias;

    private String rebuild_index;

    protected String type;

    protected String id;

    protected volatile boolean suspended = false;

    @Override
    public String strategy() {
        return "rebuild";
    }

    @Override
    public RebuildRiverMouth<RC> newInstance() {
        return new RebuildRiverMouth<RC>();
    }

    @Override
    public RebuildRiverMouth<RC> setRiverContext(RC context) {
        this.context = context;
        this.alias = (String) context.getDefinition().get("alias");
        this.index_prefix = (String) context.getDefinition().get("index_prefix");
        return this;
    }

    @Override
    public RebuildRiverMouth setIngestFactory(IngestFactory ingestFactory) {
        this.ingestFactory = ingestFactory;
        this.ingest = ingestFactory.create();
        this.metric = ingest.getMetric();
        return this;
    }

    @Override
    public Metric getMetric() {
        return metric;
    }

    @Override
    public synchronized void beforeFetch() throws IOException {
        if (ingest == null || ingest.isShutdown()) {
            ingest = ingestFactory.create();
        }

        rebuild_index = index_prefix + "_" + DateTimeFormat.forPattern("yyyy-MM-dd-hh-mm-ss-ms"). print(DateTimeUtils.currentTimeMillis());

        logger.info("creating index {} with settings {} and mappings {}",
                    rebuild_index, indexSettings != null ? indexSettings.getAsMap() : "{}", indexMappings);

        String templateSource = context.getDefinition().get("template").toString();
        byte[] fileBytes = Files.readAllBytes(new File(templateSource).toPath());

        ingest.client().admin().indices().prepareCreate(rebuild_index).setSource(fileBytes).execute().actionGet();

        long startRefreshInterval = indexSettings != null ?
                indexSettings.getAsTime("bulk." + rebuild_index + ".refresh_interval.start", indexSettings.getAsTime("alias.refresh_interval", TimeValue.timeValueSeconds(-1))).getMillis() : -1L;
        long stopRefreshInterval = indexSettings != null ?
                indexSettings.getAsTime("bulk." + rebuild_index + ".refresh_interval.stop", indexSettings.getAsTime("alias.refresh_interval", TimeValue.timeValueSeconds(1))).getMillis() : 1000L;
        ingest.startBulk(rebuild_index, startRefreshInterval, stopRefreshInterval);
    }


    @Override
    public synchronized void afterFetch() throws IOException {
        if (ingest == null || ingest.isShutdown()) {
            ingest = ingestFactory.create();
        }
        flush();
        ingest.stopBulk(rebuild_index);
        ingest.refresh(rebuild_index);
        if (metric.indices() != null && !metric.indices().isEmpty()) {
            for (String index : ImmutableSet.copyOf(metric.indices())) {
                logger.info("stopping bulk mode for index {} and refreshing...", index);
                ingest.stopBulk(index);
                ingest.refresh(index);
            }
        }

        switchAliasAndRemoveOldIndex();

        if (!ingest.isShutdown()) {
            ingest.shutdown();
        }
    }

    private void switchAliasAndRemoveOldIndex() {
        GetAliasesResponse getAliasesResponse = ingest.client().admin().indices().prepareGetAliases(alias).execute().actionGet();

        List<String> existingIndexes = new ArrayList<>();
        Iterator<ObjectCursor<String>> keysIterator = getAliasesResponse.getAliases().keys().iterator();
        while (keysIterator.hasNext()) {
            String existingIndex = keysIterator.next().value;
            existingIndexes.add(existingIndex);
        }

        IndicesAliasesRequestBuilder prepareAliasesRequest = ingest.client().admin().indices().prepareAliases();
        prepareAliasesRequest.addAlias(rebuild_index, alias).execute().actionGet().isAcknowledged();

        Iterator<String> existingIndexesIterator = existingIndexes.iterator();
        while (existingIndexesIterator.hasNext()) {
            prepareAliasesRequest.removeAlias(existingIndexesIterator.next(), alias);
        }

        prepareAliasesRequest.execute();

        Iterator<String> existingIndexesIterator2 = existingIndexes.iterator();
        while (existingIndexesIterator2.hasNext()) {
            String existingIndex = existingIndexesIterator2.next();
            logger.info("DELETE existing index {}", existingIndex);
            ingest.deleteIndex(existingIndex);
        }
    }

    @Override
    public synchronized void release() {
        try {
            flush();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }

    @Override
    public synchronized void shutdown() {
        try {
            flush();
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        if (ingest != null && !ingest.isShutdown()) {
            // shut down ingest and release ingest resources
            ingest.shutdown();
        }
    }

    @Override
    public RebuildRiverMouth setIndexSettings(Settings indexSettings) {
        this.indexSettings = indexSettings;
        return this;
    }

    @Override
    public RebuildRiverMouth setTypeMapping(Map<String, String> typeMapping) {
        this.indexMappings = typeMapping;
        return this;
    }

    @Override
    public RebuildRiverMouth setIndex(String index) {
       return this;
    }

    @Override
    public String getIndex() {
        return null;
    }

    @Override
    public RebuildRiverMouth setType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public RebuildRiverMouth setId(String id) {
        this.id = id;
        return this;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
        try {
            while (suspended) {
                Thread.sleep(1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("interrupted");
        }
        if (Strings.hasLength(object.index())) {
            setIndex(object.index());
        }
        if (Strings.hasLength(object.type())) {
            setType(object.type());
        }
        if (Strings.hasLength(object.id())) {
            setId(object.id());
        }
        IndexRequest request = Requests.indexRequest(this.rebuild_index)
                .type(this.type)
                .id(getId())
                .source(object.build());
        if (object.meta(ControlKeys._version.name()) != null) {
            request.versionType(VersionType.EXTERNAL)
                    .version(Long.parseLong(object.meta(ControlKeys._version.name())));
        }
        if (object.meta(ControlKeys._routing.name()) != null) {
            request.routing(object.meta(ControlKeys._routing.name()));
        }
        if (object.meta(ControlKeys._parent.name()) != null) {
            request.parent(object.meta(ControlKeys._parent.name()));
        }
        if (object.meta(ControlKeys._timestamp.name()) != null) {
            request.timestamp(object.meta(ControlKeys._timestamp.name()));
        }
        if (object.meta(ControlKeys._ttl.name()) != null) {
            request.ttl(Long.parseLong(object.meta(ControlKeys._ttl.name())));
        }
        if (logger.isTraceEnabled()) {
            logger.trace("adding bulk alias action {}", request.source().toUtf8());
        }
        if (ingest != null) {
            ingest.bulkIndex(request);
        }
    }

    @Override
    public void delete(IndexableObject object) {
        logger.error("Delete operation is unsupported for the RebuildStrategy!");
    }

    @Override
    public void flush() throws IOException {
        if (ingest != null) {
            ingest.flushIngest();
            // wait for all outstanding bulk requests before continue with river
            try {
                ingest.waitForResponses(TimeValue.timeValueSeconds(60));
            } catch (InterruptedException e) {
                logger.warn("interrupted while waiting for responses");
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void suspend() {
        if (ingest != null) {
            this.suspended = true;
            ingest.suspend();
        }
    }

    @Override
    public void resume() {
        if (ingest != null) {
            this.suspended = false;
            ingest.resume();
        }
    }

}
