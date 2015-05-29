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
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.joda.time.DateTimeUtils;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.plugin.jdbc.util.IndexableObject;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverContext;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverMouth;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * RebuildRiverMouth implementation based on the SimpleRiverMouth.
 *
 * @author <a href="feaster83@gmail.com">Jasper Huzen</a>
 */
public class RebuildRiverMouth<RC extends SimpleRiverContext> extends SimpleRiverMouth<RC> {

    private final static ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.RebuildRiverMouth");

    private String index_prefix;

    private String alias;

    private String rebuild_index;

    @Override
    public String strategy() {
        return StrategyConstants.STRATEGY_NAME;
    }

    @Override
    public RebuildRiverMouth<RC> newInstance() {
        return new RebuildRiverMouth<RC>();
    }

    @Override
    public RebuildRiverMouth<RC> setRiverContext(RC context) {
        super.setRiverContext(context);
        this.alias = (String) context.getDefinition().get("alias");
        this.index_prefix = (String) context.getDefinition().get("index_prefix");
        return this;
    }

    @Override
    public synchronized void beforeFetch() throws IOException {
        if (ingest == null || ingest.isShutdown()) {
            ingest = ingestFactory.create();
        }

        rebuild_index = index_prefix + "_" + DateTimeFormat.forPattern("yyyy-MM-dd-hh-mm-ss-ms"). print(DateTimeUtils.currentTimeMillis());

        logger.info("creating index {}", rebuild_index);

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

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
        this.index = rebuild_index;
        object.index(rebuild_index);
        super.index(object, create);
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
    public void delete(IndexableObject object) {
        logger.error("Delete operation is unsupported for the RebuildStrategy!");
    }

}
