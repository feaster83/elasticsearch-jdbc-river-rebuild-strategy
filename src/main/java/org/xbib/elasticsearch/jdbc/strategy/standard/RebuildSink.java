/*
 * Copyright (C) 2015 Jasper Huzen
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
package org.xbib.elasticsearch.jdbc.strategy.standard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.hppc.cursors.ObjectCursor;
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.lang3.StringUtils.isBlank;
import static org.elasticsearch.common.lang3.StringUtils.isNotBlank;

/**
 * RebuildRiverMouth implementation based on the SimpleRiverMouth.
 *
 * @author <a href="feaster83@gmail.com">Jasper Huzen</a>
 */
public class RebuildSink<C extends RebuildContext> extends StandardSink {

    private final static Logger logger = LogManager.getLogger("importer.jdbc.sink.rebuild");

    private static final String ALIAS_SETTING = "alias";

    private static final String INDEX_PREFIX_SETTING = "index_prefix";

    private static final String KEEP_LAST_INDICES_SETTING = "keep_last_indices";

    private static final String TEMPLATE_SETTING = "template";

    private String index_prefix;

    private String alias;

    private String rebuild_index;

    private int keep_last_indices;

    @Override
    public String strategy() {
        return StrategyConstants.STRATEGY_NAME;
    }

    @Override
    public RebuildSink newInstance() {
        return new RebuildSink();
    }

    @Override
    public RebuildSink<C> setContext(StandardContext context) {
        super.setContext(context);

        if (isBlank(context.getSettings().get(ALIAS_SETTING))) {
            logger.error("'alias' argument is missing!");
        }
        this.alias = context.getSettings().get(ALIAS_SETTING);

        if (isNotBlank(context.getSettings().get(INDEX_PREFIX_SETTING))) {
            this.index_prefix = context.getSettings().get(INDEX_PREFIX_SETTING);
        } else {
            this.index_prefix = alias + "_";
        }

        if (isNotBlank(context.getSettings().get(KEEP_LAST_INDICES_SETTING))) {
            this.keep_last_indices = Integer.parseInt(context.getSettings().get(KEEP_LAST_INDICES_SETTING));
        } else {
            this.keep_last_indices = 0;
        }

        return this;
    }

    @Override
    public synchronized void beforeFetch() throws IOException {
        Ingest ingest = context.getOrCreateIngest(getMetric());

        if (ingest == null) {
            logger.warn("no ingest found");
            return;
        }
        createIndex();

        long startRefreshInterval = indexSettings != null ?
                indexSettings.getAsTime("bulk." + rebuild_index + ".refresh_interval.start", indexSettings.getAsTime("index.refresh_interval", TimeValue.timeValueSeconds(-1))).getMillis() : -1L;
        long stopRefreshInterval = indexSettings != null ?
                indexSettings.getAsTime("bulk." + rebuild_index + ".refresh_interval.stop", indexSettings.getAsTime("index.refresh_interval", TimeValue.timeValueSeconds(1))).getMillis() : 1000L;
        ingest.startBulk(rebuild_index, startRefreshInterval, stopRefreshInterval);
    }

    private void createIndex() throws IOException {
        Ingest ingest = context.getOrCreateIngest(getMetric());
        rebuild_index = generateNewIndexName();
        index = rebuild_index;

        logger.info("creating index {}", rebuild_index);

        CreateIndexRequestBuilder createIndexRequestBuilder =
                ingest.client().admin().indices().prepareCreate(rebuild_index);

        if (isNotBlank(context.getSettings().get(TEMPLATE_SETTING))) {
            addTemplateToIndexRequest(createIndexRequestBuilder);
        }

        createIndexRequestBuilder.execute().actionGet();
    }

    private void addTemplateToIndexRequest(CreateIndexRequestBuilder createIndexRequestBuilder) throws IOException {
        String templateSource = context.getSettings().get(TEMPLATE_SETTING);
        File templateFile = new File(templateSource);
        if (StringUtils.isNotBlank(templateSource) && !templateFile.exists()) {
            logger.error("Template file {} can not be found! Feeder will be stopped.", templateSource);
            System.exit(-1);
        }
        byte[] fileBytes = Files.readAllBytes(templateFile.toPath());
        createIndexRequestBuilder.setSource(fileBytes);
    }

    private String generateNewIndexName() {
        return index_prefix + (index_prefix.endsWith("_") ? "" : "_" ) + TimestampUtil.getTimestamp();
    }


    @Override
    public synchronized void afterFetch() throws IOException {
        Ingest ingest = context.getIngest();

        if(ingest == null) {
            return;
        }

        ingest.stopBulk(rebuild_index);
        ingest.refreshIndex(rebuild_index);
        if (getMetric().indices() != null && !getMetric().indices().isEmpty()) {
            for (String index : ImmutableSet.copyOf(getMetric().indices())) {
                logger.info("stopping bulk mode for index {} and refreshing...", rebuild_index);
                ingest.stopBulk(index);
                ingest.refreshIndex(index);
            }
        }

        List<String> existingIndices = getCurrentActiveIndices();
        switchAliasToNewIndex(existingIndices);

        // Remove old indices
        List<String> allExistingIndices = getAllExistingIndices();
        allExistingIndices.remove(rebuild_index);

        removeOldIndices(allExistingIndices);
    }

    @Override
    public void index(IndexableObject object, boolean create) throws IOException {
        object.index(rebuild_index);
        super.index(object, create);
    }

    private List<String> getOrderedIndexList(List<String> indexList) {
        
        Map<DateTime, String> dateIndexMap = new HashMap<>();
        List<DateTime> datetimeList = new ArrayList<>();

        for (String foundIndex : indexList) {
            DateTime indexDateTime = TimestampUtil.getDateTime(foundIndex.substring(foundIndex.lastIndexOf("_") + 1));
            dateIndexMap.put(indexDateTime, foundIndex);
            datetimeList.add(indexDateTime);
            logger.info("Adding index {} to map with datetime {}", foundIndex, indexDateTime);
        }

        datetimeList.sort(new Comparator<DateTime>() {
            @Override
            public int compare(DateTime o1, DateTime o2) {
                return o1.compareTo(o2);
            }
        });

        List<String> orderedIndexList = new ArrayList<>();
        for (DateTime aDatetimeList : datetimeList) {
            orderedIndexList.add(dateIndexMap.get(aDatetimeList));
        }

        return orderedIndexList;
    }

    private List<String> getAllExistingIndices() {
        Ingest ingest = context.getIngest();
        try {
            GetIndexResponse indexResponse = ingest.client().admin().indices().prepareGetIndex().addIndices(index_prefix + "*").execute().get();

            logger.debug("Found indexes with index_prefix is \"{}\": {}", index_prefix, indexResponse.indices().length);

            List<String> indexList =  new ArrayList<String>(Arrays.asList(indexResponse.indices()));

            return indexList;

        } catch (InterruptedException e) {
           logger.error(e.getMessage(), e);
           throw new RuntimeException(e.getMessage());
        } catch (ExecutionException e) {
           logger.error(e.getMessage(), e);
           throw new RuntimeException(e.getMessage());
        }
    }

    private List<String> getCurrentActiveIndices() {
        Ingest ingest = context.getIngest();
        GetAliasesResponse getAliasesResponse = ingest.client().admin().indices().prepareGetAliases(alias).execute().actionGet();

        List<String> existingIndices = new ArrayList<>();
        Iterator<ObjectCursor<String>> keysIterator = getAliasesResponse.getAliases().keys().iterator();
        while (keysIterator.hasNext()) {
            String existingIndex = keysIterator.next().value;
            existingIndices.add(existingIndex);
        }

        return existingIndices;
    }

    private void removeOldIndices(List<String> indices) {
        Ingest ingest = context.getIngest();
        List<String> orderedIndexList = getOrderedIndexList(indices);
        for (int i = 0; i < (orderedIndexList.size() - keep_last_indices); i++) {
            String existingIndex = orderedIndexList.get(i);
            logger.info("DELETE existing index {}", existingIndex);
            ingest.deleteIndex(existingIndex);
        }
    }

    private void switchAliasToNewIndex(List<String> existingIndices) {
        Ingest ingest = context.getIngest();
        IndicesAliasesRequestBuilder prepareAliasesRequest = ingest.client().admin().indices().prepareAliases();
        prepareAliasesRequest.addAlias(rebuild_index, alias).execute().actionGet().isAcknowledged();

        Iterator<String> existingIndicesIterator = existingIndices.iterator();

        while (existingIndicesIterator.hasNext()) {
            final String exisistingIndex = existingIndicesIterator.next();
            logger.info("Found existing alias binding: [{} -> {}]", alias, exisistingIndex);
            if (exisistingIndex.startsWith(index_prefix)) {
                logger.info("Mark alias binding: [{} -> {}] for unbinding", alias, exisistingIndex);
                prepareAliasesRequest.removeAlias(exisistingIndex, alias);
            }
        }

        logger.info("Execute alias switch");

        prepareAliasesRequest.execute();
    }

    @Override
    public void delete(IndexableObject object) {
        logger.error("Delete operation is unsupported for the RebuildStrategy!");
    }

}
