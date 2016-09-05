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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesAction;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequestBuilder;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexAction;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.get.GetIndexAction;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;
import org.xbib.elasticsearch.common.util.IndexableObject;
import org.xbib.elasticsearch.helper.client.ClientAPI;
import org.xbib.elasticsearch.helper.client.ClientBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;

import static org.apache.logging.log4j.util.Strings.isBlank;
import static org.apache.logging.log4j.util.Strings.isNotBlank;

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


    private ClientAPI createClient(Settings settings) {
        Settings.Builder settingsBuilder = Settings.settingsBuilder()
                .put("cluster.name", settings.get("elasticsearch.cluster.name", settings.get("elasticsearch.cluster", "elasticsearch")))
                .putArray("host", settings.getAsArray("elasticsearch.host"))
                .put("port", settings.getAsInt("elasticsearch.port", 9300))
                .put("sniff", settings.getAsBoolean("elasticsearch.sniff", false))
                .put("autodiscover", settings.getAsBoolean("elasticsearch.autodiscover", false))
                .put("name", "importer") // prevents lookup of names.txt, we don't have it
                .put("client.transport.ignore_cluster_name", false) // ignore cluster name setting
                .put("client.transport.ping_timeout", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))) //  ping timeout
                .put("client.transport.nodes_sampler_interval", settings.getAsTime("elasticsearch.timeout", TimeValue.timeValueSeconds(5))); // for sniff sampling
        // optional found.no transport plugin
        if (settings.get("transport.type") != null) {
            settingsBuilder.put("transport.type", settings.get("transport.type"));
        }
        // copy found.no transport settings
        Settings foundTransportSettings = settings.getAsSettings("transport.found");
        if (foundTransportSettings != null) {
            Map<String,String> foundTransportSettingsMap = foundTransportSettings.getAsMap();
            for (Map.Entry<String,String> entry : foundTransportSettingsMap.entrySet()) {
                settingsBuilder.put("transport.found." + entry.getKey(), entry.getValue());
            }
        }
        return ClientBuilder.builder()
                .put(settingsBuilder.build())
                .put(ClientBuilder.MAX_ACTIONS_PER_REQUEST, settings.getAsInt("max_bulk_actions", 10000))
                .put(ClientBuilder.MAX_CONCURRENT_REQUESTS, settings.getAsInt("max_concurrent_bulk_requests",
                        Runtime.getRuntime().availableProcessors() * 2))
                .put(ClientBuilder.MAX_VOLUME_PER_REQUEST, settings.getAsBytesSize("max_bulk_volume", ByteSizeValue.parseBytesSizeValue("10m", "")))
                .put(ClientBuilder.FLUSH_INTERVAL, settings.getAsTime("flush_interval", TimeValue.timeValueSeconds(5)))
                .setMetric(getMetric())
                .toBulkTransportClient();
    }



    @Override
    public synchronized void beforeFetch() throws IOException {

        Settings settings = context.getSettings();
        String index = settings.get("index", "jdbc");
        String type = settings.get("type", "jdbc");

        if(clientAPI == null){
            clientAPI = createClient(settings);
        }

        this.setIndex(index);
        this.setType(type);

        createIndex();
        long startRefreshInterval = settings != null ?
                settings.getAsTime("bulk." + rebuild_index + ".refresh_interval.start", settings.getAsTime("index.refresh_interval", TimeValue.timeValueSeconds(-1))).getMillis() : -1L;
        long stopRefreshInterval = settings != null ?
                settings.getAsTime("bulk." + rebuild_index + ".refresh_interval.stop", settings.getAsTime("index.refresh_interval", TimeValue.timeValueSeconds(1))).getMillis() : 1000L;
        clientAPI.startBulk(rebuild_index, startRefreshInterval, stopRefreshInterval);
    }

    private void createIndex() throws IOException {
        if(clientAPI == null){
            logger.error("clientAPI is null");
        }
        rebuild_index = generateNewIndexName();
        index = rebuild_index;

        logger.info("creating index {}", rebuild_index);

        ElasticsearchClient esClient = clientAPI.client();
        CreateIndexRequestBuilder createIndexRequestBuilder = new CreateIndexRequestBuilder(esClient, CreateIndexAction.INSTANCE, rebuild_index);

        if (isNotBlank(context.getSettings().get(TEMPLATE_SETTING))) {
            addTemplateToIndexRequest(createIndexRequestBuilder);
        }

        createIndexRequestBuilder.execute().actionGet();
        logger.info("index {} created", rebuild_index);
    }

    private void addTemplateToIndexRequest(CreateIndexRequestBuilder createIndexRequestBuilder) throws IOException {
        String templateSource = context.getSettings().get(TEMPLATE_SETTING);
        File templateFile = new File(templateSource);
        if (isNotBlank(templateSource) && !templateFile.exists()) {
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

        logger.info("starting afterFetch() with rebuild_index: " + rebuild_index);

        if(clientAPI != null) {
            flushIngest();
            clientAPI.stopBulk(rebuild_index);
            clientAPI.refreshIndex(rebuild_index);
            if (getMetric().indices() != null && !getMetric().indices().isEmpty()) {
                for (String index : ImmutableSet.copyOf(getMetric().indices())) {
                    logger.info("stopping bulk mode for index {} and refreshing...", rebuild_index);
                    clientAPI.stopBulk(index);
                    clientAPI.refreshIndex(index);
                }
            }

            List<String> existingIndices = getCurrentActiveIndices();
            switchAliasToNewIndex(existingIndices);

            // Remove old indices
            List<String> allExistingIndices = getAllExistingIndices();
            allExistingIndices.remove(rebuild_index);

            removeOldIndices(allExistingIndices);
            clientAPI.shutdown();
        }
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

        Collections.sort(datetimeList);

        List<String> orderedIndexList = new ArrayList<>();
        for (DateTime aDatetimeList : datetimeList) {
            orderedIndexList.add(dateIndexMap.get(aDatetimeList));
        }

        return orderedIndexList;
    }

    private List<String> getAllExistingIndices() {
        if(clientAPI == null){
            logger.error("clientAPI is null!");
            return null;
        }

        ElasticsearchClient esClient = clientAPI.client();
        GetIndexResponse getIndexResponse = esClient.prepareExecute(GetIndexAction.INSTANCE).setIndices(index_prefix + "*").execute().actionGet();
        List<String> indexList = new ArrayList<>();
        String[] indices = getIndexResponse.getIndices();
        if (indices!= null && indices.length > 0) {
            indexList.addAll(Arrays.asList(indices));
        }
        return indexList;
    }

    private List<String> getCurrentActiveIndices() {
        if(clientAPI == null){
            logger.error("clientAPI is null!");
            return null;
        }

        ElasticsearchClient esClient = clientAPI.client();
        GetAliasesResponse getAliasesResponse = esClient.prepareExecute(GetAliasesAction.INSTANCE).setAliases(alias).execute().actionGet();

        List<String> existingIndices = new ArrayList<>();
        Iterator<ObjectCursor<String>> keysIterator = getAliasesResponse.getAliases().keys().iterator();
        while (keysIterator.hasNext()) {
            String existingIndex = keysIterator.next().value;
            existingIndices.add(existingIndex);
        }

        return existingIndices;
    }

    private void removeOldIndices(List<String> indices) {
        if(clientAPI != null){
            List<String> orderedIndexList = getOrderedIndexList(indices);
            for (int i = 0; i < (orderedIndexList.size() - keep_last_indices); i++) {
                String existingIndex = orderedIndexList.get(i);
                logger.info("DELETE existing index {}", existingIndex);
                clientAPI.deleteIndex(existingIndex);
            }
        }
    }

    private void switchAliasToNewIndex(List<String> existingIndices) {
        logger.info("starting switchAliasToNewIndex() with rebuild_index: " + rebuild_index + "and alias: " +alias);

        ElasticsearchClient esClient = clientAPI.client();
        IndicesAliasesRequestBuilder prepareAliasesRequest = new IndicesAliasesRequestBuilder(esClient, IndicesAliasesAction.INSTANCE);
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
