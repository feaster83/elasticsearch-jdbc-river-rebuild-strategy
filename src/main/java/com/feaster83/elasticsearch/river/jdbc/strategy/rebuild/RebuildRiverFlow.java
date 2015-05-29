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

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.xbib.elasticsearch.river.jdbc.strategy.simple.SimpleRiverFlow;

/**
 * River flow implementation for the 'rebuild' strategy
 *
 * @author <a href="feaster83@gmail.com">Jasper Huzen</a>
 */
public class RebuildRiverFlow extends SimpleRiverFlow<RebuildRiverContext> {

    private static final ESLogger logger = ESLoggerFactory.getLogger("river.jdbc.RebuildRiverFlow");


    @Override
    public String strategy() {
        return "rebuild";
    }

    @Override
    public RebuildRiverFlow newInstance() {
        return new RebuildRiverFlow();
    }

    @Override
    public RebuildRiverContext newRiverContext() {
        return new RebuildRiverContext();
    }



}
