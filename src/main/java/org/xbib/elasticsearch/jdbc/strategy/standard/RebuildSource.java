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

/**
 * River source implementation for the 'rebuild' strategy
 *
 * @author <a href="feaster83@gmail.com">Jasper Huzen</a>
 */
public class RebuildSource extends StandardSource {

    private final static Logger logger = LogManager.getLogger("importer.jdbc.source.rebuild");

    @Override
    public String strategy() {
        return StrategyConstants.STRATEGY_NAME;
    }

    @Override
    public RebuildSource newInstance() {
      return new RebuildSource();
    }

}
