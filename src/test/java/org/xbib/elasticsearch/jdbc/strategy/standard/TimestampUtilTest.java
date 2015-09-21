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

import org.elasticsearch.common.joda.time.DateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TimestampUtilTest {

    @Test
    public void parseDates() throws InterruptedException {
        // Loop 1000 seconds to test approx. every millisecond from a minute in a conversion
        for (int i = 0; i++ < 1200; i++) {
            String date = TimestampUtil.getTimestamp();
            DateTime dateTime = TimestampUtil.getDateTime(date);
            assertEquals(date, TimestampUtil.getTimestamp(dateTime));
            Thread.sleep(1);
        }

    }

    @Test (expected = IllegalArgumentException.class)
    public void ParseInvalidDate1() {
        String date = "2015-05-40-10-00-08-1012";
        TimestampUtil.getDateTime(date);
    }



}
