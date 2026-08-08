/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package io.questdb.test.tls;

import io.questdb.ConfigPropertyKey;
import io.questdb.DynamicPropServerConfiguration;
import io.questdb.PropertyKey;
import io.questdb.test.tools.TestUtils;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Stream;

import static org.junit.Assert.assertFalse;

public class TlsReloadTest {

    private static final PropertyKey[] TLS_PROPERTY_KEYS = {
            PropertyKey.TLS_ENABLED,
            PropertyKey.TLS_CERT_PATH,
            PropertyKey.TLS_PRIVATE_KEY_PATH,
            PropertyKey.HTTP_TLS_ENABLED,
            PropertyKey.HTTP_TLS_CERT_PATH,
            PropertyKey.HTTP_TLS_PRIVATE_KEY_PATH,
            PropertyKey.HTTP_MIN_TLS_ENABLED,
            PropertyKey.HTTP_MIN_TLS_CERT_PATH,
            PropertyKey.HTTP_MIN_TLS_PRIVATE_KEY_PATH,
            PropertyKey.LINE_TCP_TLS_ENABLED,
            PropertyKey.LINE_TCP_TLS_CERT_PATH,
            PropertyKey.LINE_TCP_TLS_PRIVATE_KEY_PATH,
            PropertyKey.PG_TLS_ENABLED,
            PropertyKey.PG_TLS_CERT_PATH,
            PropertyKey.PG_TLS_PRIVATE_KEY_PATH,
    };

    @SuppressWarnings("unchecked")
    @Test
    public void testTlsPropertiesNotDynamic() throws Exception {
        TestUtils.assertMemoryLeak(() -> {
            final var field = DynamicPropServerConfiguration.class.getDeclaredField("dynamicProps");
            field.setAccessible(true);
            final var dynamicProps = (Set<? extends ConfigPropertyKey>) field.get(null);
            Stream.of(TLS_PROPERTY_KEYS).forEach(key ->
                    assertFalse(
                            "TLS property " + key.getPropertyPath() + " must not be dynamic (requires restart)",
                            dynamicProps.contains(key)
                    )
            );
        });
    }
}
