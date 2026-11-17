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

package io.questdb.tls;

import io.questdb.Bootstrap;
import io.questdb.ServerMain;
import io.questdb.log.LogFactory;

/**
 * Entry point that starts QuestDB with TLS support. It mirrors {@link ServerMain#main(String[])}, including its
 * failure handling, and differs only in the bootstrap configuration.
 */
public class TlsServerMain {
    public static void main(String[] args) {
        try {
            new ServerMain(new Bootstrap(new TlsBootstrapConfiguration(), args)).start(true);
        } catch (Throwable th) {
            try {
                if (th instanceof Bootstrap.BootstrapException e && e.isSilentStacktrace()) {
                    System.err.println(th.getMessage());
                } else {
                    printStackTraceSafely(th);
                }
            } catch (Throwable ignore) {
            }
            try {
                LogFactory.closeInstance();
            } catch (Throwable closeFailure) {
                printStackTraceSafely(closeFailure);
            } finally {
                System.exit(55);
            }
        }
    }

    private static void printStackTraceSafely(Throwable th) {
        try {
            //noinspection CallToPrintStackTrace
            th.printStackTrace();
        } catch (Throwable ignore) {
        }
    }
}
