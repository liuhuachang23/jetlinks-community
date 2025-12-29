/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
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
package org.jetlinks.community.network.monitor;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;

@AllArgsConstructor
public class LogNetMonitor implements NetMonitor {

    private Logger logger;

    @Override
    public void buffered(int size) {
        if(size>0) {
            logger.info("current buffered : {}", size);
        }
    }

    @Override
    public void error(Throwable err) {
        logger.error(err.getMessage(), err);
    }

    @Override
    public void handled() {
    }

    @Override
    public void send() {

    }

    @Override
    public void bytesSent(long bytesLength) {
        logger.info("send number of bytes :{}",bytesLength);
    }

    @Override
    public void bytesRead(long bytesLength) {
        logger.info("receive number of bytes :{}",bytesLength);
    }


    @Override
    public void sendComplete() {

    }


    @Override
    public void sendError(Throwable err) {
        logger.error("send error", err);
    }

    @Override
    public void connected() {
    }

    @Override
    public void disconnected() {
    }
}
