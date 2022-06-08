/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.security.auth;

import java.net.InetAddress;
import java.nio.ByteBuffer;

import org.apache.kafka.common.network.PlaintextTransportLayer;

public class PlaintextAuthenticationContext implements AuthenticationContext {
    private final InetAddress clientAddress;
    private final String listenerName;
    private final String clientId;

    public PlaintextAuthenticationContext(InetAddress clientAddress, String listenerName, String clientId) {
        this.clientAddress = clientAddress;
        this.listenerName = listenerName;
        this.clientId = clientId;

        System.out.println("@@@@ PlainTextAuthenticationContext clientId " + clientId);
    }

    @Override
    public SecurityProtocol securityProtocol() {
        return SecurityProtocol.PLAINTEXT;
    }

    @Override
    public InetAddress clientAddress() {
        return clientAddress;
    }

    @Override
    public String listenerName() {
        return listenerName;
    }

    public String clientId() {
        return "CN=" + clientId;
    }
}
