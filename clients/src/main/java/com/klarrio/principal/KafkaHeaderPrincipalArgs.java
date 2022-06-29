package com.klarrio.principal;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;

import java.util.ArrayList;

public class KafkaHeaderPrincipalArgs {

    private final ArrayList<String> listeners;
    private final boolean loggingEnabled;
    private final String selfUser;
    private final String connectionId;
    private final RequestHeader header;
    private final ListenerName listenerName;
    private final boolean isPrivilegedListener;
    private final SecurityProtocol securityProtocol;
    private final KafkaPrincipal upstreamPrincipal;

    public KafkaHeaderPrincipalArgs(String[] listeners,
                                    boolean loggingEnabled,
                                    String selfUser,
                                    String connectionId,
                                    RequestHeader header,
                                    ListenerName listenerName,
                                    boolean isPrivilegedListener,
                                    SecurityProtocol securityProtocol,
                                    KafkaPrincipal upstreamPrincipal) {
        this.listeners = new ArrayList<>();
        for (int i=0; i<listeners.length; i++) {
            this.listeners.add(listeners[i]);
        }
        this.loggingEnabled = loggingEnabled;
        this.selfUser = selfUser;
        this.connectionId = connectionId;
        this.header = header;
        this.listenerName = listenerName;
        this.isPrivilegedListener = isPrivilegedListener;
        this.securityProtocol = securityProtocol;
        this.upstreamPrincipal = upstreamPrincipal;
    }

    public ArrayList<String> getListeners() {
        return listeners;
    }

    public boolean isLoggingEnabled() {
        return loggingEnabled;
    }

    public String getSelfUser() {
        return selfUser;
    }

    public String getConnectionId() {
        return connectionId;
    }

    public RequestHeader getHeader() {
        return header;
    }

    public ListenerName getListenerName() {
        return listenerName;
    }

    public boolean isPrivilegedListener() {
        return isPrivilegedListener;
    }

    public SecurityProtocol getSecurityProtocol() {
        return securityProtocol;
    }

    public KafkaPrincipal getUpstreamPrincipal() {
        return upstreamPrincipal;
    }
}
