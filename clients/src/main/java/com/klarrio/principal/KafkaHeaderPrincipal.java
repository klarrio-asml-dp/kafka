package com.klarrio.principal;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.util.concurrent.ConcurrentHashMap;

public class KafkaHeaderPrincipal {

    public static final String PROPERTY_LOGGING_ENABLED = "com.klarrio.principal.logging-enabled";
    public static final String PROPERTY_LISTENERS = "com.klarrio.principal.listeners";
    public static final String PROPERTY_SELF_USER = "com.klarrio.principal.self-user";
    public static final String[] DEFAULT_LISTENERS = {"NOT-CONFIGURED-9092"};
    public static final String LISTENER_SEPARATOR = ",";
    public static final String DEFAULT_TRUE_VALUE = "true";
    public static final String DEFAULT_SELF_USER = "ANONYMOUS";

    private static final ConcurrentHashMap<String, KafkaPrincipal> principals = new ConcurrentHashMap<>();

    /**
     * Cleans up KafkaPrincipal instances for disconnected connection ids.
     * This method is called from SocketServer.scala Processor.processDisconnected().
     *
     * @param connectionId Connection id for which the principal should be cleaned up.
     * @return Removed KafkaPrincipal instance or null, if there was no principal for ta connection id.
     */
    public static KafkaPrincipal cleanup(String connectionId, boolean loggingEnabled) {
        KafkaPrincipal removed = principals.remove(connectionId);
        if (loggingEnabled) {
            String messagePost = String.format("KafkaHeaderPrincipal: Removed a principal for %s, removed value was: %s",
                    connectionId,
                    removed);
            System.out.println(messagePost);
        }
        return removed;
    }

    /**
     * Computes a KafkaPrincipal for a connection id, if one doesn't exist, and returns it.
     *
     * This method could be configured to compute principals only for selected listeners
     * and return an upstream, original principal configured by Kafka if the listener
     * isn't the supported one.
     *
     * This method is called from SocketServer.scala Processor.processCompletedSends().
     *
     * @param args KafkaPrincipal build arguments.
     * @return KafkaPrincipal instance.
     */
    public static KafkaPrincipal principal(KafkaHeaderPrincipalArgs args) {
        // Create a principal if one for a connection id doesn't exist
        KafkaPrincipal p = principals.computeIfAbsent(args.getConnectionId(), (key) -> {
            if (args.isLoggingEnabled()) {
                String createMessage = String.format("KafkaHeaderPrincipal: Creating a new principal for connection id: %s, client id: %s",
                        args.getConnectionId(),
                        args.getHeader().clientId());
                System.out.println(createMessage);
            }
            String clientId = args.getHeader().clientId();
            boolean isSelfUser = !clientId.startsWith("spiffe://");
            String principalUser = isSelfUser ? args.getSelfUser() : clientId;
            return new KafkaPrincipal(
                    KafkaPrincipal.USER_TYPE,
                    principalUser);
        });
        // If we block the CONTROLPANE-9091, Strimzi entity operator will never come up.
        // --------------------------
        // Warning: CONTROLPANE-XXXX:
        // --------------------------
        // We do this check here, after potentially creating a principal for CONTROLPLANE-XXXX
        // so we get consistent, non-confusing logging in the cleanup. We have only consumed
        // a little amount of memory and spent an insignificant amount of time for
        // computeIfAbsent. Irrelevant.
        if (!args.getListeners().contains(args.getListenerName().value())) {
            return args.getUpstreamPrincipal();
        }
        if (args.isLoggingEnabled()) {
            String message = String.format("KafkaHeaderPrincipal: Handling %s for API key %d",
                    p.getName(),
                    args.getHeader().apiKey().id);
            System.out.println(message);
        }
        return p;
    }

}
