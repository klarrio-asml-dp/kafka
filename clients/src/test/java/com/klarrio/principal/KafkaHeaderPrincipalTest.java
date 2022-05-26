package com.klarrio.principal;

import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;

// Run tests with
// ./gradlew clients:test -x :clients:checkstyleMain -x :clients:checkstyleTest --tests KafkaHeaderPrincipalTest
public class KafkaHeaderPrincipalTest {
    @Test
    public void testPrincipalAddRemove() {

        String connectionId = "10.244.1.4:9092-127.0.0.6:57331-1";
        String removableConnectionId = "10.244.1.4:9092-127.0.0.6:57331-1";
        String[] listeners = {"PLAIN-9092"};
        boolean loggingEnabled = true;
        KafkaHeaderPrincipalArgs args = new KafkaHeaderPrincipalArgs(
                listeners,
                loggingEnabled,
                connectionId,
                new RequestHeader(ApiKeys.METADATA, (short)1, "ns-sa", 0),
                new ListenerName("test"),
                false,
                SecurityProtocol.PLAINTEXT,
                new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "")
        );
        KafkaPrincipal p = KafkaHeaderPrincipal.principal(args);
        KafkaPrincipal r = KafkaHeaderPrincipal.cleanup(removableConnectionId, loggingEnabled);

        assertNotNull(p);
        assertNotNull(r);

    }
}
