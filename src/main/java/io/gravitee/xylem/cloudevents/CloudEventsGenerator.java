package io.gravitee.xylem.cloudevents;

import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.kafka.CloudEventSerializer;
import io.cloudevents.protobuf.ProtoCloudEventData;
import io.cloudevents.protobuf.ProtobufFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;

/**
 * @author David BRASSELY (david.brassely at graviteesource.com)
 * @author GraviteeSource Team
 */
public class CloudEventsGenerator {

    public static void main(String[] args) {
        com.google.protobuf.Message myMessage = com.xylem.helios.fcw.event.api.WaterMeterEvent.Event
                .newBuilder()
                .setAppCode(108)
                .setMeterId("M11019450")
                .setDeviceType(47)
                .setFlexnetId(11019450)
                .setMeterClass("WMC_SENSUS_ECR_BIDI")
                .setMeterType("WMT_SMARTMETER_GEN2_TOUCH")
                .setTimeOfIntercept(1705341043368L)
                .build();

        CloudEventData ceData = ProtoCloudEventData.wrap(myMessage);

        CloudEvent event = CloudEventBuilder.v1()
                .withId("3u97k8nx2sxar")
                .withType("com.xylem.helios.fcw.event.api.WaterMeterEvent/v1")
                .withSource(URI.create("http://localhost"))
                .withData(ceData)
                .withExtension("id", "3u97k8nx2sxar")
                .withExtension("source", "he-andorian-adaptor-5886dd55f5-c5n4n")
                .build();

        Properties props = new Properties();

        // Other config props
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CloudEventSerializer.class);

        try (KafkaProducer<String, CloudEvent> producer = new KafkaProducer<>(props)) {
            // Produce the event
            producer.send(new ProducerRecord<>("xylem", event));
        }
    }
}
