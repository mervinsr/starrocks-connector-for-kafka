import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// Produces JSON rows {"id":..,"name":..,"score":..,"pad":..} with ids first..first+count-1 at a target rate.
// Usage: java -cp "$KAFKA_HOME/libs/*" RateProducer.java <bootstrap> <topic> <firstId> <count> <rowsPerSec> <padBytes>
// rowsPerSec <= 0 means as fast as possible.
public class RateProducer {

    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String topic = args[1];
        long first = Long.parseLong(args[2]);
        long count = Long.parseLong(args[3]);
        long rate = Long.parseLong(args[4]);
        int padBytes = Integer.parseInt(args[5]);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(512 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(256L * 1024 * 1024));

        StringBuilder padBuilder = new StringBuilder();
        for (int i = 0; i < padBytes; i++) {
            padBuilder.append((char) ('a' + i % 26));
        }
        String pad = padBuilder.toString();
        AtomicLong failures = new AtomicLong();
        long start = System.currentTimeMillis();
        long lastReport = start;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            long sent = 0;
            while (sent < count) {
                // Send in 10 ms ticks to hold the rate.
                long target = rate <= 0 ? count : Math.min(count, (System.currentTimeMillis() - start + 10) * rate / 1000);
                for (; sent < target; sent++) {
                    long id = first + sent;
                    String value = "{\"id\":" + id + ",\"name\":\"n" + id + "\",\"score\":" + (id % 100)
                            + ",\"pad\":\"" + pad + "\"}";
                    producer.send(new ProducerRecord<>(topic, String.valueOf(id), value), (metadata, e) -> {
                        if (e != null) {
                            failures.incrementAndGet();
                        }
                    });
                }
                long now = System.currentTimeMillis();
                if (now - lastReport >= 10000) {
                    System.out.printf("producer: sent %d, %.0f rows/s%n", sent, sent * 1000.0 / (now - start));
                    lastReport = now;
                }
                if (rate > 0 && sent < count) {
                    Thread.sleep(10);
                }
            }
            producer.flush();
        }
        long cost = System.currentTimeMillis() - start;
        System.out.printf("producer: done %d rows in %d ms, %.0f rows/s, failures %d%n",
                count, cost, count * 1000.0 / Math.max(1, cost), failures.get());
        if (failures.get() > 0) {
            System.exit(1);
        }
    }
}
