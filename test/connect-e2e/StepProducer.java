import java.io.File;
import java.io.FileWriter;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

// Produces JSON rows {"id":..,"name":..,"score":..,"pad":..} following a schedule of rates, without a gap between
// the steps, and writes "<epoch ms> <rows sent>" to a progress file every second. With several topics, the rows go
// to them in turn.
// With a number of keys, row n updates key first + n % keys to version n / keys, so the last version of every key
// follows from the rows sent. With delete every d, a version v of key k deletes it when (v + k) % d == d - 1, so
// every d-th version of a key is a delete and about one key in d ends deleted.
// Usage: java -cp <classes>:"$KAFKA_HOME/libs/*" StepProducer <bootstrap> <topic>[,<topic>...] <first id> <schedule>
//        <pad bytes> <progress file> [<keys> [<delete every>]], where schedule is rows/s:seconds,rows/s:seconds,...
public class StepProducer {

    public static void main(String[] args) throws Exception {
        String bootstrap = args[0];
        String[] topics = args[1].split(",");
        long first = Long.parseLong(args[2]);
        String[] steps = args[3].split(",");
        int padBytes = Integer.parseInt(args[4]);
        File progress = new File(args[5]);
        long keys = args.length > 6 ? Long.parseLong(args[6]) : 0;
        long deleteEvery = args.length > 7 ? Long.parseLong(args[7]) : 0;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, "5");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(512 * 1024));
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, String.valueOf(512L * 1024 * 1024));

        StringBuilder padBuilder = new StringBuilder();
        for (int i = 0; i < padBytes; i++) {
            padBuilder.append((char) ('a' + i % 26));
        }
        String pad = padBuilder.toString();
        AtomicLong failures = new AtomicLong();
        long sent = 0;
        long lastProgress = 0;
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (String step : steps) {
                long rate = Long.parseLong(step.split(":")[0]);
                long seconds = Long.parseLong(step.split(":")[1]);
                long stepStart = System.currentTimeMillis();
                long stepSent = 0;
                while (System.currentTimeMillis() - stepStart < seconds * 1000) {
                    // Send in 10 ms ticks to hold the rate of the step.
                    long target = (System.currentTimeMillis() - stepStart + 10) * rate / 1000;
                    for (; stepSent < target; stepSent++, sent++) {
                        String value;
                        long id;
                        if (keys > 0) {
                            id = first + sent % keys;
                            long version = sent / keys;
                            int op = deleteEvery > 0 && (version + id - first) % deleteEvery == deleteEvery - 1 ? 1 : 0;
                            value = "{\"id\":" + id + ",\"ver\":" + version + ",\"name\":\"n" + id + "-" + version
                                    + "\",\"score\":" + (version % 100) + ",\"pad\":\"" + pad + "\",\"__op\":" + op + "}";
                        } else {
                            id = first + sent;
                            value = "{\"id\":" + id + ",\"name\":\"n" + id + "\",\"score\":" + (id % 100)
                                    + ",\"pad\":\"" + pad + "\"}";
                        }
                        // All versions of a key go to the same topic, and with the key to the same partition.
                        String topic = topics[(int) ((keys > 0 ? id - first : sent) % topics.length)];
                        producer.send(new ProducerRecord<>(topic, String.valueOf(id), value), (metadata, e) -> {
                            if (e != null) {
                                failures.incrementAndGet();
                            }
                        });
                    }
                    long now = System.currentTimeMillis();
                    if (now - lastProgress >= 1000) {
                        writeProgress(progress, now, sent);
                        lastProgress = now;
                    }
                    Thread.sleep(10);
                }
            }
            producer.flush();
        }
        writeProgress(progress, System.currentTimeMillis(), sent);
        System.out.printf("producer: done %d rows, failures %d%n", sent, failures.get());
        System.exit(failures.get() > 0 ? 1 : 0);
    }

    private static void writeProgress(File file, long now, long sent) throws Exception {
        File tmp = new File(file.getPath() + ".tmp");
        try (FileWriter writer = new FileWriter(tmp)) {
            writer.write(now + " " + sent + "\n");
        }
        if (!tmp.renameTo(file)) {
            throw new IllegalStateException("cannot write " + file);
        }
    }
}
