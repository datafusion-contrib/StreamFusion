package tech.streamfusion.kafka.compat;

import java.util.Locale;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.sink.TransactionNamingStrategy;

/** Transaction naming is a connector option introduced after the 1.18 connector release. */
public final class KafkaSinkCompat {
  private KafkaSinkCompat() {}

  public static String transactionNaming(String option) {
    return option == null || "default".equalsIgnoreCase(option)
        ? TransactionNamingStrategy.DEFAULT.name()
        : TransactionNamingStrategy.valueOf(option.replace('-', '_').toUpperCase(Locale.ROOT))
            .name();
  }

  public static <T> void setTransactionNaming(KafkaSinkBuilder<T> builder, String strategy) {
    builder.setTransactionNamingStrategy(TransactionNamingStrategy.valueOf(strategy));
  }
}
