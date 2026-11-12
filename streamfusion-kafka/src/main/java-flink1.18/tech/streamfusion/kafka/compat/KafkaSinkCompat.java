package tech.streamfusion.kafka.compat;

import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;

/** Transaction naming is a connector option introduced after the 1.18 connector release. */
public final class KafkaSinkCompat {
  private KafkaSinkCompat() {}

  public static String transactionNaming(String option) {
    if (option != null)
      throw new IllegalArgumentException("Flink 1.18 has no transaction naming strategy option");
    return "DEFAULT";
  }

  public static <T> void setTransactionNaming(KafkaSinkBuilder<T> builder, String strategy) {
    if (!"DEFAULT".equals(strategy))
      throw new IllegalArgumentException("Unsupported transaction naming: " + strategy);
  }
}
