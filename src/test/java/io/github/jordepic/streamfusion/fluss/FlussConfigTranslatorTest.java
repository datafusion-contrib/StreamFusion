package io.github.jordepic.streamfusion.fluss;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class FlussConfigTranslatorTest {

  private static Map<String, String> translated(Map<String, String> options) {
    FlussConfigTranslator.Result result = FlussConfigTranslator.translate(options);
    assertTrue(
        result.isTranslated(), () -> "expected translation, got " + result.fallbackReason());
    return result.config();
  }

  private static String fallback(Map<String, String> options) {
    FlussConfigTranslator.Result result = FlussConfigTranslator.translate(options);
    assertFalse(result.isTranslated(), "expected fallback, got translation");
    return result.fallbackReason().orElseThrow();
  }

  @Test
  void mapsScannerClientKnobsToRustConfigFields() {
    Map<String, String> config =
        translated(
            Map.of(
                "bootstrap.servers", "localhost:9123",
                "client.scanner.log.max-poll-records", "2048",
                "client.scanner.log.fetch.max-bytes", "16 mb",
                "client.scanner.log.fetch.max-bytes-for-bucket", "1 mb",
                "client.scanner.log.fetch.min-bytes", "1 bytes",
                "client.scanner.log.fetch.wait-max-time", "PT0.5S",
                "client.scanner.remote-log.prefetch-num", "8",
                "client.remote-file.download-thread-num", "4"));

    assertEquals("localhost:9123", config.get("bootstrap_servers"));
    assertEquals("2048", config.get("scanner_log_max_poll_records"));
    assertEquals("16777216", config.get("scanner_log_fetch_max_bytes"));
    assertEquals("1048576", config.get("scanner_log_fetch_max_bytes_for_bucket"));
    assertEquals("1", config.get("scanner_log_fetch_min_bytes"));
    assertEquals("500", config.get("scanner_log_fetch_wait_max_time_ms"));
    assertEquals("8", config.get("scanner_remote_log_prefetch_num"));
    assertEquals("4", config.get("remote_file_download_thread_num"));
  }

  @Test
  void keepsFlinkCoordinationOptionsOutOfNativeConfig() {
    FlussConfigTranslator.Result result =
        FlussConfigTranslator.translate(
            Map.of(
                "bootstrap.servers", "localhost:9123",
                "scan.startup.mode", "earliest",
                "scan.partition.discovery.interval", "1 min"));

    assertTrue(result.isTranslated());
    assertFalse(result.config().containsKey("scan.startup.mode"));
    assertFalse(result.config().containsKey("scan.partition.discovery.interval"));
    assertEquals("earliest", result.coordinationOptions().get("scan.startup.mode"));
    assertEquals("1 min", result.coordinationOptions().get("scan.partition.discovery.interval"));
  }

  @Test
  void mapsBasicPlainSaslButRejectsUnsupportedMechanism() {
    Map<String, String> config =
        translated(
            Map.of(
                "client.security.protocol", "sasl",
                "client.security.sasl.mechanism", "PLAIN",
                "client.security.sasl.username", "alice",
                "client.security.sasl.password", "secret"));

    assertEquals("sasl", config.get("security_protocol"));
    assertEquals("PLAIN", config.get("security_sasl_mechanism"));
    assertEquals("alice", config.get("security_sasl_username"));
    assertEquals("secret", config.get("security_sasl_password"));
    assertTrue(
        fallback(
                Map.of(
                    "client.security.protocol", "sasl",
                    "client.security.sasl.mechanism", "SCRAM-SHA-256"))
            .contains("PLAIN"));
  }

  @Test
  void fallsBackOnClientOptionsNotRepresentedInFlussRsConfig() {
    assertTrue(fallback(Map.of("client.request-timeout", "30 s")).contains("request-timeout"));
    assertTrue(fallback(Map.of("client.scanner.log.check-crc", "true")).contains("check-crc"));
    assertTrue(fallback(Map.of("client.security.sasl.jaas.config", "x")).contains("jaas"));
    assertTrue(fallback(Map.of("client.scanner.future-option", "true")).contains("unmapped"));
  }
}
