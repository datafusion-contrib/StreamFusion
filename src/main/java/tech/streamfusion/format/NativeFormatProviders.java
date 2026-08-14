package tech.streamfusion.format;

import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Discovers installed native format artifacts through the same provider pattern Flink uses. */
public final class NativeFormatProviders {

  private static final Logger LOG = LoggerFactory.getLogger(NativeFormatProviders.class);

  private NativeFormatProviders() {}

  /** Returns the table's value-format name using Flink's {@code value.format} precedence. */
  public static String formatIdentifier(Map<String, String> options) {
    return options.getOrDefault("value.format", options.get("format"));
  }

  /** Finds an installed provider that accepts this table's exact decoder options. */
  public static Optional<NativeFormatProvider> find(NativeFormatContext context) {
    String identifier = formatIdentifier(context.options());
    return forIdentifier(identifier, provider -> provider.supports(context));
  }

  /** Finds an installed provider by format identifier, regardless of decode-option support. */
  public static Optional<NativeFormatProvider> forIdentifier(String identifier) {
    return forIdentifier(identifier, provider -> true);
  }

  private static Optional<NativeFormatProvider> forIdentifier(
      String identifier, Predicate<NativeFormatProvider> accepts) {
    if (identifier == null) {
      return Optional.empty();
    }
    ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
    ClassLoader providerLoader = NativeFormatProviders.class.getClassLoader();
    Set<String> seen = new HashSet<>();
    for (ClassLoader loader : new ClassLoader[] {contextLoader, providerLoader}) {
      if (loader == null) {
        continue;
      }
      Optional<NativeFormatProvider> provider = findIn(loader, identifier, accepts, seen);
      if (provider.isPresent()) {
        return provider;
      }
    }
    return Optional.empty();
  }

  private static Optional<NativeFormatProvider> findIn(
      ClassLoader loader,
      String identifier,
      Predicate<NativeFormatProvider> accepts,
      Set<String> seen) {
    Iterator<NativeFormatProvider> providers =
        ServiceLoader.load(NativeFormatProvider.class, loader).iterator();
    while (true) {
      final NativeFormatProvider provider;
      try {
        if (!providers.hasNext()) {
          return Optional.empty();
        }
        provider = providers.next();
      } catch (ServiceConfigurationError | LinkageError brokenProvider) {
        LOG.warn("Ignoring an unusable optional StreamFusion format provider", brokenProvider);
        continue;
      }
      try {
        if (!seen.add(provider.getClass().getName())) {
          continue;
        }
        if (identifier.equals(provider.formatIdentifier()) && accepts.test(provider)) {
          return Optional.of(provider);
        }
      } catch (RuntimeException | LinkageError brokenProvider) {
        LOG.warn(
            "Ignoring unusable optional StreamFusion format provider {}",
            provider.getClass().getName(),
            brokenProvider);
      }
    }
  }
}
