package tech.streamfusion.planner;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Activates optional providers only after all of their planner rules have registered. */
public final class NativePlannerExtensions {
  private static final Logger LOG = LoggerFactory.getLogger(NativePlannerExtensions.class);

  private final List<NativePlannerExtension> extensions;
  private final List<Substitution<?>> substitutions;

  private NativePlannerExtensions(
      List<NativePlannerExtension> extensions, List<Substitution<?>> substitutions) {
    this.extensions = List.copyOf(extensions);
    this.substitutions = List.copyOf(substitutions);
  }

  /** Providers whose complete rule contribution passed optional-module validation. */
  public static List<NativePlannerExtension> batchFileSourceProviders(ClassLoader loader) {
    return load(loader).extensions();
  }

  static NativePlannerExtensions load(ClassLoader loader) {
    return load(ServiceLoader.load(NativePlannerExtension.class, loader).iterator());
  }

  static NativePlannerExtensions load(Iterator<NativePlannerExtension> providers) {
    List<NativePlannerExtension> extensions = new ArrayList<>();
    List<Substitution<?>> substitutions = new ArrayList<>();
    while (true) {
      NativePlannerExtension extension;
      try {
        if (!providers.hasNext()) {
          break;
        }
        extension = providers.next();
      } catch (ServiceConfigurationError | LinkageError failure) {
        rethrowUnexpectedCause(failure);
        LOG.warn("Ignoring a broken StreamFusion planner extension during discovery", failure);
        continue;
      }
      List<Substitution<?>> contribution;
      try {
        List<Substitution<?>> pending = new ArrayList<>();
        extension.addSubstitutions(pending);
        contribution = List.copyOf(pending);
      } catch (ServiceConfigurationError | LinkageError failure) {
        rethrowUnexpectedCause(failure);
        LOG.warn(
            "Ignoring StreamFusion planner extension {}: registration failed",
            extension.getClass().getName(),
            failure);
        continue;
      }
      substitutions.addAll(contribution);
      extensions.add(extension);
    }
    return new NativePlannerExtensions(extensions, substitutions);
  }

  private static void rethrowUnexpectedCause(Throwable failure) {
    // ServiceLoader wraps provider constructor failures, including fatal JVM errors.
    for (Throwable cause = failure.getCause(); cause != null; cause = cause.getCause()) {
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error
          && !(cause instanceof LinkageError)
          && !(cause instanceof ServiceConfigurationError)) {
        throw (Error) cause;
      }
    }
  }

  List<NativePlannerExtension> extensions() {
    return extensions;
  }

  List<Substitution<?>> substitutions() {
    return substitutions;
  }
}
