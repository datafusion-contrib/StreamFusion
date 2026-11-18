package tech.streamfusion;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.params.provider.ArgumentsSource;

/** Selects valid canonical configurations after the complete provider has been assembled. */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@ArgumentsSource(CanonicalCaseProvider.class)
@interface CanonicalCases {
  String[] value();

  /** Mode, engine direction and backend/naming axes whose three-way interactions are required. */
  int[] recoveryAxes() default {};
}
