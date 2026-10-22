package tech.streamfusion.doris;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URLClassLoader;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import tech.streamfusion.planner.DorisPlannerExtension;

class DorisDeploymentTest {
  @Test
  void absentStockConnectorLeavesThePlannerUsable() throws Exception {
    String extension = DorisPlannerExtension.class.getName();
    var location = DorisPlannerExtension.class.getProtectionDomain().getCodeSource().getLocation();
    try (var isolated =
        new URLClassLoader(new java.net.URL[] {location}, getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (name.startsWith("org.apache.doris.")) throw new ClassNotFoundException(name);
            if (name.equals(extension)) {
              Class<?> loaded = findLoadedClass(name);
              if (loaded == null) loaded = findClass(name);
              if (resolve) resolveClass(loaded);
              return loaded;
            }
            return super.loadClass(name, resolve);
          }
        }) {
      var provider = isolated.loadClass(extension).getConstructor().newInstance();
      var entries = new ArrayList<>();
      provider
          .getClass()
          .getMethod("addSubstitutions", java.util.List.class)
          .invoke(provider, entries);
      assertTrue(entries.isEmpty());
    }
  }
}
