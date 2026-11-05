package tech.streamfusion.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.paimon.flink.sink.StreamFusionStoreSinkWrite;
import org.junit.jupiter.api.Test;

class PaimonCompactionClassLoaderTest {
  @Test
  void splitRuntimePackagesDeclineThePackagePrivateConstructor() throws Exception {
    var name = StreamFusionStoreSinkWrite.class.getName();
    var resource = "/" + name.replace('.', '/') + ".class";
    byte[] bytes;
    try (var input = getClass().getResourceAsStream(resource)) {
      assertNotNull(input);
      bytes = input.readAllBytes();
    }
    var loader =
        new ClassLoader(getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String target, boolean resolve)
              throws ClassNotFoundException {
            if (!target.equals(name)) return super.loadClass(target, resolve);
            var type = findLoadedClass(target);
            if (type == null) type = defineClass(target, bytes, 0, bytes.length);
            if (resolve) resolveClass(type);
            return type;
          }
        };
    assertTrue(StreamFusionStoreSinkWrite.available());
    assertEquals(false, loader.loadClass(name).getMethod("available").invoke(null));
  }
}
