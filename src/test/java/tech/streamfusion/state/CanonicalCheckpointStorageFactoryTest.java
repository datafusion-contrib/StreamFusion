package tech.streamfusion.state;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.util.Base64;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

class CanonicalCheckpointStorageFactoryTest {
  @Test
  void plannerProjectionUsesItsDefiningLoaderAcrossTheUserLoaderBoundary() throws Exception {
    var config = new Configuration();
    config.setString(CanonicalCheckpointStorageFactory.ACTIVE, "true");
    var projection = new OperatorIdentityProjection(Map.of());
    config.setString(
        CanonicalCheckpointStorageFactory.IDENTITIES,
        Base64.getEncoder().encodeToString(InstantiationUtil.serializeObject(projection)));
    ClassLoader userLoader =
        new ClassLoader(getClass().getClassLoader()) {
          @Override
          protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.equals(OperatorIdentityProjection.class.getName()))
              return super.loadClass(name, resolve);
            var loaded = findLoadedClass(name);
            if (loaded == null) {
              try (var input = getResourceAsStream(name.replace('.', '/') + ".class")) {
                if (input == null) throw new ClassNotFoundException(name);
                byte[] bytes = input.readAllBytes();
                loaded = defineClass(name, bytes, 0, bytes.length);
              } catch (IOException failure) {
                throw new ClassNotFoundException(name, failure);
              }
            }
            if (resolve) resolveClass(loaded);
            return loaded;
          }
        };
    assertNotSame(
        OperatorIdentityProjection.class, userLoader.loadClass(projection.getClass().getName()));
    var provider = new JobManagerCheckpointStorage();
    var projected = CanonicalCheckpointStorageFactory.project(provider, config, userLoader);
    assertInstanceOf(CanonicalCheckpointStorage.class, projected);
    assertSame(provider, CanonicalCheckpointStorage.unwrap(projected));
  }
}
