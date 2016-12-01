package infinispantest;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static class CacheSource {
        private final EmbeddedCacheManager cacheManager;

        public CacheSource() throws IOException {
            cacheManager = new DefaultCacheManager("cache.xml");
        }

        private Configuration defaultConfiguration = new ConfigurationBuilder()
                .jmxStatistics()
                    .enabled(false)
                    .available(false)
                .transaction()
                    .transactionManagerLookup(new GenericTransactionManagerLookup())
                    .transactionMode(TransactionMode.TRANSACTIONAL)
                    .useSynchronization(false)
                    .lockingMode(LockingMode.PESSIMISTIC)
                .recovery()
                    .enabled(false)
                    .invocationBatching()
                    .enable(false)
                .indexing()
                    .index(Index.LOCAL)
                    .addProperty("default.directory_provider", "ram")
                    .addProperty("default.worker.execution", "async")
                    .addProperty("default.indexmanager", "near-real-time")
                .build();

        public <K, V> Cache<K, V> getCache(final String name) {
            return getCache(name, defaultConfiguration);
        }

        public synchronized <K, V> Cache<K, V> getCache(final String name, final Configuration configuration) {
            cacheManager.defineConfiguration(name, configuration);
            return cacheManager.getCache(name);
        }
    }

    private static class Mutable {
        private String string;

        public String getString() {
            return string;
        }

        public void setStrings(final String string) {
            this.string = string;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final CacheSource cacheSource = new CacheSource();
        final Cache<Long, Mutable> cache = cacheSource.getCache("cache");
        final long key = 1L;
        final Mutable object = new Mutable();
        object.setStrings("123");
        cache.put(key, object);
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final Runnable runnable1 = () -> {
            try {
                final Mutable mutable = cache.get(key);
                if (mutable.getString() != null) {
                    try {
                        Thread.currentThread().sleep(3000);
                    } catch (InterruptedException e) {
                        log.error("Interrupted in runnable2", e);
                        return;
                    }
                    log.debug(mutable.getString().toLowerCase());
                } else {
                    log.debug("String is null");
                }
            } catch (Throwable e) {
                log.error("error in runnable1", e);
            }
        };
        final Runnable runnable2 = () -> {
            try {
                final Mutable mutable = cache.get(key);
                try {
                    Thread.currentThread().sleep(1000);
                } catch (InterruptedException e) {
                    log.error("Interrupted in runnable2", e);
                    return;
                }
                log.debug("setting string of object {} to null", System.identityHashCode(mutable));
                mutable.setStrings(null);
            } catch (Throwable e) {
                log.error("error in runnable2", e);
            }
        };
        executorService.submit(runnable1);
        executorService.submit(runnable2);
        TimeUnit.SECONDS.sleep(5);
        executorService.shutdown();
    }
}
