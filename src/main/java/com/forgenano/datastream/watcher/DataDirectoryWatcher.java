package com.forgenano.datastream.watcher;

import com.forgenano.datastream.listeners.DataDirectoryEventListener;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by michael on 4/18/17.
 */
public class DataDirectoryWatcher {

    private static final Logger log = LoggerFactory.getLogger(DataDirectoryWatcher.class);

    private static DataDirectoryWatcher instance;

    public static DataDirectoryWatcher getSingleton() {
        if (instance == null) {
            log.warn("The data directory watcher has not been initialized yet.");
        }

        return instance;
    }

    public static DataDirectoryWatcher InitializeSingleton(Path directoryToWatch) {
        if (!Files.exists(directoryToWatch) || !Files.isDirectory(directoryToWatch) ||
                !Files.isReadable(directoryToWatch)) {
            log.error("The data directory specified to watch doesn't exist, isn't a directory, or isn't readable: " +
                      directoryToWatch.toAbsolutePath().toString());

            throw new IllegalArgumentException("The data directy specified isn't accessible.");
        }

        instance = new DataDirectoryWatcher(directoryToWatch);

        return instance;
    }

    private Path directoryToWatch;
    private WatchKey watchingDirectoryKey;
    private WatchService watchService;
    private ExecutorService fsEventMonitorService;
    private AtomicBoolean shutdown;
    private List<DataDirectoryEventListener> eventListeners;

    private DataDirectoryWatcher(Path directoryToWatch) {
        this.directoryToWatch = directoryToWatch;
        this.shutdown = new AtomicBoolean(true);
        this.eventListeners = Lists.newArrayList();

        try {
            this.watchService = FileSystems.getDefault().newWatchService();
        }
        catch(Exception e) {
            log.error("Failed to setup a new watch service for the DataDirectoryWatcher.");
            throw new IllegalStateException("The watch service was unable to be created.", e);
        }

        this.fsEventMonitorService = Executors.newSingleThreadExecutor();
    }

    public void start() {
        if (this.shutdown.get()) {
            this.shutdown.set(false);

            this.fsEventMonitorService.submit(fsEventHandlerRunnable);
        }
    }

    public void stop() {
        if (!this.shutdown.get()) {
            this.shutdown.set(true);

            log.info("Waiting for data directory event monitor to stop monitoring.");
        }
    }

    public void shutdown() {
        this.shutdown.set(true);

        this.fsEventMonitorService.shutdown();
    }

    public void addDirectoryEventListener(DataDirectoryEventListener listener) {
        if (!this.eventListeners.contains(listener)) {
            this.eventListeners.add(listener);
        }
    }

    private Runnable fsEventHandlerRunnable = () -> {
        log.info("Watching data directory for changes: " + this.directoryToWatch.toAbsolutePath().toString());

        try {
            this.watchingDirectoryKey =
                    this.directoryToWatch.register(this.watchService, StandardWatchEventKinds.ENTRY_CREATE);
        }
        catch(Exception e) {
            log.error("Couldn't register the data directory to be watched.");
            throw new IllegalStateException(
                    "The data directory to be watched couldn't be registerd with the watch service.", e);
        }

        while(!this.shutdown.get()) {

            try {
                WatchKey watchKey = this.watchService.poll(1, TimeUnit.SECONDS);

                if (watchKey != null) {
                    List<WatchEvent<?>> dirEvents = watchKey.pollEvents();

                    dirEvents.forEach((dirEvent) -> {
                        WatchEvent.Kind<?> eventKind = dirEvent.kind();

                        if (eventKind == StandardWatchEventKinds.OVERFLOW) {
                            log.warn("Overflow watch event for data directory: " +
                                    this.directoryToWatch.toAbsolutePath().toString());
                        }
                        else if (dirEvent.context() instanceof Path) {
                            Path newFilePath = this.directoryToWatch.resolve((Path) dirEvent.context());

                            if (!Files.isDirectory(newFilePath) && !Files.isRegularFile(newFilePath)) {
                                log.info("New file found in data directory: " + newFilePath.toAbsolutePath().toString());

                                notifyListenersOfNewFile(newFilePath);
                            }
                            else if (Files.isDirectory(newFilePath)) {
                                log.info("New directory found in data directory: " +
                                        newFilePath.toAbsolutePath().toString() + " registering for events.");

                                try {
                                    newFilePath.register(this.watchService, StandardWatchEventKinds.ENTRY_CREATE);
                                }
                                catch(Exception e) {
                                    log.error("Failed to register for events on the child directory: " +
                                            newFilePath.toAbsolutePath().toString());
                                }
                            }
                            else {
                                log.warn("Ignoring new unknown file type in data directory: " +
                                        newFilePath.toAbsolutePath().toString());
                            }
                        } else {
                            log.warn("Unknown context object for directory watcher event: " + dirEvent.toString());
                        }
                    });

                    if (!watchKey.reset()) {
                        log.error("Failed to reset the watch key for the data directory, exiting the watch loop.");
                        throw new IllegalStateException("Failed to reset watch key for the data directory.");
                    }
                }
            }
            catch(InterruptedException ie) {
                log.warn("Got interrupted while waiting for an fs event.");
            }
            catch(Exception e) {
                log.error("Failed to successfully poll the watch service, bailing out.", e);
                throw new IllegalStateException("Unknown state after failing to poll the watch service.", e);
            }
        }

        log.info("Stopped watching data directory for changes.");
    };

    private void notifyListenersOfNewFile(Path newFile) {
        this.eventListeners.forEach((listener) -> {
            listener.newFileInDataDirectory(newFile);
        });
    }
}
