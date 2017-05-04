package com.forgenano.datastream;

import com.forgenano.datastream.arbin.ArbinDbStreamer;
import com.forgenano.datastream.arbin.ArbinEventFirehoseConsumer;
import com.forgenano.datastream.aws.ArbinDataFirehoseClient;
import com.forgenano.datastream.config.Configuration;
import com.forgenano.datastream.filter.StreamableFileFilter;
import com.forgenano.datastream.listeners.DataDirectoryEventListener;
import com.forgenano.datastream.model.StreamDataFileRunnable;
import com.forgenano.datastream.status.StatusMaintainer;
import com.forgenano.datastream.util.SingleApplicationInstanceUtil;
import com.forgenano.datastream.watcher.DataDirectoryWatcher;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.log4j.FileAppender;
import org.apache.log4j.PatternLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.InputStream;
import java.nio.file.*;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by michael on 4/7/17.
 */
public class DataStreamer implements DataDirectoryEventListener {

    private static Logger log = LoggerFactory.getLogger(DataStreamer.class);

    private static final int NUM_TASK_WORKER_THREADS = 4;

    public static Configuration configuration;
    public static DataStreamer instance;

    private ArbinDataFirehoseClient firehoseClient;
    private ArbinEventFirehoseConsumer firehoseArbinEventConsumer;
    private Map<Path, ArbinDbStreamer> dbStreamers;
    private ExecutorService taskExecutorService;
    private StatusMaintainer statusMaintainer;
    private DataDirectoryWatcher dataDirectoryWatcher;
    private ConcurrentLinkedQueue<Path> arbinFilesToConsume;

    private DataStreamer(Configuration configuration) {
        this.arbinFilesToConsume = Queues.newConcurrentLinkedQueue();
        this.dbStreamers = Maps.newHashMap();
        this.statusMaintainer = StatusMaintainer.getSingleton();
        this.firehoseClient = ArbinDataFirehoseClient.BuildKinesisFirehoseClient(
                        configuration.getAwsRegionName(), configuration.getFirehoseStreamName());

        this.firehoseArbinEventConsumer = new ArbinEventFirehoseConsumer(this.firehoseClient);

        this.taskExecutorService = Executors.newFixedThreadPool(NUM_TASK_WORKER_THREADS);
    }

    public void shutdown() {
        this.taskExecutorService.shutdown();
        this.dataDirectoryWatcher.shutdown();
        this.statusMaintainer.saveStatusFileBlocking();
        this.statusMaintainer.shutdown();
    }

    public void startWatchingDirectoryAndConsumingDataFiles(Path directoryToWatchPath) {
        log.info("Looking for new data files that haven't been consumed.");

        try {
            Files.newDirectoryStream(directoryToWatchPath, new StreamableFileFilter()).forEach((arbinDbFile) -> {
                log.info("Scheduling data file for consumption and streaming: " +
                        arbinDbFile.toAbsolutePath().toString());

                this.taskExecutorService.submit(new StreamDataFileRunnable(this,
                        arbinDbFile.toAbsolutePath(), false));
            });
        }
        catch(Exception e) {
            log.error("Failed to create a directory stream for: " +
                    directoryToWatchPath.toAbsolutePath().toString(), e);
        }

        this.dataDirectoryWatcher = DataDirectoryWatcher.InitializeSingleton(directoryToWatchPath);

        this.dataDirectoryWatcher.addDirectoryEventListener(DataStreamer.instance);

        this.dataDirectoryWatcher.start();

        this.dataDirectoryWatcher.waitForShutdown();
    }

    @Override
    public void handleDataFileEvent(Path dataFilePath, WatchEvent.Kind<Path> eventKind) {
        if (!StreamableFileFilter.IsFileStreamable(dataFilePath)) {
            log.warn("Ignoring file: " + dataFilePath.toAbsolutePath().toString());

            return;
        }

        if (eventKind == StandardWatchEventKinds.ENTRY_CREATE) {
            log.info("Consuming available data from new arbin db: " + dataFilePath.toAbsolutePath().toString());

            this.taskExecutorService.submit(new StreamDataFileRunnable(this, dataFilePath, true));
        }
        else if (eventKind == StandardWatchEventKinds.ENTRY_MODIFY) {
            log.info("Consuming available data from modified file: " + dataFilePath.toAbsolutePath().toString());

            this.taskExecutorService.submit(new StreamDataFileRunnable(this, dataFilePath, true));
        }
    }

    public void startStreamingDataFromNewDataFile(Path newDataFile) {
        ArbinDbStreamer dbStreamer = ArbinDbStreamer.CreateArbinDbStreamer(newDataFile);

        this.dbStreamers.put(newDataFile.toAbsolutePath(), dbStreamer);

        dbStreamer.addConsumer(this.firehoseArbinEventConsumer);

        try {
            dbStreamer.startLiveMonitoring();
        }
        catch (Exception e) {
            log.error("Caught an exception while starting live monitoring of: " +
                    newDataFile.toAbsolutePath().toString(), e);

            this.dbStreamers.remove(newDataFile.toAbsolutePath());
        }
    }

    public void startStreamingNewDataFromExistingDataFile(Path existingDataFile) {
        if (this.dbStreamers.containsKey(existingDataFile.toAbsolutePath())) {
            ArbinDbStreamer dbStreamer = this.dbStreamers.get(existingDataFile.toAbsolutePath());

            log.info("Notifying arbin db streamer of data file modification.");

            dbStreamer.dataFileWasModified();
        }
        else {
            log.info("Data file was modified, but there isn't an active db streamer for it, creating a new one.");

            ArbinDbStreamer dbStreamer = ArbinDbStreamer.CreateArbinDbStreamer(existingDataFile);

            this.dbStreamers.put(existingDataFile.toAbsolutePath(), dbStreamer);

            dbStreamer.addConsumer(this.firehoseArbinEventConsumer);

            try {
                dbStreamer.startLiveMonitoring();
            }
            catch (Exception e) {
                log.error("Caught an exception while starting live monitoring of: " +
                        existingDataFile.toAbsolutePath().toString(), e);

                this.dbStreamers.remove(existingDataFile.toAbsolutePath());
            }
        }
    }

    private void blockAndConsumeArbinFile(Path arbinFilePath, boolean dumpMetadataOnly) {
        if (Files.exists(arbinFilePath) && Files.isReadable(arbinFilePath)) {
            log.info("Reading all arbin data events from: " + arbinFilePath.toAbsolutePath().toString());
        }
        else {
            log.error("The supplied file: " + arbinFilePath.toAbsolutePath().toString() +
                    " doesn't exist or isn't readable.");
            System.exit(1);
        }


        ArbinDbStreamer dbStreamer = null;
        try {
            dbStreamer = ArbinDbStreamer.CreateArbinDbStreamer(arbinFilePath);
        }
        catch(Exception e) {
            log.error("Failed to create an arbin db streamer object: " + e.getMessage());
            System.exit(1);
        }

        if (!dumpMetadataOnly) {
            log.info("Consuming arbin data file: " + arbinFilePath.toAbsolutePath().toString());


            dbStreamer.addConsumer(this.firehoseArbinEventConsumer);

            try {
                dbStreamer.blockAndStreamFinishedArbinDatabase();
            }
            catch(Exception e) {
                log.error("Failed to block and stream arbin db file: ", e);
            }
        }
        else {
            log.info("Dumping arbin data file metadata for: " + arbinFilePath.toAbsolutePath().toString());

            dbStreamer.dumpArbinDbDetails();
        }

        dbStreamer.shutdown();
    }

    public static void main(String[] args) {
        try {
            SingleApplicationInstanceUtil.StartMultipleApplicationBlock();
        }
        catch(Exception e) {
            log.error(e.getMessage());
            System.out.println(e.getMessage());
            System.exit(1);
        }

        System.out.println("DataStreamer Starting...");

        OptionSet runOptions = parseRunArgs(args);

        DataStreamer.configuration = setupConfiguration(runOptions);

        addFileLogAppender();

        instance = new DataStreamer(configuration);

        log.info("DataStreamer Started.");

        if (runOptions.has("f") && runOptions.hasArgument("f") && !runOptions.has("d")) {
            Path dataFilePath = Paths.get((String) runOptions.valueOf("f"));

            log.info("Consuming available data from arbin db: " + dataFilePath.toAbsolutePath().toString());

            instance.blockAndConsumeArbinFile(dataFilePath, false);
        }
        else if (runOptions.has("f") && runOptions.hasArgument("f") && runOptions.has("d")) {
            Path dataFilePath = Paths.get((String) runOptions.valueOf("f"));

            log.info("Dumping metadata for arbin db: " + dataFilePath.toAbsolutePath().toAbsolutePath());
            instance.blockAndConsumeArbinFile(dataFilePath, true);
        }
        else if (runOptions.has("w")) {
            Path watchPath = Paths.get(configuration.getDirectoryToWatch());

            log.info("Watching directory and consuming new arbin files as well as new to this run: " +
                    watchPath.toAbsolutePath().toString());


            instance.startWatchingDirectoryAndConsumingDataFiles(watchPath);
        }

        log.info("Shutting down services...");

        instance.shutdown();


        log.info("Finished Running Data Streamer.");
    }



    private static OptionSet parseRunArgs(String[] args) {
        try {
            OptionParser optionParser = new OptionParser("c:f:Lwd");

            return optionParser.parse(args);
        }
        catch(Exception e) {
            log.error("Failed to parse the command line args: " + e.getMessage(), e);
            System.exit(1);
        }

        return null;
    }

    private static Configuration setupConfiguration(OptionSet runOptions) {
        boolean writeConfigurationFile = false;
        InputStream configInputStream = null;

        if (Files.exists(Paths.get(Configuration.DefaultConfigDirLocation)) &&
                Files.isReadable(Paths.get(Configuration.DefaultConfigDirLocation))) {
            try {
                configInputStream = Files.newInputStream(Paths.get(Configuration.DefaultConfigDirLocation));
                log.info("Using configuration file located at: " +
                        Paths.get(Configuration.DefaultConfigDirLocation).toAbsolutePath().toString());
            }
            catch(Exception e) {
                log.error("Failed to open the default config location: " + Configuration.DefaultConfigDirLocation, e);
                System.exit(1);
            }
        }
        else if (!runOptions.has("c")) {
            try {
                configInputStream = DataStreamer.class.getResourceAsStream(Configuration.DefaultConfigPackageLocation);
                log.info("Using the default application configuration, and writing it to: " +
                        Paths.get(Configuration.DefaultConfigDirLocation).toAbsolutePath().toString());

                writeConfigurationFile = true;
            }
            catch(Exception e) {
                log.error("Failed to get the default config: ", e);
                System.exit(1);
            }
        }
        else {
            try {
                String configLocation = (String) runOptions.valueOf("c");
                Path configPath = Paths.get(configLocation);

                if (Files.exists(configPath) && Files.isReadable(configPath)) {
                    configInputStream = Files.newInputStream(configPath);
                    log.info("Using configuration file: " + configPath.toAbsolutePath().toString());
                }
                else {
                    log.error("Configuration file does not exist or isn't readable: " +
                            configPath.toAbsolutePath().toString());
                }
            }
            catch (Exception e) {
                log.error("Failed to open specified configuration file: " + runOptions.valueOf("c"));
            }
        }

        Configuration config = Configuration.getSingleton();

        config.setModelFromInputStream(configInputStream);

        if (writeConfigurationFile) {
            writeConfigurationFileToDefaultLocation(config);
        }

        return config;
    }

    private static void writeConfigurationFileToDefaultLocation(Configuration configuration) {
        try {
            Path defaultConfigPath = Paths.get(Configuration.DefaultConfigDirLocation);

            log.info("Writing the configuration file to: " + defaultConfigPath.toAbsolutePath().toString());

            Files.write(defaultConfigPath, configuration.getModelAsJsonBytes(), StandardOpenOption.CREATE);
        }
        catch(Exception e) {
            System.out.println("Failed to write the config file to the default location.");
            log.warn("Failed to write the config file to the default path.");
        }
    }

    private static void addFileLogAppender() {
        System.out.println("Attempting to use log location: " + DataStreamer.configuration.getLogFileString());
        try {
            Path logFilePath = Paths.get(DataStreamer.configuration.getLogFileString());
            FileAppender fileAppender = new FileAppender(
                    new PatternLayout("%d %-5p [%c{1}] %m%n"), logFilePath.toAbsolutePath().toString(), true);

            fileAppender.setThreshold(org.apache.log4j.Level.ALL);
            org.apache.log4j.Logger.getRootLogger().addAppender(fileAppender);
        }
        catch(Exception e) {
            System.out.println("Failed to setup the log file at: " + DataStreamer.configuration.getLogFileString() +
                    " becuase: " + e.getMessage());
            log.error("Failed to setup the log file at: " + DataStreamer.configuration.getLogFileString() +
                    " becuase: " + e.getMessage(), e);

            System.exit(1);
        }
    }
}
