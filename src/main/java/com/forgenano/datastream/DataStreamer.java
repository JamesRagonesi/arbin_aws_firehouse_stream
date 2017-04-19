package com.forgenano.datastream;

import com.forgenano.datastream.arbin.ArbinDbStreamer;
import com.forgenano.datastream.arbin.ArbinEventFirehoseConsumer;
import com.forgenano.datastream.aws.ArbinDataFirehoseClient;
import com.forgenano.datastream.config.Configuration;
import com.forgenano.datastream.listeners.DataDirectoryEventListener;
import com.forgenano.datastream.watcher.DataDirectoryWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by michael on 4/7/17.
 */
public class DataStreamer {

    private static Logger log = LoggerFactory.getLogger(DataStreamer.class);

    public static Configuration configuration;

    public static void main(String[] args) {

        OptionSet runOptions = parseRunArgs(args);

        DataStreamer.configuration = setupConfiguration(runOptions);

        if (runOptions.has("f") && runOptions.hasArgument("f") && !runOptions.has("d")) {
            Path dataFilePath = Paths.get((String) runOptions.valueOf("f"));

            consumeArbinFile(dataFilePath, false);
        }
        else if (runOptions.has("f") && runOptions.hasArgument("f") && runOptions.has("d")) {
            Path dataFilePath = Paths.get((String) runOptions.valueOf("f"));

            consumeArbinFile(dataFilePath, true);
        }
        else if (runOptions.has("w")) {
            Path watchPath = Paths.get(configuration.getDirectoryToWatch());

            DataDirectoryWatcher directoryWatcher = DataDirectoryWatcher.InitializeSingleton(watchPath);

            directoryWatcher.addDirectoryEventListener(new DataDirectoryEventListener() {
                @Override
                public void newFileInDataDirectory(Path newDataFilePath) {
                    log.info("Here is a new data file: " + newDataFilePath.toAbsolutePath().toString());
                }
            });

            directoryWatcher.start();
        }

        log.info("Finished Running Data Streamer.");
    }

    private static void consumeArbinFile(Path arbinFilePath, boolean dumpMetadataOnly) {
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

            ArbinDataFirehoseClient arbinDataFirehoseClient =
                    ArbinDataFirehoseClient.BuildKinesisFirehoseClient(
                            configuration.getAwsRegionName(), configuration.getFirehoseStreamName());

            ArbinEventFirehoseConsumer firehoseConsumer = new ArbinEventFirehoseConsumer(arbinDataFirehoseClient);
            dbStreamer.addConsumer(firehoseConsumer);
            dbStreamer.blockAndStreamFinishedArbinDatabase();
        }
        else {
            log.info("Dumping arbin data file metadata for: " + arbinFilePath.toAbsolutePath().toString());

            dbStreamer.dumpArbinDbDetails();
        }


        dbStreamer.shutdown();
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

        return config;
    }
}
