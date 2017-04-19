package com.forgenano.datastream.config;

import com.forgenano.datastream.model.ConfigModel;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Created by michael on 4/18/17.
 */
public class Configuration {

    private static final Logger log = LoggerFactory.getLogger(Configuration.class);

    public static String DefaultConfigPackageLocation = "/default-config.json";
    public static String DefaultConfigDirLocation = "./config.json";

    private static Configuration instance;

    public static Configuration getSingleton() {
        if (instance == null)
            instance = new Configuration();

        return instance;
    }

    private ConfigModel model;

    public void setModelFromInputStream(InputStream configInputStream) {
        if (model != null) {
            log.error("Can't set the configuration model twice, its already been set once.");
            return;
        }
        try {
            Reader inputStreamReader = new InputStreamReader(configInputStream, ConfigModel.CONFIG_ENCODING);
            this.model = new Gson().fromJson(inputStreamReader, ConfigModel.class);
        }
        catch (Exception e) {
            log.error("Failed to convert the configuration input stream to a config model: ", e);
            throw new IllegalStateException("Configuration input stream is not able to be read / converted.");
        }
    }

    public String getAwsRegionName() {
        return this.model.awsRegionName;
    }

    public String getFirehoseStreamName() {
        return this.model.firehoseStreamName;
    }

    public String getDirectoryToWatch() {
        return this.model.directoryToWatch;
    }

    public String getLogFileString() {
        return this.model.logFile;
    }
}
