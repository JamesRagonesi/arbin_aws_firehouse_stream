package com.forgenano.datastream.model;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Map;

/**
 * Created by michael on 4/20/17.
 */
public class StatusModel {

    private static final Logger log = LoggerFactory.getLogger(StatusModel.class);

    public static final String ENCODING_NAME = "UTF-8";

    public class RunStatus {

        public long lastConsumedChannelOffset;

        public RunStatus() {
            this.lastConsumedChannelOffset = 0;
        }
    }

    private Map<String, RunStatus> arbinDataFileStatuses;

    public StatusModel() {
        this.arbinDataFileStatuses = Maps.newHashMap();
    }

    public synchronized void addNewArbinFileStatus(Path arbinDbPath) {
        String key = pathToKey(arbinDbPath);

        if (!this.arbinDataFileStatuses.containsKey(key)) {
            this.arbinDataFileStatuses.put(key, new RunStatus());
        }
    }

    public synchronized void updateLastConsumedChannelOffset(Path arbinDbPath, long lastConsumedChannelOffset) {
        String key = pathToKey(arbinDbPath);

        if (this.arbinDataFileStatuses.containsKey(key)) {
            this.arbinDataFileStatuses.get(key).lastConsumedChannelOffset = lastConsumedChannelOffset;
        }
        else {
            log.warn("Unknown arbinDbPath, can't update last consumed offset for: " + key);
        }
    }

    public synchronized long getLastConsumedChannelOffset(Path arbinDbPath) throws IllegalArgumentException {
        String key = pathToKey(arbinDbPath);

        if (this.arbinDataFileStatuses.containsKey(key)) {
            return this.arbinDataFileStatuses.get(key).lastConsumedChannelOffset;
        }

        throw new IllegalArgumentException("Supplied ArbinDbPath isn't being monitored for status changes yet.");
    }

    public synchronized boolean hasStatusForArbinDbPath(Path arbinDbPath) {
        String key = pathToKey(arbinDbPath);

        return this.arbinDataFileStatuses.containsKey(key);
    }

    private String pathToKey(Path arbinDbPath) {
        return arbinDbPath.toAbsolutePath().toString();
    }
}
