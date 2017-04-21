package com.forgenano.datastream.model;

import com.forgenano.datastream.status.StatusMaintainer;
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

        public long lastConsumedOffset;

        public RunStatus() {
            this.lastConsumedOffset = 0;
        }
    }


    private Map<String, RunStatus> runningStatuses;
    private Map<String, RunStatus> finishedStatuses;

    public StatusModel() {
        this.runningStatuses = Maps.newHashMap();
        this.finishedStatuses = Maps.newHashMap();
    }

    public synchronized void addNewRunningStatus(Path arbinDbPath) {
        this.runningStatuses.put(pathToKey(arbinDbPath), new RunStatus());
    }

    public synchronized void updateLastConsumedOffset(Path arbinDbPath, long lastConsumedOffset) {
        String key = pathToKey(arbinDbPath);

        if (this.runningStatuses.containsKey(key)) {
            this.runningStatuses.get(key).lastConsumedOffset = lastConsumedOffset;
        }
        else if (this.finishedStatuses.containsKey(key)) {
            this.finishedStatuses.get(key).lastConsumedOffset = lastConsumedOffset;
        }
        else {
            log.warn("Unknown arbinDbPath, can't update last consumed offset for: " + key);
        }
    }

    public synchronized void finishedConsuming(Path arbinDbPath, long lastConsumedOffset) {
        String key = pathToKey(arbinDbPath);
        RunStatus runStatus = this.runningStatuses.remove(key);

        if (runStatus != null) {
            this.finishedStatuses.put(key, runStatus);
        }
        else {
            log.error("Can't set the arbinDbPath to finished because its not in the run list: " + key);
        }
    }

    public synchronized boolean hasPathBeenConsumed(Path arbinDbPath) {
        String key = pathToKey(arbinDbPath);

        return this.runningStatuses.containsKey(key) || this.finishedStatuses.containsKey(key);
    }

    private String pathToKey(Path arbinDbPath) {
        return arbinDbPath.toAbsolutePath().toString();
    }
}
