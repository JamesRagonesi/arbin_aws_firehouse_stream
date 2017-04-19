package com.forgenano.datastream.model;

import com.forgenano.datastream.DataStreamer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

/**
 * Created by michael on 4/18/17.
 */
public class StreamDataFileRunnable implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(StreamDataFileRunnable.class);

    private Path newDataFileToStream;
    private DataStreamer dataStreamer;

    public StreamDataFileRunnable(DataStreamer dataStreamer, Path newDataFileToStream) {
        this.dataStreamer = dataStreamer;
        this.newDataFileToStream = newDataFileToStream;
    }

    @Override
    public void run() {
        this.dataStreamer.startStreamingDataFromNewDataFile(this.newDataFileToStream);
    }
}
