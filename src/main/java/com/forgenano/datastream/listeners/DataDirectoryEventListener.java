package com.forgenano.datastream.listeners;

import java.nio.file.Path;
import java.nio.file.WatchEvent;

/**
 * Created by michael on 4/18/17.
 */
public interface DataDirectoryEventListener {

    public void handleDataFileEvent(Path newDataFilePath, WatchEvent.Kind<Path> eventKind);
}
