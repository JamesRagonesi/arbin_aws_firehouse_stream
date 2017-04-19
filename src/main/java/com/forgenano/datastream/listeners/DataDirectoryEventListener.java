package com.forgenano.datastream.listeners;

import java.nio.file.Path;

/**
 * Created by michael on 4/18/17.
 */
public interface DataDirectoryEventListener {

    public void newFileInDataDirectory(Path newDataFilePath);
}
