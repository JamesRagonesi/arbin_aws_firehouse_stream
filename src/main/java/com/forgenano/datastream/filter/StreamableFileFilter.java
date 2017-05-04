package com.forgenano.datastream.filter;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Created by michael on 4/19/17.
 */
public class StreamableFileFilter implements DirectoryStream.Filter<Path> {

    private static final Logger log = LoggerFactory.getLogger(StreamableFileFilter.class);

    private static List<String> allowed_extensions = Lists.newArrayList("res");

    public static boolean IsFileStreamable(Path questionableFile) {
        if (Files.isRegularFile(questionableFile)) {
            String extension = getFileExtension(questionableFile);

            if (allowed_extensions.contains(extension)) {
                return true;
            }
        }

        log.info("Ignoring file: " + questionableFile.toAbsolutePath().toString());

        return false;
    }

    private static String getFileExtension(Path file) {
        String fileName = file.getFileName().toString();

        if(fileName.lastIndexOf(".") != -1 && fileName.lastIndexOf(".") != 0)
            return fileName.substring(fileName.lastIndexOf(".")+1);
        else return "";
    }

    @Override
    public boolean accept(Path entry) throws IOException {
        return IsFileStreamable(entry);
    }
}
