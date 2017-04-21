package com.forgenano.datastream.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by michael on 4/20/17.
 */
public class SingleApplicationInstanceUtil {

    private static final Logger log = LoggerFactory.getLogger(SingleApplicationInstanceUtil.class);

    private static int port = 64582;
    private static ServerSocket multiAppBlockSocket;

    public static void StartMultipleApplicationBlock() {
        try {
            multiAppBlockSocket = new ServerSocket(port);
        }
        catch(Exception e) {
            log.warn("Failed to bind to socket: " + port +
                    " which probably means another instance of data-streamer is running.");

            throw new IllegalStateException("Another instance of data-stream is probably running.");
        }
    }
}
