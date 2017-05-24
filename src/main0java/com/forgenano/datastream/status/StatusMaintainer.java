package com.forgenano.datastream.status;

import com.forgenano.datastream.model.StatusModel;
import com.forgenano.datastream.util.NamedThreadFactory;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by michael on 4/18/17.
 */
public class StatusMaintainer {

    private static final Logger log = LoggerFactory.getLogger(StatusMaintainer.class);

    private static final String StatusFileStr = "./data-streamer.status";

    private static StatusMaintainer instance;

    public static StatusMaintainer getSingleton() {
        if (instance == null)
            instance = new StatusMaintainer();

        return instance;
    }

    private StatusModel statusModel;
    private ExecutorService statusWriteService;
    private AtomicBoolean shutdown;
    private ReentrantLock writeNotificationLock;
    private Condition writeCondition;
    private AtomicBoolean writeStatusFlag;

    private StatusMaintainer() {
        this.statusWriteService = Executors.newSingleThreadExecutor(
                NamedThreadFactory.NewNamedDaemonThreadFactory("StatusMaintainerWorker"));
        this.shutdown = new AtomicBoolean(false);
        this.writeStatusFlag = new AtomicBoolean(false);
        this.writeNotificationLock = new ReentrantLock();
        this.writeCondition = this.writeNotificationLock.newCondition();

        try {
            this.statusModel = getFromLocalFile();
        }
        catch(Exception e) {
            log.warn(e.getMessage());

            log.info("Creating new status file at: " + StatusFileStr);

            this.statusModel = new StatusModel();

            writeStatusToFile();
        }

        this.statusWriteService.submit(this.writeStatusRunnable);
    }

    private Runnable writeStatusRunnable = () -> {
        while(!this.shutdown.get())  {
            try {
                this.writeNotificationLock.lock();

                if (this.writeStatusFlag.get()) {
                    writeStatusToFile();

                    this.writeStatusFlag.set(false);
                }

                writeCondition.await(3, TimeUnit.SECONDS);
            }
            catch(Exception e) {
                log.warn("Caught an exception while working the write status runnable: ", e);
            }
            finally {
                this.writeNotificationLock.unlock();
            }
        }
    };

    public void saveStatusFileBlocking() {
        try {
            this.writeStatusToFile();
        }
        catch(Exception e) {
            log.error("Failed to write the status file: ", e);
        }
    }

    public void shutdown() {
        this.shutdown.set(true);

        this.statusWriteService.shutdown();
    }

    public synchronized void startedConsumingArbinDb(Path arbinDbPath) {
        this.statusModel.addNewArbinFileStatus(arbinDbPath);

        notifyWriteServiceToWrite();
    }

    public synchronized void updateLastConsumedChannelOffset(Path arbinDbPath, long lastConsumedChannelOffset) {
        this.statusModel.updateLastConsumedChannelOffset(arbinDbPath, lastConsumedChannelOffset);

        notifyWriteServiceToWrite();
    }

    public synchronized boolean hasStatusForArbinDb(Path arbinDbPath) {
        return this.statusModel.hasStatusForArbinDbPath(arbinDbPath);
    }

    public synchronized long getLastConsumedOffsetForArbinDb(Path arbinDbPath) {
        try {
            return this.statusModel.getLastConsumedChannelOffset(arbinDbPath);
        }
        catch(Exception e) {
            log.warn("No last consumed offset was available for: " + arbinDbPath.toAbsolutePath().toString());

            return 0;
        }
    }

    private synchronized void notifyWriteServiceToWrite() {
        try {
            this.writeNotificationLock.lock();

            this.writeStatusFlag.set(true);

        }
        finally {
            this.writeNotificationLock.unlock();
        }
    }

    private synchronized StatusModel getFromLocalFile() throws IllegalStateException {
        Path statusFilePath = Paths.get(StatusFileStr);

        if (Files.exists(statusFilePath) && Files.isReadable(statusFilePath)) {
            try {
                log.info("Loading existing status model file...");

                InputStreamReader statusFileReader = new InputStreamReader(
                        Files.newInputStream(statusFilePath, StandardOpenOption.READ), StatusModel.ENCODING_NAME);

                return statusModel = new Gson().fromJson(statusFileReader, StatusModel.class);
            }
            catch(Exception e) {
                throw new IllegalStateException("Failed to read and parse the status file: " +
                        statusFilePath.toAbsolutePath().toString(), e);
            }
        }
        else {
            throw new IllegalStateException("Status file is missing or unreadable: " +
                    statusFilePath.toAbsolutePath().toString());
        }
    }

    private synchronized void writeStatusToFile() {
        Path statusFilePath = Paths.get(StatusFileStr);

        try {
            log.info("Writing status to file...");

            byte[] statusModelBytes =
                    new Gson().toJson(this.statusModel, StatusModel.class).getBytes(StatusModel.ENCODING_NAME);

            Files.write(statusFilePath, statusModelBytes, StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);

        }
        catch (Exception e) {
            log.error("An error occurred while writing the status to the file: " +
                    statusFilePath.toAbsolutePath().toString(), e);
        }
    }
}
