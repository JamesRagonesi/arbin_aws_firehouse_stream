package com.forgenano.datastream.arbin;

import com.amazonaws.services.kinesis.model.InvalidArgumentException;
import com.forgenano.datastream.DataStreamer;
import com.forgenano.datastream.model.ArbinChannelEvent;
import com.forgenano.datastream.status.StatusMaintainer;
import com.forgenano.datastream.util.NamedThreadFactory;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InvalidClassException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.forgenano.datastream.model.ArbinChannelEvent.*;

/**
 * Created by michael on 4/7/17.
 */
public class ArbinDbStreamer {

    private static final Logger log = LoggerFactory.getLogger(ArbinDbStreamer.class);

    private static final int MAX_NEW_DATA_WAIT_ITERATIONS = 12; //  should be 2 minutes worth of waiting before exiting.
    private static final long WAIT_FOR_NEW_DATA_TIME = 10;
    private static final TimeUnit WAIT_FOR_NEW_DATA_TIMEUNIT = TimeUnit.SECONDS;

    /**
     * Attempts to create a connection to the specified arbin database file.
     *
     * @param arbinDbPath a string path to the arbin database file.
     * @return fully setup ArbinDbStreamer.
     * @throws InvalidClassException
     */
    public static ArbinDbStreamer CreateArbinDbStreamer(Path arbinDbPath, DataStreamer dataStreamer) {

        if (!Files.exists(arbinDbPath) || !Files.isReadable(arbinDbPath)) {
            log.error("The file: " + arbinDbPath.toAbsolutePath().toString() + " doesn't exist or isn't readable.");

            throw new InvalidArgumentException("Arbin DB File doesn't exist or isn't readable.");
        }

        try {
            Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
        }
        catch(Exception e) {
            log.error("Failed to get the : " + e.getMessage());

            throw new IllegalStateException("Missing UcanaccessDriver in class path.");
        }

        Connection dbConnection;
        try {
            dbConnection = DriverManager.getConnection("jdbc:ucanaccess://" + arbinDbPath.toAbsolutePath());
            dbConnection.setReadOnly(true);
        }
        catch (Exception e) {
            log.error("Failed to establish the connection to the arbin db file because: " + e.getMessage());

            throw new InvalidArgumentException("Arbin DB connection failed.");
        }

        return new ArbinDbStreamer(arbinDbPath, dataStreamer, dbConnection, 1000);
    }

    private static final String CHANNEL_TEST_ENTRY_QUERY =
            "SELECT c.Test_ID, c.Data_Point, c.Test_Time, c.Step_Time, c.DateTime, c.Step_Index, c.Cycle_Index, " +
                    "c.Is_FC_Data, c.Current, c.Voltage, c.Charge_Capacity, c.Discharge_Capacity, c.Charge_Energy, " +
                    "c.Discharge_Energy, c.`dV/dt`, c.Internal_Resistance, c.AC_Impedance, c.ACI_Phase_Angle, " +
                    "g.Test_Name, g.Channel_Index, g.DAQ_Index, g.Channel_Type, g.Creator, g.Schedule_File_Name, " +
                    "g.Comments, s.Vmax_On_Cycle, s.Charge_Time, s.Discharge_Time " +
             "FROM Channel_Normal_Table as c " +
             "LEFT JOIN Global_Table as g on c.Test_ID = g.Test_ID " +
             "LEFT JOIN Channel_Statistic_Table as s on c.Test_ID = s.Test_ID AND c.Data_Point = s.Data_Point " +
             "ORDER BY c.Data_Point ASC, c.Test_ID LIMIT ? OFFSET ?";

    private static final String VERIFY_COMPATIBLE_DATA_QUERY =
            "SELECT g.Test_ID, g.Test_Name, g.Channel_Index, g.Comments " +
            "FROM Global_Table as g;";

    private static final String CHANNEL_TEST_COUNT_QUERY =
            "SELECT count(c.Data_Point) FROM Channel_Normal_Table as c where c.Data_point > ?;";

    private Path arbinDbPath;
    private DataStreamer dataStreamer;
    private ExecutorService eventNotifierService;
    private ExecutorService eventMonitorService;
    private Connection dbConnection;
    private AtomicBoolean shutdownSignal;
    private List<ArbinDataConsumer> eventConsumers;
    private SynchronousQueue<ArbinChannelEvent> newEventQueue;
    private PreparedStatement channelTestEntryStatement;
    private PreparedStatement channelTestCountStatement;
    private PreparedStatement dataCompatibilitySatement;
    private int resultPageSize;
    private ReentrantLock dataFileLock;
    private Condition dataFileModifiedCondition;
    private AtomicInteger numberOfWaitCycles;

    private Runnable eventNotifierRunnable = () -> {
        while(!this.shutdownSignal.get()) {
            try {
                ArbinChannelEvent newEvent = this.newEventQueue.poll(3, TimeUnit.SECONDS);

                if (newEvent != null) {
                    this.notifyEventConsumers(newEvent);
                }
            }
            catch(Exception e) {
                log.error("Caught " + e.getClass().getSimpleName() + " while waiting for a new arbin event.", e);
            }
        }
    };

    private Runnable eventMonitorRunnable = () -> {
        long lastDataPoint = 0;

        StatusMaintainer.getSingleton().startedConsumingArbinDb(this.arbinDbPath);
        if (StatusMaintainer.getSingleton().hasStatusForArbinDb(this.arbinDbPath)) {
            lastDataPoint = StatusMaintainer.getSingleton().getLastConsumedOffsetForArbinDb(this.arbinDbPath);
        }

        while(!this.shutdownSignal.get() && numberOfWaitCycles.get() <= MAX_NEW_DATA_WAIT_ITERATIONS) {
            try {
                long numberOfRecordsToConsume = getNumberOfDataEventsAvailable(lastDataPoint);

                if (numberOfRecordsToConsume > 0){
                    // reset wait for data iteration count
                    this.numberOfWaitCycles.set(0);

                    lastDataPoint = consumeAvailableArbinData(numberOfRecordsToConsume);
                    StatusMaintainer.getSingleton().updateLastConsumedChannelOffset(this.arbinDbPath, lastDataPoint);

                    log.info("Finished consuming all available content for: " + this.arbinDbPath.toAbsolutePath().toString());
                }
                else {
                    try {
                        this.numberOfWaitCycles.getAndIncrement();

                        this.dataFileLock.lock();
                        log.info("Waiting " + WAIT_FOR_NEW_DATA_TIME + " " + WAIT_FOR_NEW_DATA_TIMEUNIT.name() +
                                " for new arbin db content: " + this.arbinDbPath.toAbsolutePath().toString());
                        this.dataFileModifiedCondition.await(WAIT_FOR_NEW_DATA_TIME, WAIT_FOR_NEW_DATA_TIMEUNIT);
                    }
                    finally {
                        this.dataFileLock.unlock();
                    }
                }
            }
            catch(Exception e) {
                log.error("An exception occurred while monitoring for new arbin events: ", e);
            }
        }
        if (this.shutdownSignal.get()) {
            log.info("Received external shutdown signal, stopping monitoring of: " +
                    this.arbinDbPath.toAbsolutePath().toString());
        }
        else {
            log.info("Shutting down monitoring after waiting 2 minutes for new data: " +
                    arbinDbPath.toAbsolutePath().toString());

            this.shutdown();
            this.dataStreamer.handleArbinDbStreamerShuttingDown(this, this.arbinDbPath);
        }

    };

    private ArbinDbStreamer(Path arbinDbPath, DataStreamer dataStreamer, Connection dbConnection, int resultPageSize)  {
        this.dataStreamer = dataStreamer;
        this.arbinDbPath = arbinDbPath;
        this.dbConnection = dbConnection;
        this.eventConsumers = Lists.newArrayList();
        this.eventMonitorService = Executors.newSingleThreadExecutor(
                NamedThreadFactory.NewNamedDaemonThreadFactory("ArbinDBEventMonitor"));
        this.eventNotifierService = Executors.newSingleThreadExecutor(
                NamedThreadFactory.NewNamedDaemonThreadFactory("ArbinDbEventNotifier"));
        this.newEventQueue = Queues.newSynchronousQueue();
        this.shutdownSignal = new AtomicBoolean(false);
        this.resultPageSize = resultPageSize;
        this.dataFileLock = new ReentrantLock();
        this.dataFileModifiedCondition = this.dataFileLock.newCondition();
        this.numberOfWaitCycles = new AtomicInteger(0);

        this.eventNotifierService.submit(this.eventNotifierRunnable);

        try {
            this.channelTestEntryStatement = this.dbConnection.prepareStatement(CHANNEL_TEST_ENTRY_QUERY);
            this.channelTestEntryStatement.setFetchSize(this.resultPageSize);
            this.channelTestEntryStatement.setMaxRows(this.resultPageSize);

            this.channelTestCountStatement = this.dbConnection.prepareStatement(CHANNEL_TEST_COUNT_QUERY);
            this.dataCompatibilitySatement = this.dbConnection.prepareStatement(VERIFY_COMPATIBLE_DATA_QUERY);
        }
        catch(Exception e) {
            log.error("Failed to build the channel test entry query because: " + e.getMessage());
            throw new IllegalStateException("Failed to build a prepared query: ", e);
        }
    }

    public void shutdown() {
        this.shutdownSignal.set(true);

        this.eventNotifierService.shutdown();
        this.eventMonitorService.shutdown();
    }

    public synchronized void addConsumer(ArbinDataConsumer consumer) {
        if (!this.eventConsumers.contains(consumer)) {
            this.eventConsumers.add(consumer);
        }
    }

    public void startLiveMonitoring() {
        if (!this.isArbinDbFileCompatible()) {
            log.warn("Ignoring incompatible arbin db file: " + this.arbinDbPath.toAbsolutePath().toString());
            this.shutdown();

            throw new IllegalStateException("Not compatible arbin file.");
        }

        this.eventMonitorService.submit(this.eventMonitorRunnable);
    }

    public void blockAndStreamFinishedArbinDatabase() {
        if (!this.isArbinDbFileCompatible()) {
           log.warn("Ignoring incompatible arbin file: " + this.arbinDbPath.toAbsolutePath().toString());

           throw new IllegalStateException("Not compatible arbin file");
        }

        StatusMaintainer.getSingleton().startedConsumingArbinDb(this.arbinDbPath);
        long lastOffset = consumeAvailableArbinData(getNumberOfDataEventsAvailable(0));
        StatusMaintainer.getSingleton().updateLastConsumedChannelOffset(this.arbinDbPath, lastOffset);
    }

    public void dataFileWasModified() {
        try {
            this.dataFileLock.lock();

            this.dataFileModifiedCondition.signalAll();
        }
        catch (Exception e) {
            log.warn("Caught an exception while notifying the data file monitor: ", e);
        }
        finally {
            this.dataFileLock.unlock();
        }
    }

    private long getNumberOfDataEventsAvailable(long sinceLastDataPoint) {
        long numberOfRecordsToConsume = 0;

        try {
            this.channelTestCountStatement.setLong(1, sinceLastDataPoint);

            ResultSet countResult = this.channelTestCountStatement.executeQuery();

            if (countResult.next()) {
                numberOfRecordsToConsume = countResult.getLong(1);
            }
            else {
                log.warn("No arbin channel data events to consume.");
            }
        }
        catch(Exception e) {
            log.error("Failed to find the number of arbin data events because: " + e.getMessage());
            throw new IllegalStateException("Don't know the number of arbin data events.", e);
        }

        return numberOfRecordsToConsume;
    }

    private synchronized void notifyEventConsumers(ArbinChannelEvent event) {
        this.eventConsumers.parallelStream().forEach(consumer -> {
            consumer.consume(event);
        });
    }

    private long consumeAvailableArbinData(long numberOfRecordsToConsume) {
        long offset = 0;

        log.info("Consuming " + numberOfRecordsToConsume + " of arbin channel data.");

        while(offset < numberOfRecordsToConsume) {
            try {
                this.channelTestEntryStatement.setInt(1, this.resultPageSize);
                this.channelTestEntryStatement.setLong(2, offset);
            }
            catch(Exception e) {
                log.error("Failed to setup the channel test entry query with bounds. Because: ", e);
                break;
            }

            try {
                ResultSet channelQueryResultSet = this.channelTestEntryStatement.executeQuery();

                while(channelQueryResultSet.next()) {
                    ArbinChannelEvent arbinChannelEvent = convertArbinFromChannelTestEntryResultSet(channelQueryResultSet);

                    this.newEventQueue.put(arbinChannelEvent);
                    offset++;
                }

                StatusMaintainer.getSingleton().updateLastConsumedChannelOffset(this.arbinDbPath, offset);

                System.gc();
            }
            catch(Exception e) {
                log.error("Failed to execute channel test entry query with offset: " + offset + " - limit:" +
                        this.resultPageSize + " because: ", e);
                break;
            }

        }

        log.info("Finished consuming " + numberOfRecordsToConsume + " records...");

        return offset;
    }

    public boolean isArbinDbFileCompatible() throws IllegalStateException {
        try {
            ResultSet compatibleDataResults = this.dataCompatibilitySatement.executeQuery();

            boolean compatible = false;

            while(compatibleDataResults.next()) {
                String test_name = compatibleDataResults.getString("Test_Name");
                String comments = compatibleDataResults.getString("Comments");

                try {
                    ArbinTestMetadata metadata =
                            ArbinChannelEvent.ArbinTestMetadata.FromCommentsString(comments, test_name);

                    compatible = metadata != null;

                    if (!compatible) {
                        log.warn("One of the arbin channel tests doesn't have the proper metadata, marking the entire file as not compatible.");
                        break;
                    }
                }
                catch(Exception e) {
                    log.warn("Couldn't get the arbin test metadata, which means this db isn't compatible.");
                    compatible = false;
                    break;
                }
            }

            return compatible;
        }
        catch(Exception e) {
            log.error("Failed to query the arbin db file for compatiblity because: ", e);

            throw new IllegalStateException("Couldn't query arbin db for compatibility: ", e);
        }
    }

    private ArbinChannelEvent convertArbinFromChannelTestEntryResultSet(ResultSet resultSetRow) {
        ArbinChannelEvent newArbinChannelEvent = null;
        try {
            long testId = resultSetRow.getLong("Test_ID");
            String testName = resultSetRow.getString("Test_Name");
            long dataPoint = resultSetRow.getLong("Data_Point");
            int channelIndex = resultSetRow.getInt("Channel_Index");
            int daqIndex = resultSetRow.getInt("DAQ_Index");
            int channelType = resultSetRow.getInt("Channel_Type");
            String creator = resultSetRow.getString("Creator");
            double testTimeRaw = resultSetRow.getDouble("Test_Time");
            double stepTimeRaw = resultSetRow.getDouble("Step_Time");
            double dateTimeRaw = resultSetRow.getDouble("DateTime");
            int stepIndex = resultSetRow.getInt("Step_Index");
            int cycleIndex = resultSetRow.getInt("Cycle_Index");
            boolean isFCData = resultSetRow.getBoolean("Is_FC_Data");
            double current = resultSetRow.getDouble("Current");
            double voltage = resultSetRow.getDouble("Voltage");
            double chargeCapacity = resultSetRow.getDouble("Charge_Capacity");
            double dischargeCapacity = resultSetRow.getDouble("Discharge_Capacity");
            double dV_dt = resultSetRow.getDouble("dV/dt");
            double internalResistence = resultSetRow.getDouble("Internal_Resistance");
            double acImpedance = resultSetRow.getDouble("AC_Impedance");
            double aciPhaseAngle = resultSetRow.getDouble("ACI_Phase_Angle");
            String scheduleFileName = resultSetRow.getString("Schedule_File_Name");
            String comments = resultSetRow.getString("Comments");

            // s.Vmax_On_Cycle, s.Charge_Time, s.Discharge_Time
            double vMaxOnCycleStat = resultSetRow.getDouble("Vmax_On_Cycle");
            double chargeTimeCycleStat = resultSetRow.getDouble("Charge_Time");
            double dischargeTimeCycleStat = resultSetRow.getDouble("Discharge_Time");

            ArbinChannelEvent.ArbinCycleStatistics cycleStats = null;
            if (!resultSetRow.wasNull()) {
                cycleStats = new ArbinChannelEvent.ArbinCycleStatistics(vMaxOnCycleStat, chargeTimeCycleStat,
                        dischargeTimeCycleStat);
            }

            newArbinChannelEvent = new ArbinChannelEvent(
                    testId, testName, dataPoint, channelIndex, daqIndex, channelType, creator, testTimeRaw, stepTimeRaw,
                    dateTimeRaw, stepIndex, cycleIndex, isFCData, current, voltage, chargeCapacity, dischargeCapacity,
                    dV_dt, internalResistence, acImpedance, aciPhaseAngle, scheduleFileName, comments, cycleStats);
        }
        catch(Exception e) {
            log.warn("Failed to convert arbin channel data because: ", e);
        }

        return newArbinChannelEvent;
    }

    public void dumpArbinDbDetails() {
        try {
            DatabaseMetaData metaData = this.dbConnection.getMetaData();
            ResultSet tableResults = metaData.getTables(null, null, "%", null);

            log.info("Arbin file contains tables: ");

            while (tableResults.next()) {
                String tableName = tableResults.getString(3);

                log.info("Table: " + tableName);

                ResultSet columnResults = metaData.getColumns(null, null, tableName, null);

                String firstColName = null;
                while(columnResults.next()) {
                    String columnName = columnResults.getString(4);
                    int columnTypeNum = columnResults.getInt(5);
                    String columnTypeName = columnResults.getString(6);

                    if (firstColName == null)
                        firstColName = columnName;

                    log.info("    Column: " + columnName + " type: " + columnTypeName + " - " + columnTypeNum);
                }

                ResultSet numberOfRows = this.dbConnection.createStatement().executeQuery(
                        "select count(" + firstColName + ") from " + tableName + ";");

                if (numberOfRows.next()) {
                    int numEntries =numberOfRows.getInt(1);
                    log.info("        Contains " + numEntries + " entries.");

                    if (numEntries > 0) {
                        int limit = 5;

                        if (numEntries <= 10)
                            limit = 10;

                        ResultSet exampleRows = this.dbConnection.createStatement().executeQuery(
                                "SELECT * from " + tableName + " limit " + limit + ";");

                        log.info("        Example Rows:");

                        ResultSetMetaData exampleRowsMetadata = exampleRows.getMetaData();
                        int numColumns = exampleRowsMetadata.getColumnCount();

                        while (exampleRows.next()) {
                            StringBuffer exampleRow = new StringBuffer();

                            exampleRow.append("          - ");

                            for (int i = 1; i <= numColumns; i++) {
                                exampleRow.append("'");
                                exampleRow.append(exampleRows.getString(i));
                                exampleRow.append("', ");
                            }

                            log.info(exampleRow.toString());
                        }
                    }
                }
            }
        }
        catch(Exception e) {
            log.error("Failed to get the tables for the db because: " + e.getMessage());
        }
    }


}
