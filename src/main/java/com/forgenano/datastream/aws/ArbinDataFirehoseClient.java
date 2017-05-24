package com.forgenano.datastream.aws;

import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.forgenano.datastream.model.ArbinChannelEvent;
import com.forgenano.datastream.util.NamedThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by michael on 4/10/17.
 */
public class ArbinDataFirehoseClient {

    private static final Logger log = LoggerFactory.getLogger(ArbinDataFirehoseClient.class);

    private static final Charset UTF8_CHARSET = Charset.forName("UTF-8");
    private static final String RECORD_DELEMITER = "\n";
    private static final int NUM_ASYNC_WORKERS = 10;


    public static ArbinDataFirehoseClient BuildKinesisFirehoseClient(String regionName, String streamName) {
        try {
            AWSCredentialsProviderChain creds = new DefaultAWSCredentialsProviderChain();

            Regions region = Regions.fromName(regionName);


            AmazonKinesisFirehoseAsync kinesisAsyncClient = AmazonKinesisFirehoseAsyncClientBuilder.standard()
                    .withExecutorFactory(new ExecutorFactory() {
                        @Override
                        public ExecutorService newExecutor() {
                            return Executors.newFixedThreadPool(NUM_ASYNC_WORKERS,
                                    NamedThreadFactory.NewNamedDaemonThreadFactory("AWSAsyncWorker"));
                        }
                    })
                    .withCredentials(creds)
                    .withRegion(region)
                    .build();

            return new ArbinDataFirehoseClient(kinesisAsyncClient, streamName);
        }
        catch(Exception e) {
            log.error("Failed to setup the firehose kinesis client for stream: " + streamName + " and region: " +
                    regionName + " because: ", e);

            throw new IllegalStateException("Failed to setup the kinesis client", e);
        }
    }

    private AmazonKinesisFirehoseAsync kinesisAsyncClient;
    private String defaultStreamName;
    private ExecutorService asyncLoggingService;

    private ArbinDataFirehoseClient(AmazonKinesisFirehoseAsync kinesisAsyncClient, String defaultStreamName) {
        this.asyncLoggingService = Executors.newFixedThreadPool(5,
                NamedThreadFactory.NewNamedDaemonThreadFactory("FirehoseClientResultWorker"));
        this.kinesisAsyncClient = kinesisAsyncClient;
        this.defaultStreamName = defaultStreamName;
    }

    public void writeArbinEvent(ArbinChannelEvent event, boolean blockSynchronously) {
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setDeliveryStreamName(this.defaultStreamName);

        Record record = new Record();

        StringBuffer recordData = new StringBuffer();
        recordData.append(event.toJsonString());
        recordData.append(RECORD_DELEMITER);

        record.setData(ByteBuffer.wrap(recordData.toString().getBytes(UTF8_CHARSET)));

        putRecordRequest.setRecord(record);

        Future<PutRecordResult> resultFuture = this.kinesisAsyncClient.putRecordAsync(putRecordRequest);

        Runnable resultLoggingRunnable = () -> {
            try {
                PutRecordResult result = resultFuture.get();

                log.info("Firehose result: " + result);
            }
            catch (Exception e) {
                log.error("Failed to get the result: ", e);
            }
        };

        if (blockSynchronously) {
            resultLoggingRunnable.run();
        }
        else {
            this.asyncLoggingService.submit(resultLoggingRunnable);
        }

    }
}
