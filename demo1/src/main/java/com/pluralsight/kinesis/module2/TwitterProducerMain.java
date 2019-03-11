package com.pluralsight.kinesis.module2;

import com.amazonaws.services.kinesis.producer.*;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class TwitterProducerMain {

    public static void main(String... args) {
        TwitterStream twitterStream = createTwitterStream();
        twitterStream.addListener(createListener());
        twitterStream.sample();
    }

    private static TwitterStream createTwitterStream() {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("...")
                .setOAuthConsumerSecret("...")
                .setOAuthAccessToken("...")
                .setOAuthAccessTokenSecret("...");

        return new TwitterStreamFactory(cb.build()).getInstance();
    }

    private static RawStreamListener createListener() {
        KinesisProducer kinesisProducer = createKinesisProducer();
        return new TweetsStatusListener(kinesisProducer);
    }

    private static KinesisProducer createKinesisProducer() {
        KinesisProducerConfiguration config = new KinesisProducerConfiguration()
                // Longer timeout for slower connections
                .setRequestTimeout(60000)
                // Longer buffered time for more aggregation
                .setRecordMaxBufferedTime(15000)
                // AWS region
                .setRegion("us-east-1");
        return new KinesisProducer(config);
    }

    static class TweetsStatusListener implements RawStreamListener {
        private final KinesisProducer kinesisProducer;
        int count = 0;

        public TweetsStatusListener(KinesisProducer kinesisProducer) {
            this.kinesisProducer = kinesisProducer;
        }

        public void onMessage(String rawString) {
            // Reduce amount of records
            if (count++ % 5 != 0) return;
            try {
                Status status = TwitterObjectFactory.createStatus(rawString);
                // If new tweet
                if (status.getUser() != null) {

                    byte[] tweetsBytes = rawString.getBytes(StandardCharsets.UTF_8);
                    String partitionKey = status.getLang();
                    ListenableFuture<UserRecordResult> f = kinesisProducer.addUserRecord(
                            "tweets-stream",
                            partitionKey,
                            ByteBuffer.wrap(tweetsBytes));

                    Futures.addCallback(f, new FutureCallback<UserRecordResult>() {
                        @Override
                        public void onSuccess(UserRecordResult result) {
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            if (t instanceof UserRecordFailedException) {
                                UserRecordFailedException e =
                                        (UserRecordFailedException) t;
                                UserRecordResult result = e.getResult();

                                Attempt last = Iterables.getLast(result.getAttempts());
                                System.err.println(String.format(
                                        "Put failed - %s",
                                        last.getErrorMessage()));
                            }
                        }
                    });
                }

            } catch (TwitterException e) {
                e.printStackTrace();
            }
        }

        public void onException(Exception ex) {
            ex.printStackTrace();
        }
    }
}
