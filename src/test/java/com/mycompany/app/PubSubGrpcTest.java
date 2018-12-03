package com.mycompany.app;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.DeleteSubscriptionRequest;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import com.google.pubsub.v1.SubscriberGrpc;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberBlockingStub;
import com.google.pubsub.v1.SubscriberGrpc.SubscriberStub;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.Topic;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Mike Eltsufin
 */
@Slf4j
public class PubSubGrpcTest {

  public static final int PORT = 443;
  public static final String TEST_TOPIC = "projects/eltsufin-sandbox/topics/my-test";
  public static final String TEST_TOPIC_SUBSCRIPTION = "projects/eltsufin-sandbox/subscriptions/my-test";

  private PublisherBlockingStub publisherBlockingClient;

  private SubscriberBlockingStub subscriberBlockingStub;
  private SubscriberStub subscriberAsyncStub;

  @Before
  public void before() throws IOException {
    createClients();

    this.publisherBlockingClient.createTopic(Topic.newBuilder().setName(TEST_TOPIC).build());
    this.subscriberBlockingStub.createSubscription(
        Subscription.newBuilder().setTopic(TEST_TOPIC).setName(TEST_TOPIC_SUBSCRIPTION).build());

    publishMessages(1000);
    publishMessages(1000);
    publishMessages(1000);
  }


  @After
  public void after() {
    this.publisherBlockingClient
        .deleteTopic(DeleteTopicRequest.newBuilder().setTopic(TEST_TOPIC).build());
    this.subscriberBlockingStub.deleteSubscription(
        DeleteSubscriptionRequest.newBuilder().build().newBuilder()
            .setSubscription(TEST_TOPIC_SUBSCRIPTION).build());
  }

  private void createClients() throws IOException {
    // Create a channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("pubsub.googleapis.com", PORT)
        .executor(Executors.newCachedThreadPool())
        .build();

    // Create blocking and async stubs using the channel
    CallCredentials callCredentials = MoreCallCredentials
        .from(GoogleCredentials.getApplicationDefault());

    this.publisherBlockingClient = PublisherGrpc.newBlockingStub(channel)
        .withCallCredentials(callCredentials);

    this.subscriberBlockingStub = SubscriberGrpc.newBlockingStub(channel)
        .withCallCredentials(callCredentials);

    this.subscriberAsyncStub = SubscriberGrpc.newStub(channel)
        .withCallCredentials(callCredentials);

  }

  public void publishMessages(int count) {

    PublishRequest.Builder publishRequestBuilder = PublishRequest.newBuilder().setTopic(TEST_TOPIC);

    for (int i = 0; i < count; i++) {
      publishRequestBuilder.addMessages(PubsubMessage.newBuilder().setData(ByteString
          .copyFromUtf8("msg " + i)).build());
    }

    PublishResponse response = this.publisherBlockingClient.publish(publishRequestBuilder.build());
    System.out.println("Published: " + response.getMessageIdsCount());
  }

  @Test
  public void testSubscribe() throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(1);

    StreamObserver<StreamingPullRequest> requestObserver =
        this.subscriberAsyncStub.streamingPull(new StreamObserver<StreamingPullResponse>() {
          @Override
          public void onNext(StreamingPullResponse value) {
            System.out.println("Received: " + value.getReceivedMessagesCount() + " messages.");
          }

          @Override
          public void onError(Throwable t) {
            t.printStackTrace();
            latch.countDown();
          }

          @Override
          public void onCompleted() {
            System.out.println("Completed.");
            latch.countDown();
          }
        });

    System.out.println("Sending initial request.");
    requestObserver.onNext(StreamingPullRequest.newBuilder()
        .setSubscription(TEST_TOPIC_SUBSCRIPTION)
        .setStreamAckDeadlineSeconds(10)
        .build());

    new Timer().schedule(new TimerTask() {
      @Override
      public void run() {
        // initiate end of stream
        requestObserver.onCompleted();
      }
    }, 5000);

    latch.await(10, TimeUnit.SECONDS);

  }

  @Test
  public void testSubscribe_FlowControl() throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(1);

    ClientCallStreamObserver<StreamingPullRequest> clientCallStreamObservers;

    StreamObserver<StreamingPullRequest> requestObserver =
        this.subscriberAsyncStub.streamingPull(
            new ClientResponseObserver<StreamingPullRequest, StreamingPullResponse>() {
              @Override
              public void beforeStart(
                  ClientCallStreamObserver<StreamingPullRequest> clientCallStreamObserver) {

                clientCallStreamObserver.disableAutoInboundFlowControl();

                new Timer().schedule(new TimerTask() {
                  @Override
                  public void run() {

                    System.out.println("Sending request for new batch.");
                    clientCallStreamObserver.request(1);

                  }
                }, 3000, 1000);
              }

              @Override
              public void onNext(StreamingPullResponse value) {
                System.out.println(
                    "Received batch of " + value.getReceivedMessagesCount() + " messages.");
              }

              @Override
              public void onError(Throwable t) {
                t.printStackTrace();
                latch.countDown();
              }

              @Override
              public void onCompleted() {
                System.out.println("Completed.");
                latch.countDown();
              }
            });

    System.out.println("Sending initial request.");
    requestObserver.onNext(StreamingPullRequest.newBuilder()
        .setSubscription(TEST_TOPIC_SUBSCRIPTION)
        .setStreamAckDeadlineSeconds(10)
        .build());

    new Timer().schedule(new TimerTask() {
      @Override
      public void run() {
        // initiate end of stream
        requestObserver.onCompleted();
      }
    }, 5000);

    latch.await(1, TimeUnit.MINUTES);

  }

}
