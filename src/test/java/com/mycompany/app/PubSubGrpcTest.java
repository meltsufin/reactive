package com.mycompany.app;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.pubsub.v1.DeleteTopicRequest;
import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import com.google.pubsub.v1.PublisherGrpc.PublisherStub;
import com.google.pubsub.v1.Topic;
import io.grpc.CallCredentials;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.auth.MoreCallCredentials;
import java.io.IOException;
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

  private PublisherBlockingStub blockingClient;

  private PublisherFutureStub futureClient;

  private PublisherStub asyncClient;

  @Before
  public void before() throws IOException {
    createClients();

  }

  @After
  public void after() {
    this.blockingClient.deleteTopic(DeleteTopicRequest.newBuilder().setTopic("projects/eltsufin-sandbox/topics/my-test").build());
  }

  private void createClients() throws IOException {
    // Create a channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("pubsub.googleapis.com", PORT)
        .build();

    // Create blocking and async stubs using the channel
    CallCredentials callCredentials = MoreCallCredentials.from(GoogleCredentials.getApplicationDefault());

    this.blockingClient = PublisherGrpc.newBlockingStub(channel)
        .withCallCredentials(callCredentials);

    this.futureClient = PublisherGrpc.newFutureStub(channel)
        .withCallCredentials(callCredentials);

    this.asyncClient = PublisherGrpc.newStub(channel)
        .withCallCredentials(callCredentials);
  }

  @Test
  public void test() {
    this.blockingClient.createTopic(Topic.newBuilder().setName("projects/eltsufin-sandbox/topics/my-test").build());


  }

}
