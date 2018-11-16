package com.mycompany.app;

import com.google.pubsub.v1.PublisherGrpc;
import com.google.pubsub.v1.PublisherGrpc.PublisherBlockingStub;
import com.google.pubsub.v1.PublisherGrpc.PublisherFutureStub;
import com.google.pubsub.v1.PublisherGrpc.PublisherStub;
import com.google.pubsub.v1.Topic;
import java.io.IOException;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import com.google.common.base.Stopwatch;
import com.mycompany.app.api.ChatGrpc;
import com.mycompany.app.api.ChatGrpc.ChatBlockingStub;
import com.mycompany.app.api.ChatGrpc.ChatStub;
import com.mycompany.app.api.ChatProto.ChatMessage;
import com.mycompany.app.api.ChatProto.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
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
  public void before() {
    createClients();

  }

  private void createClients() {
    // Create a channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("pubsub.googleapis.com", PORT)
        .build();

    // Create blocking and async stubs using the channel
    this.blockingClient = PublisherGrpc.newBlockingStub(channel);
    this.futureClient = PublisherGrpc.newFutureStub(channel);
    this.asyncClient = PublisherGrpc.newStub(channel);
  }

  @Test
  public void test() {
    this.blockingClient.createTopic(Topic.newBuilder().setName("my-test").build());
  }

  @After
  public void after() {

  }

}
