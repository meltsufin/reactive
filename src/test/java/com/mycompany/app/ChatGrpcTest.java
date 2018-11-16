package com.mycompany.app;

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
import java.io.IOException;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Mike Eltsufin
 */
@Slf4j
public class ChatGrpcTest {

  public static final int PORT = 8080;
  private Server server;

  private ChatBlockingStub blockingClient;

  private ChatStub asyncClient;

  private Stopwatch stopwatch = Stopwatch.createUnstarted();

  @Before
  public void before() throws IOException, InterruptedException {
    startServer();
    createClients();

    Stream
        .of("hi there", "how is it going?", "anyone there?", "i can't stay here long", "i gotta go")
        .map(m -> createMessage("John", m))
        .forEach(this.blockingClient::postMessage);

  }

  private void startServer() throws IOException {
    this.server = ServerBuilder
        .forPort(PORT)
        .executor(Executors.newFixedThreadPool(4))
        .addService(new ChatService())
        .build();

    this.stopwatch.reset().start();
    server.start();
    log.info("Server started in {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }

  private void createClients() {
    // Create a channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", PORT)
        .usePlaintext()
        .build();

    // Create blocking and async stubs using the channel
    this.blockingClient = ChatGrpc.newBlockingStub(channel);
    this.asyncClient = ChatGrpc.newStub(channel);
  }

  @After
  public void after() {
    stopwatch.reset().start();
    this.server.shutdown();
    log.info("Server shutdown in {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    stopwatch.reset();
  }

  private ChatMessage createMessage(String author, String text) {
    return ChatMessage.newBuilder()
        .setAuthor(author)
        .setMessage(text)
        .setWhen(System.currentTimeMillis()).build();

  }

  @Test
  public void testPostMessage_Blocking() {
    stopwatch.reset().start();
    blockingClient.postMessage(createMessage("Mike", "Hello"));
    log.info("Posted message in {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));

    stopwatch.reset().start();
    blockingClient.postMessage(createMessage("Mike", "Hello???"));
    log.info("Posted message in {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
  }

  @Test
  public void testPostMessage_Async() throws InterruptedException {
    CountDownLatch finishLatch = new CountDownLatch(1);

    stopwatch.reset().start();
    asyncClient.postMessage(createMessage("Mike", "Hello"), new StreamObserver<Empty>() {
      @Override
      public void onNext(Empty o) {
        log.info("Notified that message posted in {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
      }

      @Override
      public void onError(Throwable throwable) {
        log.error("Send failed in {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS), throwable);
      }

      @Override
      public void onCompleted() {
        log.info("Completed request {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        finishLatch.countDown();
      }
    });

    log.info("Async request sent in {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));

    finishLatch.await(5, TimeUnit.SECONDS);
  }


  @Test
  public void testGetMessages_Blocking() {
    stopwatch.reset().start();

    Iterator<ChatMessage> messages = blockingClient.getMessages(Empty.getDefaultInstance());

    log.info("Got messages in {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));

    messages.forEachRemaining(chatMessage -> log
        .info("Msg: {} says '{}'", chatMessage.getAuthor(), chatMessage.getMessage()));
  }

  @Test
  public void testGetMessages_Async() throws InterruptedException {
    CountDownLatch finishLatch = new CountDownLatch(1);

    stopwatch.reset().start();

    asyncClient
        .getMessages(Empty.getDefaultInstance(), new ClientResponseObserver<Empty, ChatMessage>() {
          ClientCallStreamObserver<Empty> clientCallStreamObserver;

          @Override
          public void beforeStart(ClientCallStreamObserver<Empty> clientCallStreamObserver) {
            this.clientCallStreamObserver = clientCallStreamObserver;

            clientCallStreamObserver.disableAutoInboundFlowControl();

            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
              @Override
              public void run() {
                  clientCallStreamObserver.request(2);
              }
            }, 1000, 1000);
          }

          @Override
          public void onNext(ChatMessage chatMessage) {
            log.info("Msg: {} says '{}' received in {} ms.", chatMessage.getAuthor(),
                chatMessage.getMessage(), stopwatch.elapsed(TimeUnit.MILLISECONDS));
          }

          @Override
          public void onError(Throwable throwable) {
            log.error("Receive failed in {}ms", stopwatch.elapsed(TimeUnit.MILLISECONDS),
                throwable);
          }

          @Override
          public void onCompleted() {
            log.info("Completed request {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
            finishLatch.countDown();
          }
        });

    log.info("Submitted request in {}ms.", stopwatch.elapsed(TimeUnit.MILLISECONDS));

    finishLatch.await(5, TimeUnit.SECONDS);
  }


}
