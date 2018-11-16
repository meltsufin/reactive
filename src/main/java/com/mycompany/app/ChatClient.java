package com.mycompany.app;

import com.mycompany.app.api.ChatGrpc;
import com.mycompany.app.api.ChatGrpc.ChatBlockingStub;
import com.mycompany.app.api.ChatGrpc.ChatStub;
import com.mycompany.app.api.ChatProto.ChatMessage;
import com.mycompany.app.api.ChatProto.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

public class ChatClient {

  public static void main(String[] args) throws InterruptedException {
    System.out.println("Hello World!");
    ChatMessage chatMessage = ChatMessage.newBuilder()
        .setAuthor("Mike")
        .setMessage("Hello")
        .setWhen(System.currentTimeMillis()).build();

    System.out.println(chatMessage);

    // Create a channel
    ManagedChannel channel = ManagedChannelBuilder
        .forAddress("localhost", 8080)
        .usePlaintext()
        .build();

    // Create blocking and async stubs using the channel
    ChatBlockingStub blockingStub =  ChatGrpc.newBlockingStub(channel);
    ChatStub asyncStub = ChatGrpc.newStub(channel);

    blockingStub.postMessage(chatMessage);


    asyncStub.getMessages(Empty.newBuilder().build(), new StreamObserver<ChatMessage>() {
      @Override
      public void onNext(ChatMessage chatMessage) {
        System.out.println("New message arrived: " + chatMessage.toString());
      }

      @Override
      public void onError(Throwable throwable) {
        throwable.printStackTrace();
      }

      @Override
      public void onCompleted() {
        System.out.println("Stream completed.");
      }
    });



    asyncStub.getMessages(Empty.newBuilder().build(), new ClientResponseObserver<Empty, ChatMessage>() {
      @Override
      public void beforeStart(ClientCallStreamObserver<Empty> requestStream) {

        requestStream.request(1);
      }

      @Override
      public void onNext(ChatMessage chatMessage) {
        System.out.println("New message arrived: " + chatMessage.toString());
      }


      @Override
      public void onError(Throwable throwable) {
        throwable.printStackTrace();
      }

      @Override
      public void onCompleted() {
        System.out.println("Stream completed.");
      }
    });



    Thread.sleep(10000);
  }





}
