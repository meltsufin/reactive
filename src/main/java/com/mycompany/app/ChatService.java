/*
 *  Copyright 2018 original author or authors.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.mycompany.app;

import com.mycompany.app.api.ChatGrpc;
import com.mycompany.app.api.ChatProto.ChatMessage;
import com.mycompany.app.api.ChatProto.Empty;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Consumer;

/**
 * See: https://grpc.io/docs/tutorials/basic/java.html
 *
 * @author Mike Eltsufin
 */
public class ChatService extends ChatGrpc.ChatImplBase {

  private List<ChatMessage> messages = Collections.synchronizedList(new LinkedList());
  private List<Consumer<ChatMessage>> observers = Collections.synchronizedList(new ArrayList<>());


  @Override
  public void postMessage(ChatMessage request, StreamObserver<Empty> responseObserver) {
    addMessage(request);
    responseObserver.onNext(Empty.newBuilder().build());
    responseObserver.onCompleted();
  }

  @Override
  public void getMessages(Empty request, StreamObserver<ChatMessage> responseObserver) {
    messages.stream().forEach(responseObserver::onNext);
    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<ChatMessage> postMessages(StreamObserver<Empty> responseObserver) {
    return new StreamObserver<ChatMessage>() {
      @Override
      public void onNext(ChatMessage chatMessage) {
        addMessage(chatMessage);
      }

      @Override
      public void onError(Throwable throwable) {
        responseObserver.onError(throwable);
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  @Override
  public StreamObserver<ChatMessage> chat(StreamObserver<ChatMessage> responseObserver) {

    Consumer<ChatMessage> observer = message -> responseObserver.onNext(message);

    messages.stream().forEach(responseObserver::onNext);

    observers.add(observer);

    return new StreamObserver<ChatMessage>() {
      @Override
      public void onNext(ChatMessage chatMessage) {
        addMessage(chatMessage);
      }

      @Override
      public void onError(Throwable throwable) {
        responseObserver.onError(throwable);
      }

      @Override
      public void onCompleted() {
        observers.remove(observer);
        responseObserver.onCompleted();
      }
    };
  }

  private void addMessage(ChatMessage message) {
    messages.add(message);
    observers.stream().forEach(o -> o.accept(message));
  }
}
