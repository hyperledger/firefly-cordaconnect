// Copyright © 2021 Kaleido, Inc.
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.kaleido.cordaconnector.service;

import io.kaleido.cordaconnector.db.IEventStreamRepository;
import io.kaleido.cordaconnector.db.ISubscriptionRepository;
import io.kaleido.cordaconnector.db.entity.EventStreamInfo;
import io.kaleido.cordaconnector.db.entity.SubscriptionInfo;
import io.kaleido.cordaconnector.exception.CordaConnectionException;
import io.kaleido.cordaconnector.model.common.EventStreamData;
import io.kaleido.cordaconnector.model.common.SubscriptionData;
import io.kaleido.cordaconnector.rpc.NodeRPCClient;
import io.kaleido.cordaconnector.ws.ClientMessage;
import io.kaleido.cordaconnector.ws.WebSocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import javax.transaction.Transactional;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class EventStreamService {
    private static final Logger logger = LoggerFactory.getLogger(EventStreamService.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    @Autowired
    private NodeRPCClient rpcClient;
    @Autowired
    private IEventStreamRepository eventStreamRepository;
    @Autowired
    private ISubscriptionRepository subscriptionRepository;
    private Map<String, WebSocketConnection> webSocketConnectionMap;
    // Map of subscription id to runtime subscription object
    private ConcurrentHashMap<String, Subscription> subscriptions;
    private ConcurrentHashMap<String, String> topicToEventStreamId;
    // Map of stream Id to runtime stream object
    private ConcurrentHashMap<String, EventStream> streams;
    public EventStreamService() {
        this.streams = new ConcurrentHashMap<>();
        this.subscriptions = new ConcurrentHashMap<>();
        this.webSocketConnectionMap = new HashMap<>();
        this.topicToEventStreamId = new ConcurrentHashMap<>();
        scheduler.submit(() -> {
            boolean connectionEstablished=false;
            long waitBeforeRetry = 1000;
            long delayMultiplier = 2;
            long retries = 0;
            long maxRetries= 10;
            while(!connectionEstablished) {
                try {
                    rpcClient.getRpcProxy();
                    connectionEstablished = true;
                } catch (CordaConnectionException e) {
                    logger.info("Failed to connect to corda on EventStreamService startup. retrying attempt {}", retries, e);
                }
                if(retries == maxRetries) {
                    logger.error("Failed to connect to corda on EventStreamService startup after {} attempts, exiting.", maxRetries);
                    System.exit(1);
                }
                waitBeforeRetry *= delayMultiplier;
                retries++;
                if(!connectionEstablished) {
                    try {
                        Thread.sleep(waitBeforeRetry);
                    } catch (InterruptedException e) {
                        logger.error("Thread interrupted while waiting before retrying.", e);
                    }
                }
            }
            // setup observer for corda rpc connection changes
            rpcClient.getRpcConnectionObservable().subscribe((isConnected) -> {
                if(isConnected) onCordaRpcConnect();
                else onCordaRpcDisconnect();
            });
            restoreStreamsFromDb();
            restoreSubscriptionsFromDb();
        });
    }

    private void restoreStreamsFromDb() {
        logger.info("Restoring streams from database.");
        eventStreamRepository.findAllStream().forEach(this::_createOrUpdateStream);
    }
    private void restoreSubscriptionsFromDb() {
        logger.info("Restoring subscriptions from database.");
        subscriptionRepository.findAllSubscriptions().forEach(this::_createOrResetSubscription);
    }

    private void onCordaRpcDisconnect() {
        // pause all subscriptions
        logger.info("Corda rpc disconnect detected, pausing subscriptions.");
        subscriptions.forEachValue(5, Subscription::pauseSubscription);
    }

    private void onCordaRpcConnect() {
        // resume all subscriptions
        logger.info("Corda rpc reconnect detected, resuming subscriptions.");
        subscriptions.forEachValue(5, subscription -> {
            if(subscription.isPaused()) {
                subscription.resumeSubscription();
            }
        });
    }

    private EventStream getEventStreamByTopic(String topic) throws Exception {
        String eventStreamId = this.topicToEventStreamId.get(topic);
        EventStream stream = this.streams.get(eventStreamId);
        if(stream == null) {
            throw new Exception("No stream with topic "+topic+" exists", null);
        }
        return stream;
    }

    private void checkPointSubscription(Subscription sub) {
        SubscriptionInfo subscriptionInfo = sub.getSubscriptionInfo();
        logger.info("{} Persisting current HWM {}", subscriptionInfo.getId(), sub.getTimeHWM());
        Timestamp lastCheckpoint = subscriptionInfo.getLastCheckpoint();
        try {
            if(lastCheckpoint == null || sub.getTimeHWM() != null && sub.getTimeHWM().isAfter(lastCheckpoint.toInstant())) {
                subscriptionInfo.setLastCheckpoint(Timestamp.from(sub.getTimeHWM()));
                subscriptionRepository.save(subscriptionInfo);
            }
        } catch (Exception e) {
            logger.error("{} failed to persist current HWM {}", subscriptionInfo.getId(), sub.getTimeHWM(), e);
            subscriptionInfo.setLastCheckpoint(lastCheckpoint);
        }
    }

    private void handleAck(EventStream stream) {
        logger.debug("Handling ack for the eventstream {}", stream.getEventStreamInfo().getId());
        Map<String, Instant> checkpointMap = stream.getLatestCheckpoints();
        checkpointMap.forEach((subscriptionId, checkpoint) -> {
            Subscription subscription = this.subscriptions.get(subscriptionId);
            if(subscription != null) {
                logger.debug("Subscription {}, checkpointing", subscriptionId);
                subscription.setTimeHWM(checkpoint);
                checkPointSubscription(subscription);
            } else {
                logger.debug("Subscription {} not found, skipping checkpointing", subscriptionId);
            }
        });
        stream.onAck();
    }

    public void addWebSocketConnection(WebSocketConnection connection) {
        this.webSocketConnectionMap.put(connection.getId(), connection);
    }

    public void removeWebSocketConnection(String connectionId) {
        //Just Removes connections from service
        //closed connections from eventstream object are removed lazily, (when trying to push events)
        this.webSocketConnectionMap.remove(connectionId);
    }

    public void onWebSocketMessage(String connectionId, ClientMessage message) {
        scheduler.submit(() ->{
            WebSocketConnection conn = webSocketConnectionMap.get(connectionId);
            String topic = message.getTopic();
            EventStream eventStream = null;
            try {
                eventStream = getEventStreamByTopic(topic);
            } catch (Exception e) {
                logger.error("Received message from client {} on topic {} for which eventstream doesn't exists", connectionId, topic, e);
                return;
            }
            switch(message.getType()) {
                case LISTEN:
                    // add connection to eventstream, eventstream will push events to conn as they are available
                    eventStream.addStreamConsumer(conn);
                    break;
                case ACK:
                    handleAck(eventStream);
                    break;
                case ERROR:
                    eventStream.onError(conn.getId());
                    break;
            }
        });
    }

    private void _createOrUpdateStream(EventStreamInfo eventStreamInfo) {
        EventStream eventStream = this.streams.get(eventStreamInfo.getId());
        if(eventStream == null) {
            eventStream = new EventStream(eventStreamInfo);
        } else {
            eventStream.setEventStreamInfo(eventStreamInfo);
        }
        this.streams.put(eventStreamInfo.getId(), eventStream);
        this.topicToEventStreamId.put(eventStreamInfo.getWebsocketTopic(), eventStreamInfo.getId());
    }

    private void _suspendStream(String eventstreamId) {
        EventStream eventStream = this.streams.get(eventstreamId);
        if(eventStream != null) {
            // when stream is suspended, checkpoint and pause all subscriptions that push events to the stream
            Map<String, Instant> checkpointMap = eventStream.getLatestCheckpoints();
            checkpointMap.forEach((subscriptionId, checkpoint) -> {
                Subscription subscription = this.subscriptions.get(subscriptionId);
                if(subscription != null) {
                    subscription.setTimeHWM(checkpoint);
                    checkPointSubscription(subscription);
                    subscription.pauseSubscription();
                }
            });
            eventStream.onSuspend();
        }

    }

    private void _resumeStream(String eventstreamId) {
        EventStream eventStream = this.streams.get(eventstreamId);
        if(eventStream != null) {
            eventStream.onResume();
            this.subscriptions.forEachValue(5, subscription -> {
                if(subscription.getStream().getEventStreamInfo().getId().equalsIgnoreCase(eventstreamId)) subscription.resumeSubscription();
            });
        }
    }

    private void _deleteSubscription(String subscriptionId) {
        // clean up subscription runtime
        Subscription subscription = this.subscriptions.get(subscriptionId);
        if(subscription != null) {
            subscription.pauseSubscription();
            this.subscriptions.remove(subscriptionId);
        }
    }

    private void _deleteStream(String eventstreamId) {
        // clean up stream runtime
        EventStream stream = this.streams.get(eventstreamId);
        if(stream != null) {
            stream.onSuspend();
            this.streams.remove(eventstreamId);
        }
    }

    @Transactional
    public EventStreamInfo createEventStream(EventStreamData streamData) {
        String id = "es-"+ UUID.randomUUID().toString();
        Optional<EventStreamInfo> existingStream = eventStreamRepository.findById(id);
        if(existingStream.isPresent()) {
            return createEventStream(streamData);
        } else {
            EventStreamInfo newStream = new EventStreamInfo(id, streamData);
            logger.info("Inserting eventstream '{}' with name {}", newStream.getId(), newStream.getName());
            eventStreamRepository.save(newStream);
            EventStreamInfo createdStream = eventStreamRepository.findById(id).orElseThrow();
            _createOrUpdateStream(createdStream);
            return createdStream;
        }
    }

    public List<EventStreamInfo> listEventStreams() {
        return eventStreamRepository.findAllStream().collect(Collectors.toList());
    }

    public EventStreamInfo getEventStreamById(String eventStreamId) {
        Optional<EventStreamInfo> existingStream = eventStreamRepository.findById(eventStreamId);
        return existingStream.orElseThrow();
    }

    @Transactional
    public EventStreamInfo updateEventStream(String eventStreamId, EventStreamData updatedStreamData) {
        Optional<EventStreamInfo> existingStream = eventStreamRepository.findById(eventStreamId);
        if(existingStream.isEmpty()) {
            throw new RuntimeException();
        } else {
            EventStreamInfo eventStreamInfo = existingStream.get();
            if(!updatedStreamData.getName().equals(eventStreamInfo.getName())) {
                eventStreamInfo.setName(updatedStreamData.getName());
            }
            eventStreamInfo.setWebsocketTopic(updatedStreamData.getWebsocket().getTopic());
            if(updatedStreamData.getBatchSize() != eventStreamInfo.getBatchSize()){
                eventStreamInfo.setBatchSize(updatedStreamData.getBatchSize());
            }
            if(updatedStreamData.getBatchTimeoutMS() != eventStreamInfo.getBatchTimeoutMs()) {
                eventStreamInfo.setBatchTimeoutMs(updatedStreamData.getBatchTimeoutMS());
            }
            if(updatedStreamData.getBlockedRetryDelaySec() != eventStreamInfo.getBatchRetryDelaySec()) {
                eventStreamInfo.setBatchRetryDelaySec(updatedStreamData.getBlockedRetryDelaySec());
            }
            if(updatedStreamData.getErrorHandling() != eventStreamInfo.getErrorHandling()) {
                eventStreamInfo.setErrorHandling(updatedStreamData.getErrorHandling());
            }
            eventStreamRepository.save(eventStreamInfo);
            EventStreamInfo updatedStream = eventStreamRepository.findById(eventStreamId).orElseThrow();
            _createOrUpdateStream(updatedStream);
            return updatedStream;
        }
    }

    @Transactional
    public boolean suspendEventStream(String eventStreamId){
        Optional<EventStreamInfo> existingStream = eventStreamRepository.findById(eventStreamId);
        if(existingStream.isEmpty()) {
            logger.error("Failed to suspend stream {} which doesn't exists.", eventStreamId);
            throw new NoSuchElementException(String.format("EventStream with id %s not found", eventStreamId));
        } else {
            boolean suspendRequired = false;
            EventStreamInfo eventStreamInfo = existingStream.get();
            if(!eventStreamInfo.isSuspended()) {
                logger.info("Persisting stream {} with suspended status {}", eventStreamId, true);
                eventStreamInfo.setSuspended(true);
                eventStreamRepository.save(eventStreamInfo);
                suspendRequired = true;
            }
            if(suspendRequired) _suspendStream(eventStreamId);
            return true;
        }
    }

    @Transactional
    public boolean resumeEventStream(String eventStreamId) {
        Optional<EventStreamInfo> existingStream = eventStreamRepository.findById(eventStreamId);
        if(existingStream.isEmpty()) {
            logger.error("Failed to resume stream {} which doesn't exists.", eventStreamId);
            throw new NoSuchElementException(String.format("EventStream with id %s not found", eventStreamId));
        } else {
            boolean resumeRequired = false;
            EventStreamInfo eventStreamInfo = existingStream.get();
            if(eventStreamInfo.isSuspended()) {
                logger.info("Persisting stream {} with suspended status {}", eventStreamId, false);
                eventStreamInfo.setSuspended(false);
                eventStreamRepository.save(eventStreamInfo);
                resumeRequired = true;
            }
            if(resumeRequired) _resumeStream(eventStreamId);
            return true;
        }
    }

    @Transactional
    public boolean deleteEventStream(String eventStreamId) {
        Optional<EventStreamInfo> existingStream = eventStreamRepository.findById(eventStreamId);
        if(existingStream.isEmpty()) {
            logger.error("Failed to delete stream {} which doesn't exists.", eventStreamId);
            throw new NoSuchElementException(String.format("EventStream with id %s not found", eventStreamId));
        } else {
            Stream<String> subscriptionsToDelete = subscriptionRepository.listSubscriptionIdsByEventStreamId(eventStreamId);
            // clean subscription runtimes
            subscriptionsToDelete.forEach(this::deleteSubscription);
            // clean subscriptions from db
            subscriptionRepository.deleteInBulkByEventStreamId(eventStreamId);
            // delete eventstream runtime
            _deleteStream(eventStreamId);
            // delete eventstream from db
            eventStreamRepository.delete(existingStream.get());
            return true;
        }
    }

    private void _createOrResetSubscription(SubscriptionInfo subscriptionInfo) {
        Subscription subscription = this.subscriptions.get(subscriptionInfo.getId());
        if(subscription == null) {
            // create request, create runtime
            logger.info("Creating subscription runtime for {}", subscriptionInfo.getId());
            subscription = new Subscription(subscriptionInfo);
            subscription.setStream(this.streams.get(subscriptionInfo.getStream().getId()));
            this.subscriptions.put(subscriptionInfo.getId(),subscription);
        } else {
            // reset request, reset HWM to get events from the start
            subscription.setSubscriptionInfo(subscriptionInfo);
            subscription.setTimeHWM(null);
        }
        subscription.initializeOrReset();
    }

    @Transactional
    public SubscriptionInfo createSubscription(SubscriptionData subscriptionData) {
        String id = "sb-"+ UUID.randomUUID().toString();
        Optional<SubscriptionInfo> existingSub = subscriptionRepository.findById(id);
        if(existingSub.isPresent()) {
            return createSubscription(subscriptionData);
        } else {
            EventStreamInfo streamInfo = eventStreamRepository.findById(subscriptionData.getStream()).orElseThrow();
            SubscriptionInfo newSub = new SubscriptionInfo(id, subscriptionData);
            newSub.setStream(streamInfo);
            subscriptionRepository.save(newSub);
            newSub =  subscriptionRepository.findById(id).orElseThrow();
            _createOrResetSubscription(newSub);
            return newSub;
        }
    }

    public SubscriptionInfo getSubscriptionById(String subscriptionId) {
        return subscriptionRepository.findById(subscriptionId).orElseThrow();
    }

    public List<SubscriptionInfo> listSubsciptions(){
        return subscriptionRepository.findAllSubscriptions().collect(Collectors.toList());
    }

    @Transactional
    public boolean deleteSubscription(String subscriptionId) {
        Optional<SubscriptionInfo> subscriptionInfo = subscriptionRepository.findById(subscriptionId);
        if(subscriptionInfo.isEmpty()) {
            logger.error("Failed to delete subscription {} which doesn't exists.", subscriptionId);
            throw new NoSuchElementException(String.format("Subscription with id %s not found", subscriptionId));
        } else {
            // delete subscription runtime
            _deleteSubscription(subscriptionId);
            subscriptionRepository.deleteById(subscriptionId);
            return true;
        }
    }

    @Transactional
    public boolean resetSubscription(String subscriptionId) {
        Optional<SubscriptionInfo> subscriptionInfo = subscriptionRepository.findById(subscriptionId);
        if(subscriptionInfo.isEmpty()) {
            logger.error("Failed to reset subscription {} which doesn't exists.", subscriptionId);
            throw new NoSuchElementException(String.format("Subscription with id %s not found", subscriptionId));
        } else {
            SubscriptionInfo subInfo = subscriptionInfo.get();
            subInfo.setFromTime(null);
            subscriptionRepository.save(subInfo);
            subInfo = subscriptionRepository.findById(subscriptionId).orElseThrow();
            _createOrResetSubscription(subInfo);
            return true;
        }
    }
}
