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

import io.kaleido.cordaconnector.db.entity.EventStreamInfo;
import io.kaleido.cordaconnector.model.common.ErrorHandling;
import io.kaleido.cordaconnector.model.common.Event;
import io.kaleido.cordaconnector.rpc.NodeRPCClient;
import io.kaleido.cordaconnector.ws.WebSocketConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.websocket.EncodeException;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class EventStream {
    public static final int MAX_BATCH_SIZE = 1000;
    @Autowired
    private NodeRPCClient rpcClient;
    private EventStreamInfo eventStreamInfo;
    // Information about eventstream that is persisted to database
    private static final Logger log = LoggerFactory.getLogger(EventStream.class);
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    // Ongoing batch thread should not compete with other tasks, so we have a separate scheduler for the inFlightTask
    private final ScheduledExecutorService inFlightScheduler = Executors.newSingleThreadScheduledExecutor();
    // Batch timeout task is used for batch processing,
    // It is canceled and rescheduled to execute after batchTimeout time, whenever new batch of events begin processing
    // If executed, push whatever batch size we have available to a client
    private ScheduledFuture<?> batchTimeoutTask;
    // Task to keep track of ongoing push of current batch to a client with retries
    private ScheduledFuture<?> inFlightTask;
    // thread safe buffer of batches, ready to be pushed to clients
    // TODO set buffer size limit?
    private BlockingQueue<List<Event<?>>> eventBatchBuffer;
    // active client connections consuming batches of events,
    // events are pushed to clients in round robin fashion.
    // an eventstream can have multiple consumers for load-balancing reason
    private BlockingQueue<WebSocketConnection> activeConnections;
    // current batch of events, it is added to buffer whenever batch size is reached
    // or batchTimeout expires, whichever happens 1st
    private BlockingQueue<Event<?>> currentBatch;
    // Semaphore used to make sure batches of events are pushed one by one
    // Acquired by a thread when it starts attempts with retries to push a batch to the client
    // Released when a ack is received, or when we have SKIP error handling and an error is received
    // Consumer application is responsible for not sending duplicate acks
    private Semaphore inFlightLock;

    public long getBatchCount() {
        return batchCount.get();
    }

    // Volatile, used for debug logging
    private AtomicLong batchCount = new AtomicLong(0);
    // Map of subscription id to acked hwm.
    private ConcurrentHashMap<String, Instant> subscriptionCheckpoints;

    public EventStream(EventStreamInfo eventStreamInfo) {
        this.eventStreamInfo = eventStreamInfo;
        this.currentBatch = new LinkedBlockingQueue<>();
        this.eventBatchBuffer = new LinkedBlockingQueue<>();
        this.activeConnections = new LinkedBlockingQueue<>();
        this.inFlightLock = new Semaphore(1);
        this.subscriptionCheckpoints = new ConcurrentHashMap<>();
    }

    public EventStreamInfo getEventStreamInfo() {
        return eventStreamInfo;
    }

    public void setEventStreamInfo(EventStreamInfo eventStreamInfo) {
        this.eventStreamInfo = eventStreamInfo;
    }

    public void addStreamConsumer(WebSocketConnection connection) {
        log.info("{}, Adding stream consumer client with connection id {}", eventStreamInfo.getId(), connection.getId());
        activeConnections.add(connection);
        _dispatchInQueueBatches();
    }

    public Map<String, Instant> getLatestCheckpoints() {
        Map<String, Instant> checkpoints = Map.copyOf(this.subscriptionCheckpoints);
        return checkpoints;
    }

    private void _dispatchInQueueBatches() {
        // if there are batches in buffer waiting to be delivered, start dispatch if no new batch is in process
        if(this.eventBatchBuffer.isEmpty() ) {
            // cancel any running batch retries as we received the ack
            // when there are events in the buffer, next batch will cancel previous retry task;
            cancelInFlightBatch();
        }
        // dispatch next batch in the buffer
        scheduler.submit(this::dispatchBatch);
    }

    public void onAck() {
        log.debug("{} ack received. releasing in flight lock to allow next batch push.", eventStreamInfo.getId());
        if(!eventBatchBuffer.isEmpty()) eventBatchBuffer.remove();
        inFlightLock.release();
        _dispatchInQueueBatches();
    }

    public void onError(String connectionId) {
        log.debug("{} received error on connection with id: {}", eventStreamInfo.getId(), connectionId);
        if(eventStreamInfo.getErrorHandling() == ErrorHandling.SKIP) {
            log.debug("{} SKIP error handler, releasing in flight lock to allow next batch push.", eventStreamInfo.getId());
            if(!eventBatchBuffer.isEmpty()) eventBatchBuffer.remove();
            inFlightLock.release();
            _dispatchInQueueBatches();
        } else {
            log.debug("{} BLOCK error handler", eventStreamInfo.getId());
        }
    }

    public void clearCheckpoints() {
        log.info("{} clearing checkpoints for all subscriptions", eventStreamInfo.getId());
        subscriptionCheckpoints.clear();
    }

    public void onSuspend() {
        // cancel any ongoing batch push, release locks and clear buffers.
        // subscriptions will take care of restarting from appropriate time based on persisted checkpoints
        log.info("{} suspending stream", eventStreamInfo.getId());
        eventStreamInfo.setSuspended(true);
        if(inFlightTask != null) inFlightTask.cancel(true);
        inFlightLock.release();
        eventBatchBuffer.clear();
        currentBatch.clear();
    }

    public void onResume() {
        log.info("{} Resuming stream", eventStreamInfo.getId());
        eventStreamInfo.setSuspended(false);
    }

    private void onBatchTimeoutExpiry() {
        // batch timeout expired, we should push if push the new batch if we have some events
        log.trace("{} batch timeout expired.", eventStreamInfo.getId());
        if(currentBatch.size() > 0) {
            log.debug("{} On batchTimeout expiry, pushing new batch with {} events, batch size {}", eventStreamInfo.getId(), currentBatch.size(), eventStreamInfo.getBatchSize());
            onBatchComplete();
        } else if(eventBatchBuffer.size() > 0) {
            scheduler.submit(this::dispatchBatch);
        }
    }

    private void onBatchComplete() {
        log.trace("{} On Batch completed.",eventStreamInfo.getId());
        List<Event<?>> batch = new ArrayList<>();
        currentBatch.drainTo(batch);
        if(batch.size() > 0) {
            eventBatchBuffer.add(batch);
            log.debug("{} On Batch completed. sending new batch",eventStreamInfo.getId());
            scheduler.submit(this::dispatchBatch);
        }
    }

    /*
     * Used by Upstream subscription to push events as they arrive. New events are ignored of stream is suspended.
     * Triggers the batch processing logic, i.e., adds the event to current batch or start a batchtimeout timer if
     * starting a new batch or starts dispatch if current batch is complete. Blocks if current batch is full and not
     * dispatched.
     * */
    public void pushNewEvent(Event<?> vaultEvent) {
        log.debug("{} Received new event for batch processing",eventStreamInfo.getId());
        if(eventStreamInfo.isSuspended()) {
            log.debug("{} Ignoring event as stream is suspended",eventStreamInfo.getId());
            return;
        }
        if(currentBatch.size() == 0) {
            log.debug("{} Starting new batch of event and batchTimeout task",eventStreamInfo.getId());
            batchTimeoutTask = scheduler.schedule(() -> onBatchTimeoutExpiry(), eventStreamInfo.getBatchTimeoutMs(), TimeUnit.MILLISECONDS);
        }

        if(currentBatch.size() < eventStreamInfo.getBatchSize()-1) {
            log.debug("{} current batch size {}, batch size {}",eventStreamInfo.getId(), currentBatch.size()+1, eventStreamInfo.getBatchSize());
            currentBatch.add(vaultEvent);
        } else if(currentBatch.size() == eventStreamInfo.getBatchSize()-1){
            batchTimeoutTask.cancel(true);
            currentBatch.add(vaultEvent);
            log.debug("{} Batch completed. cancelling batchTimeoutTask, size {}",eventStreamInfo.getId(), currentBatch.size());
            onBatchComplete();
        } else {
            // batch is full, queue add task
            log.debug("{} Batch full. blocking until batch is queued for dispatch", eventStreamInfo.getId());
            onBatchComplete();
            pushNewEvent(vaultEvent);
        }
    }



    /*
     *  Attempts to send a batch from queued batches. Inflight lock ensures that only one batch (front of queue) is in-flight at a time.
     *  After acquiring inflight lock, we cancel old inflight task if it is running. Update future checkpoints of subscriptions (values that will be checkpointed
     *  when ack is received for this eventstream. The inflihgt lock is released upon receipt of an ack or an error (if SKIP errorhandling) and front of batch buffer is removed,
     *  to allow next batch of event to be disptached. The inflight lock can also be released if there are no batches to send or eventstream is suspended or
     *  no active consumers of the stream.
     * */

    private void dispatchBatch(){
        log.debug("{} Start dispatching new batch",eventStreamInfo.getId());
        try {
            log.trace("{} acquiring lock for batch {}...", eventStreamInfo.getId(), batchCount.get()+1);
            inFlightLock.acquire();
            log.trace("{} acquired lock for batch {}...", eventStreamInfo.getId(), batchCount.get()+1);
        } catch (InterruptedException e) {
            log.error("{}, error while sending batch {}", eventStreamInfo.getId(), batchCount.get()+1, e);
        }
        if (inFlightTask != null && !inFlightTask.isCancelled()) {
            log.debug("{} cancelling old inflight task", eventStreamInfo.getId());
            inFlightTask.cancel(true);
        }
        if (eventStreamInfo.isSuspended()) {
            log.debug("{} is suspended, abort send of batch {}", eventStreamInfo.getId(), batchCount.get()+1);
            inFlightLock.release();
            return;
        }
        if(this.activeConnections.isEmpty()) {
            log.debug("{} Aborting dispatch of batch {}, no active consumers of the stream", eventStreamInfo.getId(), batchCount.get()+1);
            inFlightLock.release();
            return;
        }
        if(this.eventBatchBuffer.isEmpty()) {
            log.debug("{} Aborting dispatch, no batch available to dispatch", eventStreamInfo.getId());
            inFlightLock.release();
            return;
        }
        attemptBatchWithRetry(this.eventBatchBuffer.peek(), batchCount.incrementAndGet());
    }

    private void _updateCheckpoint(String subscriptionId, Instant time){
        Instant oldCheckpoint = subscriptionCheckpoints.get(subscriptionId);
        if(oldCheckpoint == null || time.isAfter(oldCheckpoint)) {
            log.info("{} updating checkpoint for the subscription {}, new {}, old {}", eventStreamInfo.getId() ,subscriptionId, time, oldCheckpoint);
            subscriptionCheckpoints.put(subscriptionId, time);
        }
    }

    /*
     * Next batch attempt clears previous inflight task. If there are no more batches to send, this task can be queued on receipt of ack
     * or error (SKIP handling) to cancel inflight task. We simply release the lock after cancelling the task to allow new batch of events.
     * */
    private void cancelInFlightBatch() {
        scheduler.submit(() -> {
            try {
                log.trace("{} acquiring lock for cancelling inflight task...", eventStreamInfo.getId());
                inFlightLock.acquire();
                log.trace("{} acquired lock for cancelling inflight task...", eventStreamInfo.getId());
                if(inFlightTask != null && !inFlightTask.isCancelled()) {
                    log.info("{} cancelling old inflight task", eventStreamInfo.getId());
                    inFlightTask.cancel(true);
                }
                inFlightLock.release();
            } catch (InterruptedException e) {
                log.error("{}, error while cancelling inflight task... {}", eventStreamInfo.getId(), batchCount, e);
            }
        });
    }

    /*
     *  Updates future checkpoints for all subscription based on events in the batch.
     *  Schedules a periodic task (InflightTask) to send data to active consumers of this eventstream,
     *  Inflight task attempts to send data to active consumers after every blockedRetryDelaySec.
     *  Inflight task would be cancelled if ack/ error(SKIP handling) is received from a consumer
     * */
    private void attemptBatchWithRetry(List<Event<?>> batch, long batchCount) {
        log.debug("{} sending batch {} to clients",eventStreamInfo.getId(), batchCount);
        // update checkpoints, before attempting new send
        batch.forEach(event -> {
            Instant update = event.getConsumedTime()!=null?event.getConsumedTime():event.getRecordedTime();
            _updateCheckpoint(event.getSubId(), update);
        });
        log.debug("{} setting new inflight task for batch {}", eventStreamInfo.getId(), batchCount);
        inFlightTask = inFlightScheduler.scheduleWithFixedDelay(() -> {
            //round robin scheduling
            WebSocketConnection connection = null;
            log.trace("{} active connection size {}", eventStreamInfo.getId(), activeConnections.size());
            do {
                // remove closed/inactive connection
                if(this.activeConnections.size() == 0) return;
                connection = this.activeConnections.remove();
            } while (!connection.isOpen());
            //add to back of the queue again.
            this.activeConnections.add(connection);
            log.trace("{} trying connection with id {}", eventStreamInfo.getId(), connection.getId());
            try {
                connection.sendData(batch);
                log.info("Sent batch {} to client {}", batchCount, connection.getId());
            } catch (IOException | EncodeException e ) {
                log.error("{}, error while sending batch {} to client {}", eventStreamInfo.getId(), batchCount, connection.getId(), e);
            }
        }, 100, this.eventStreamInfo.getBatchRetryDelaySec()*1000, TimeUnit.MILLISECONDS);
    }
}
