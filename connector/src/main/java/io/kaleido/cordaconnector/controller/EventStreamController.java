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

package io.kaleido.cordaconnector.controller;
import io.kaleido.cordaconnector.db.entity.EventStreamInfo;
import io.kaleido.cordaconnector.db.entity.SubscriptionInfo;
import io.kaleido.cordaconnector.model.request.ConnectorRequest;
import io.kaleido.cordaconnector.model.response.ConnectorResponse;
import io.kaleido.cordaconnector.model.common.EventStreamData;
import io.kaleido.cordaconnector.model.common.SubscriptionData;
import io.kaleido.cordaconnector.service.EventStreamService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;

@RestController
public class EventStreamController {
    @Autowired
    private EventStreamService eventStreamService;

    @PostMapping("/eventstreams")
    public ConnectorResponse<EventStreamInfo> createEventStream(@RequestBody ConnectorRequest<EventStreamData> request) {
        EventStreamInfo res = eventStreamService.createEventStream(request.getData());
        return new ConnectorResponse<>(res);
    }

    @GetMapping("/eventstreams")
    public ConnectorResponse<List<EventStreamInfo>> listEventStreams() {
        List<EventStreamInfo> res = eventStreamService.listEventStreams();
        return new ConnectorResponse<>(res);
    }

    @GetMapping("/eventstreams/{eventstream_id}")
    public ConnectorResponse<EventStreamInfo> getEventStreamById(@PathVariable("eventstream_id") String eventstreamId) {
        EventStreamInfo res = eventStreamService.getEventStreamById(eventstreamId);
        return new ConnectorResponse<>(res);
    }

    @PatchMapping("/eventstreams/{eventstream_id}")
    public ConnectorResponse<EventStreamInfo> updateEventStream(@PathVariable("eventstream_id") String eventstreamId, @RequestBody ConnectorRequest<EventStreamData> request) {
        EventStreamInfo res = eventStreamService.updateEventStream(eventstreamId, request.getData());
        return new ConnectorResponse<>(res);
    }

    @DeleteMapping("/eventstreams/{eventstream_id}")
    public ConnectorResponse deleteEventStream(@PathVariable("eventstream_id") String eventstreamId) {
        eventStreamService.deleteEventStream(eventstreamId);
        return new ConnectorResponse();
    }

    @PostMapping("/eventstreams/{eventstream_id}/suspend")
    public ConnectorResponse suspendEventStream(@PathVariable("eventstream_id") String eventstreamId) {
        eventStreamService.suspendEventStream(eventstreamId);
        return new ConnectorResponse();
    }

    @PostMapping("/eventstreams/{eventstream_id}/resume")
    public ConnectorResponse resumeEventStream(@PathVariable("eventstream_id") String eventstreamId) {
        eventStreamService.resumeEventStream(eventstreamId);
        return new ConnectorResponse();
    }

    @PostMapping("/subscriptions")
    public ConnectorResponse<SubscriptionInfo> createSubscription(@RequestBody ConnectorRequest<SubscriptionData> request) {
        SubscriptionInfo res = eventStreamService.createSubscription(request.getData());
        return new ConnectorResponse<>(res);
    }

    @GetMapping("/subscriptions")
    public ConnectorResponse<List<SubscriptionInfo>> listSubscriptions() {
        List<SubscriptionInfo> res = eventStreamService.listSubsciptions();
        return new ConnectorResponse<>(res);
    }

    @GetMapping("/subscriptions/{subscription_id}")
    public ConnectorResponse<SubscriptionInfo> getSubscriptionById(@PathVariable("subscription_id") String subscriptionId) {
        SubscriptionInfo res = eventStreamService.getSubscriptionById(subscriptionId);
        return new ConnectorResponse<>(res);
    }

    @DeleteMapping("/subscriptions/{subscription_id}")
    public ConnectorResponse deleteSubscription(@PathVariable("subscription_id") String subscriptionId) {
        eventStreamService.deleteSubscription(subscriptionId);
        return new ConnectorResponse();
    }

    @PostMapping("/subscriptions/{subscription_id}/reset")
    public ConnectorResponse resetSubscription(@PathVariable("subscription_id") String subscriptionId) {
        eventStreamService.resetSubscription(subscriptionId);
        return new ConnectorResponse();
    }
}
