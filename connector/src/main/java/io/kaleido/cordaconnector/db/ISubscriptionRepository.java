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

package io.kaleido.cordaconnector.db;

import io.kaleido.cordaconnector.db.entity.SubscriptionInfo;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.stream.Stream;

@Repository
public interface ISubscriptionRepository extends JpaRepository<SubscriptionInfo, String> {

    @Query("select id from SubscriptionInfo sub where sub.stream.id = ?1")
    Stream<String> listSubscriptionIdsByEventStreamId(String eventStreamId);

    @Query("select s from SubscriptionInfo s order by s.id asc")
    Stream<SubscriptionInfo> findAllSubscriptions();

    @Modifying
    @Query("delete from SubscriptionInfo sub where sub.stream.id = ?1")
    void deleteInBulkByEventStreamId(String eventStreamId);
}
