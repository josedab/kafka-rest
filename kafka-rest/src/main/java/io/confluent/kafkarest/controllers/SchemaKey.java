/*
 * Copyright 2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafkarest.controllers;

import com.google.auto.value.AutoValue;
import java.util.Optional;

/**
 * A key for caching schema lookups. This key uniquely identifies a schema request based on the
 * parameters used to fetch it.
 */
@AutoValue
abstract class SchemaKey {

  abstract String topicName();

  abstract Optional<String> subject();

  abstract Optional<Integer> schemaId();

  abstract Optional<Integer> schemaVersion();

  abstract boolean isKey();

  static SchemaKey create(
      String topicName,
      Optional<String> subject,
      Optional<Integer> schemaId,
      Optional<Integer> schemaVersion,
      boolean isKey) {
    return new AutoValue_SchemaKey(topicName, subject, schemaId, schemaVersion, isKey);
  }
}
