"use strict";
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// src/index.ts
var index_exports = {};
__export(index_exports, {
  Bucketsd: () => Bucketsd
});
module.exports = __toCommonJS(index_exports);
var import_kafka_javascript = require("@confluentinc/kafka-javascript");
var import_lmdb = require("lmdb");
var import_buffer = require("buffer");
var import_buckets = require("buckets");
var import_events = require("events");

// src/util.ts
function getTopicsMetadata(metadata, topics) {
  return metadata.topics.filter((metaTopic) => {
    return topics.some((topicFilter) => {
      if (typeof topicFilter === "string") {
        return metaTopic.name === topicFilter;
      } else if (topicFilter instanceof RegExp) {
        return topicFilter.test(metaTopic.name);
      }
      return false;
    });
  });
}
function getMetadata(consumer, options) {
  return new Promise(
    (resolve, reject) => consumer.getMetadata(
      options,
      (err, metadata) => err ? reject(err) : resolve(metadata)
    )
  );
}
function getAssigments(metadata, topics) {
  const assigments = getTopicsMetadata(metadata, topics).flatMap(
    (metaTopic) => metaTopic.partitions.map((partition) => ({
      topic: metaTopic.name,
      partition: partition.id,
      offset: 0
      // Change this offset if you want to start elsewhere
    }))
  );
  return assigments;
}

// src/index.ts
var Bucketsd = class {
  constructor(options) {
    this.initialized = false;
    this.watermarks = /* @__PURE__ */ new Map();
    this.topicOffsets = /* @__PURE__ */ new Map();
    const { createTopicOptions, appId, brokers, store, consumer, producer } = options;
    this.topic = createTopicOptions ? createTopicOptions.topic : `${appId}.kv`;
    this.appId = appId;
    this.createTopicOptions = createTopicOptions;
    this.store = new import_buckets.Store(`db/${appId}`, { cache: false, ...store });
    this.store.on("change", (ev) => this.handleStoreChange(ev));
    const brokerList = Array.isArray(brokers) ? brokers.join(",") : "localhost:19092";
    this.consumer = new import_kafka_javascript.KafkaConsumer(
      {
        "group.id": "kv",
        "bootstrap.servers": brokerList,
        "enable.auto.commit": false,
        "enable.auto.offset.store": false,
        ...consumer?.global
      },
      { "auto.offset.reset": "beginning", ...consumer?.topic || {} }
    );
    this.producer = new import_kafka_javascript.Producer(
      {
        "bootstrap.servers": brokerList,
        ...producer?.global
      },
      { ...producer?.topic }
    );
    this.producer.setPollInterval(100);
  }
  handleStoreChange(ev) {
    const { op, bucket, id, value, ttl } = ev;
    const headers = [];
    if (op) headers.push({ op });
    if (bucket) headers.push({ bucket });
    if (ttl) headers.push({ ttl });
    const messageValue = op === "remove" ? null : import_buffer.Buffer.isBuffer(value) ? value : import_buffer.Buffer.from(value);
    try {
      this.producer.produce(
        this.topic,
        null,
        messageValue,
        id,
        Date.now(),
        void 0,
        headers
      );
    } catch (err) {
      const error = err;
      if (import_kafka_javascript.CODES.ERRORS.ERR__QUEUE_FULL === error.code) {
        console.log("Queue full, re-buffering message");
        this.producer.poll();
      } else {
        throw err;
      }
    }
  }
  queryWatermark(partition) {
    return new Promise((resolve, reject) => {
      this.consumer.queryWatermarkOffsets(
        this.topic,
        partition,
        5e3,
        (err, offsets) => {
          if (err) return reject(err);
          console.log("queryWatermarkOffsets", { partition, offsets });
          this.watermarks.set(partition, Number(offsets.highOffset));
          this.topicOffsets.set(partition, -1);
          resolve();
        }
      );
    });
  }
  async setupPartitionStatus(assignments) {
    await Promise.all(
      assignments.map(({ partition }) => this.queryWatermark(partition))
    );
    console.log("Partition status initialized:", this.watermarks);
  }
  async start() {
    this.consumer.setDefaultConsumeTimeout(1e3);
    this.consumer.connect();
    this.producer.connect();
    await Promise.all([
      (0, import_events.once)(this.consumer, "ready"),
      (0, import_events.once)(this.producer, "ready")
    ]);
    const admin = import_kafka_javascript.AdminClient.createFrom(this.consumer);
    admin.createTopic(
      {
        num_partitions: 5,
        replication_factor: 3,
        config: {
          "segment.ms": "300000",
          "segment.bytes": "102400",
          "cleanup.policy": "compact"
        },
        ...{ ...this.createTopicOptions, topic: this.topic }
      },
      async (err) => {
        if (err && err.code !== import_kafka_javascript.CODES.ERRORS.ERR_TOPIC_ALREADY_EXISTS) {
          throw err;
        }
        const metadata = await getMetadata(this.consumer, {});
        const assignments = getAssigments(metadata, [this.topic]);
        this.consumer.assign(assignments);
        await this.setupPartitionStatus(assignments);
        this.consumer.consume();
        this.consumer.on("data", (message) => this.handleConsumerData(message));
      }
    );
    await this.waitUntilCaughtUp();
  }
  handleConsumerData(message) {
    this.topicOffsets.set(message.partition, message.offset);
    const headers = message.headers?.reduce(
      (obj, h) => {
        const key = Object.keys(h)[0];
        obj[key] = h[key].toString();
        return obj;
      },
      {}
    );
    if (message.key && headers?.bucket && headers?.op) {
      const kv = this.store.bucket(headers.bucket);
      if (headers.op === "put" && message.value) {
        kv.put(message.key.toString(), (0, import_lmdb.asBinary)(message.value), {
          quiet: true
        });
      } else {
        kv.remove(message.key.toString(), { quiet: true });
      }
    }
  }
  waitUntilCaughtUp() {
    return new Promise((resolve) => {
      const interval = setInterval(() => {
        if (this.hasCaughtUp()) {
          clearInterval(interval);
          console.log("All partitions caught up. KV is ready.");
          this.initialized = true;
          resolve();
        }
      }, 1e3);
    });
  }
  hasCaughtUp() {
    return Array.from(this.watermarks.entries()).every(
      ([partition, watermark]) => {
        const current = this.topicOffsets.get(partition);
        return current !== void 0 && current >= watermark - 1;
      }
    );
  }
  async stop() {
    this.consumer.unsubscribe();
    this.consumer.pause(this.consumer.assignments());
    this.producer.flush(5e3, (err) => {
      console.log(err);
      this.producer.disconnect();
      this.consumer.disconnect();
    });
    await Promise.all(
      [this.consumer, this.producer].map(
        (client) => (0, import_events.once)(client, "disconnected")
      )
    );
  }
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  Bucketsd
});
//# sourceMappingURL=index.js.map