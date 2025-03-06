import {
  KafkaConsumer,
  ConsumerGlobalConfig,
  ConsumerTopicConfig,
  Producer,
  ProducerGlobalConfig,
  ProducerTopicConfig,
  Assignment,
  Message,
} from "@confluentinc/kafka-javascript";
import { RootDatabaseOptions } from "lmdbx";
import { Buffer } from "buffer";
import { Store } from "buckets";
import { once } from "events";
import { getAssigments, getMetadata } from "./util";

type Options = {
  topic: string;
  appId: string;
  brokers: string[];
  store?: RootDatabaseOptions;
  consumer?: {
    global: ConsumerGlobalConfig;
    topic?: ConsumerTopicConfig;
  };
  producer?: {
    global: ProducerGlobalConfig;
    topic?: ProducerTopicConfig;
  };
};

export class Bucketsd {
  topic: string;
  appId: string;
  consumer: KafkaConsumer;
  producer: Producer;
  initialized = false;
  store: Store;
  watermarks = new Map<number, number>();
  topicOffsets = new Map<number, number>();

  constructor(options: Options) {
    const { topic, appId, brokers, store, consumer, producer } = options;
    this.topic = topic;
    this.appId = appId;

    this.store = new Store(`${appId}:buckets`, { cache: true, ...store });
    this.store.on("change", (ev) => this.handleStoreChange(ev));

    const brokerList = Array.isArray(brokers)
      ? brokers.join(",")
      : "localhost:19092";

    this.consumer = new KafkaConsumer(
      {
        "group.id": "kv",
        "bootstrap.servers": brokerList,
        "enable.auto.commit": false,
        "enable.auto.offset.store": false,
        ...consumer?.global,
      },
      { "auto.offset.reset": "beginning", ...(consumer?.topic || {}) }
    );

    this.producer = new Producer(
      {
        "bootstrap.servers": brokerList,
        ...producer?.global,
      },
      { ...producer?.topic }
    );
  }

  private handleStoreChange(ev: {
    op: string;
    bucket: string;
    id: string;
    value: string | Buffer;
    ttl?: string;
  }) {
    const { op, bucket, id, value, ttl } = ev;
    console.log(value, JSON.parse(value as string));

    // Construct headers array
    const headers = [];
    if (op) headers.push({ op });
    if (bucket) headers.push({ bucket });
    if (ttl) headers.push({ ttl });

    const messageValue =
      op === "remove"
        ? null
        : Buffer.isBuffer(value)
        ? value
        : Buffer.from(value);

    this.producer.produce(
      this.topic,
      null,
      messageValue,
      id,
      Date.now(),
      null,
      headers
    );
  }

  private queryWatermark(partition: number): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.consumer.queryWatermarkOffsets(
        this.topic,
        partition,
        5000,
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

  protected async setupPartitionStatus(assignments: Assignment[]) {
    await Promise.all(
      assignments.map(({ partition }) => this.queryWatermark(partition))
    );
    console.log("Partition status initialized:", this.watermarks);
  }

  async start() {
    this.consumer.setDefaultConsumeTimeout(1000);
    this.consumer.connect();
    this.producer.connect();

    await Promise.all([
      once(this.consumer, "ready"),
      once(this.producer, "ready"),
    ]);

    const metadata = await getMetadata(this.consumer, {});
    const assignments = getAssigments(metadata, [this.topic]);
    this.consumer.assign(assignments);
    await this.setupPartitionStatus(assignments);

    this.consumer.consume();

    this.consumer.on("data", (message) => this.handleConsumerData(message));

    await this.waitUntilCaughtUp();
  }

  private handleConsumerData(message: Message) {
    this.topicOffsets.set(message.partition, message.offset);

    const headers = message.headers?.reduce<Record<string, string>>(
      (obj, h) => {
        const key = Object.keys(h)[0];
        obj[key] = h[key].toString();
        return obj;
      },
      {}
    );

    if (message.key && headers?.bucket && headers?.op) {
      const kv = this.store.bucket(headers.bucket, { encoding: "json" });
      if (headers.op === "put") {
        kv.put(message.key.toString(), message.value, { quiet: true });
      } else {
        kv.remove(message.key.toString(), { quiet: true });
      }
    }
  }

  private waitUntilCaughtUp(): Promise<void> {
    return new Promise((resolve) => {
      const interval = setInterval(() => {
        if (this.hasCaughtUp()) {
          clearInterval(interval);
          console.log("All partitions caught up. KV is ready.");
          this.initialized = true;
          resolve();
        }
      }, 1000);
    });
  }

  hasCaughtUp() {
    return Array.from(this.watermarks.entries()).every(
      ([partition, watermark]) => {
        const current = this.topicOffsets.get(partition);
        return current !== undefined && current >= watermark - 1;
      }
    );
  }

  async stop() {
    this.consumer.unsubscribe();
    this.consumer.pause(this.consumer.assignments());

    this.producer.disconnect();
    this.consumer.disconnect();

    await Promise.all(
      [this.consumer, this.producer].map((client) =>
        once(client, "disconnected")
      )
    );
  }
}
