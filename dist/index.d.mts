import { KafkaConsumer, Producer, NewTopic, ConsumerGlobalConfig, ConsumerTopicConfig, ProducerGlobalConfig, ProducerTopicConfig, Assignment } from '@confluentinc/kafka-javascript';
import { RootDatabaseOptions } from 'lmdbx';
import { Store } from 'buckets';

type Options = {
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
    createTopicOptions?: NewTopic;
};
declare class Bucketsd {
    topic: string;
    appId: string;
    consumer: KafkaConsumer;
    producer: Producer;
    initialized: boolean;
    store: Store;
    watermarks: Map<number, number>;
    topicOffsets: Map<number, number>;
    createTopicOptions?: NewTopic;
    constructor(options: Options);
    private handleStoreChange;
    private queryWatermark;
    protected setupPartitionStatus(assignments: Assignment[]): Promise<void>;
    start(): Promise<void>;
    private handleConsumerData;
    private waitUntilCaughtUp;
    hasCaughtUp(): boolean;
    stop(): Promise<void>;
}

export { Bucketsd };
