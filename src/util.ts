import {
  Assignment,
  KafkaConsumer,
  Metadata,
  MetadataOptions,
  SubscribeTopicList,
} from "@confluentinc/kafka-javascript";

export function getWatermarkOffsets(
  consumer: KafkaConsumer,
  topic: string,
  partitions: number[],
  timeout = 1000
) {
  const queries = partitions.map((partition) => {
    return new Promise((resolve, reject) => {
      consumer.queryWatermarkOffsets(
        topic,
        partition,
        timeout,
        (err, offsets) => {
          if (err) {
            return reject(err);
          }
          resolve({ partition, highOffset: offsets.highOffset });
        }
      );
    });
  });

  return Promise.all(queries);
}

export function getTopicsMetadata(
  metadata: Metadata,
  topics: SubscribeTopicList
) {
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

export function getMetadata(
  consumer: KafkaConsumer,
  options: MetadataOptions
): Promise<Metadata> {
  return new Promise((resolve, reject) =>
    consumer.getMetadata(options, (err, metadata) =>
      err ? reject(err) : resolve(metadata)
    )
  );
}

export function getAssigments(
  metadata: Metadata,
  topics: SubscribeTopicList
): Assignment[] {
  const assigments = getTopicsMetadata(metadata, topics).flatMap((metaTopic) =>
    metaTopic.partitions.map((partition) => ({
      topic: metaTopic.name,
      partition: partition.id,
      offset: 0, // Change this offset if you want to start elsewhere
    }))
  );

  return assigments;
}
