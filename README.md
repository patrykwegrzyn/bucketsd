# Bucketsd

Bucketsd is a key-value store that leverages Kafka for replication and LMDB for storage. It ensures high availability and durability by integrating Kafka's streaming capabilities with LMDB's efficient data storage.

## Features
- **Kafka Integration**: Uses Kafka for replication and streaming changes.
- **LMDB Storage**: Provides efficient and persistent key-value storage.
- **Automatic Topic Management**: Creates Kafka topics dynamically.
- **High Availability**: Ensures data integrity across partitions.

## Installation
```sh
npm install patrykwegrzyn/bucketsd
```

## Usage

```typescript
import { Bucketsd } from "bucketsd";

const bucketsd = new Bucketsd({
  appId: "myApp",
  brokers: ["localhost:9092"]
});

bucketsd.start().then(() => {
  console.log("Bucketsd started!");
});
```

## API Reference

### `constructor(options: Options)`
Initializes a new Bucketsd instance.

- `options.appId`: Unique application ID.
- `options.brokers`: List of Kafka brokers.
- `options?.store`: LMDB database options.
- `options?.consumer`: Kafka consumer configuration.
- `options?.producer`: Kafka producer configuration.
- `options?.createTopicOptions`: Kafka topic creation options.

### `start(): Promise<void>`
Starts the Bucketsd service, connecting to Kafka and LMDB.

### `stop(): Promise<void>`
Stops the service, disconnecting from Kafka and LMDB.


## License
MIT
