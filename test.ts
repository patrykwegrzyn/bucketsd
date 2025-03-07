import { Bucketsd } from "./src";

const buckets = new Bucketsd({
  appId: "testappid",

  brokers: ["localhost:19092"],
  store: {
    encoding: "json",
  },
});

async function main() {
  try {
    await buckets.start();
    console.log("test here");

    const ci = buckets.store.bucket("test1");

    for (let i = 0; i < 1000; i++) {
      // const res = await ci.put("hello_" + i, { i });
      // console.log(ci.get("hello_" + i));
    }
    await buckets.stop();
  } catch (err) {
    console.log(err);
  }
}

main();
