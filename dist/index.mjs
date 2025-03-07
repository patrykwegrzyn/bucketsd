import{KafkaConsumer as g,Producer as b,AdminClient as k,CODES as C}from"@confluentinc/kafka-javascript";import{Buffer as l}from"buffer";import{Store as w}from"buckets";import{once as p}from"events";function d(n,t){return n.topics.filter(e=>t.some(s=>typeof s=="string"?e.name===s:s instanceof RegExp?s.test(e.name):!1))}function m(n,t){return new Promise((e,s)=>n.getMetadata(t,(r,o)=>r?s(r):e(o)))}function f(n,t){return d(n,t).flatMap(s=>s.partitions.map(r=>({topic:s.name,partition:r.id,offset:0})))}var h=class{constructor(t){this.initialized=!1;this.watermarks=new Map;this.topicOffsets=new Map;let{createTopicOptions:e,appId:s,brokers:r,store:o,consumer:a,producer:i}=t;this.topic=e?e.topic:`${s}.kv`,this.appId=s,this.createTopicOptions=e,this.store=new w(`db/${s}`,{cache:!0,...o}),this.store.on("change",u=>this.handleStoreChange(u));let c=Array.isArray(r)?r.join(","):"localhost:19092";this.consumer=new g({"group.id":"kv","bootstrap.servers":c,"enable.auto.commit":!1,"enable.auto.offset.store":!1,...a?.global},{"auto.offset.reset":"beginning",...a?.topic||{}}),this.producer=new b({"bootstrap.servers":c,...i?.global},{...i?.topic})}handleStoreChange(t){let{op:e,bucket:s,id:r,value:o,ttl:a}=t,i=[];e&&i.push({op:e}),s&&i.push({bucket:s}),a&&i.push({ttl:a});let c=e==="remove"?null:l.isBuffer(o)?o:l.from(o);console.log({messageValue:c});let u=this.producer.produce(this.topic,null,c,r,Date.now(),null,i);console.log({res:u})}queryWatermark(t){return new Promise((e,s)=>{this.consumer.queryWatermarkOffsets(this.topic,t,5e3,(r,o)=>{if(r)return s(r);console.log("queryWatermarkOffsets",{partition:t,offsets:o}),this.watermarks.set(t,Number(o.highOffset)),this.topicOffsets.set(t,-1),e()})})}async setupPartitionStatus(t){await Promise.all(t.map(({partition:e})=>this.queryWatermark(e))),console.log("Partition status initialized:",this.watermarks)}async start(){this.consumer.setDefaultConsumeTimeout(1e3),this.consumer.connect(),this.producer.connect(),await Promise.all([p(this.consumer,"ready"),p(this.producer,"ready")]),k.createFrom(this.consumer).createTopic({num_partitions:5,replication_factor:3,config:{"segment.ms":"300000","segment.bytes":"102400","cleanup.policy":"compact"},...this.createTopicOptions,topic:this.topic},async e=>{if(e&&e.code!==C.ERRORS.ERR_TOPIC_ALREADY_EXISTS)throw e;let s=await m(this.consumer,{}),r=f(s,[this.topic]);this.consumer.assign(r),await this.setupPartitionStatus(r),this.consumer.consume(),this.consumer.on("data",o=>this.handleConsumerData(o))}),await this.waitUntilCaughtUp()}handleConsumerData(t){this.topicOffsets.set(t.partition,t.offset);let e=t.headers?.reduce((s,r)=>{let o=Object.keys(r)[0];return s[o]=r[o].toString(),s},{});if(t.key&&e?.bucket&&e?.op){let s=this.store.bucket(e.bucket,{encoding:"json"});e.op==="put"?s.put(t.key.toString(),t.value,{quiet:!0}):s.remove(t.key.toString(),{quiet:!0})}}waitUntilCaughtUp(){return new Promise(t=>{let e=setInterval(()=>{this.hasCaughtUp()&&(clearInterval(e),console.log("All partitions caught up. KV is ready."),this.initialized=!0,t())},1e3)})}hasCaughtUp(){return Array.from(this.watermarks.entries()).every(([t,e])=>{let s=this.topicOffsets.get(t);return s!==void 0&&s>=e-1})}async stop(){this.consumer.unsubscribe(),this.consumer.pause(this.consumer.assignments()),this.producer.flush(5e3,t=>{console.log(t),this.producer.disconnect(),this.consumer.disconnect()}),await Promise.all([this.consumer,this.producer].map(t=>p(t,"disconnected")))}};export{h as Bucketsd};
//# sourceMappingURL=index.mjs.map