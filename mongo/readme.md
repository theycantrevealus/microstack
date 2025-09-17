# Running Container
```bash
docker-compose up -d
```

## 1. Initialize the replica sets (config servers and shards)
```bash
docker-compose exec configsvr01 sh -c "mongosh < /scripts/init-configserver.js"

docker-compose exec shard01-a sh -c "mongosh < /scripts/init-shard01.js"
docker-compose exec shard02-a sh -c "mongosh < /scripts/init-shard02.js"
docker-compose exec shard03-a sh -c "mongosh < /scripts/init-shard03.js"
```

## 2. Initializing the router
```bash
docker-compose exec router01 sh -c "mongosh < /scripts/init-router.js"
```

# 3. Enable sharding and setup sharding-key
```bash
docker-compose exec router01 mongosh --port 27017

// Enable sharding for database `MyDatabase`
sh.enableSharding("MyDatabase")

// Setup shardingKey for collection `MyCollection`**
db.adminCommand( { shardCollection: "MyDatabase.MyCollection", key: { oemNumber: "hashed", zipCode: 1, supplierId: 1 }, numInitialChunks: 3 } )

```

## 4. Verify the status of the sharded cluster

```bash
docker-compose exec router01 mongosh --port 27017
sh.status()
```

## 5. Verify status of replica set for each shard
```bash
docker exec -it shard-01-node-a bash -c "echo 'rs.status()' | mongosh --port 27017" 
docker exec -it shard-02-node-a bash -c "echo 'rs.status()' | mongosh --port 27017" 
docker exec -it shard-03-node-a bash -c "echo 'rs.status()' | mongosh --port 27017" 
```

## 6. Check database status
```bash
docker-compose exec router01 mongosh --port 27017
use MyDatabase
db.stats()
db.MyCollection.getShardDistribution()
```