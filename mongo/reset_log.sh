#!/bin/bash
set -e

# Paths
MONGO_PATH="/home/takashitanaka/mongo"
MONITOR_PATH="/home/takashitanaka/grafana"

# MongoDB services in your docker-compose.yml
services=(router01 router02 configsvr01 configsvr02 configsvr03 \
          shard01-a shard01-b shard01-c \
          shard02-a shard02-b shard02-c \
          shard03-a shard03-b shard03-c)

echo "🛑 Stopping promtail (detach from volume)..."
cd "$MONITOR_PATH"
docker-compose -p "monitoring" down

echo "⏳ Stopping MongoDB cluster..."
cd "$MONGO_PATH"
docker-compose -p "mongo" down || true

# Wait until all mongo containers are stopped
for s in "${services[@]}"; do
  while docker ps --format '{{.Names}}' | grep -q "mongo_${s}"; do
    echo "   $s still stopping..."
    sleep 2
  done
done
echo "✅ All MongoDB services stopped."

# Clean up any leftover mongo networks
echo "🧹 Cleaning up docker networks..."
mongo_networks=$(docker network ls --format '{{.Name}}' | grep '^mongo' || true)
for net in $mongo_networks; do
  echo "   Removing network $net..."
  # disconnect any dangling containers first
  for cid in $(docker network inspect -f '{{range .Containers}}{{.Name}} {{end}}' "$net"); do
    echo "     disconnecting $cid from $net..."
    docker network disconnect -f "$net" "$cid" || true
  done
  docker network rm "$net" || true
done

# Remove & recreate ALL mongo volumes (data + logs)
echo "🗑️  Removing all mongo volumes..."
mongo_volumes=$(docker volume ls --format '{{.Name}}' | grep '^mongo' || true)
for vol in $mongo_volumes; do
  echo "   Removing volume $vol..."
  docker volume rm -f "$vol" || true
done

# Recreate only the base volumes you need fresh (example: logs)
echo "📦 Recreating base volumes..."
docker volume create mongo_logs

# Start Monitoring
echo "📦 Restarting monitoring service..."
cd "$MONITOR_PATH"
docker-compose -p "monitoring" up -d

# Start MongoDB cluster
cd "$MONGO_PATH"
echo "🚀 Starting MongoDB cluster..."
docker-compose -p "mongo" up -d

echo "🚀 Initializing MongoDB scripts in parallel..."

# Run all scripts in the background
docker-compose exec -T configsvr01 sh -c "mongosh < /scripts/init-configserver.js" &
docker-compose exec -T shard01-a sh -c "mongosh < /scripts/init-shard01.js" &
docker-compose exec -T shard02-a sh -c "mongosh < /scripts/init-shard02.js" &
docker-compose exec -T shard03-a sh -c "mongosh < /scripts/init-shard03.js" &
docker-compose exec -T router01 sh -c "mongosh < /scripts/init-router.js" &

# Wait for all background jobs
wait

echo "✅ All MongoDB initialization scripts have finished."

# -------------------------
# Wait until MongoDB cluster is fully initialized
# -------------------------
echo "⏳ Waiting for MongoDB cluster to initialize..."

# Check config servers
configsvrs=(configsvr01 configsvr02 configsvr03)
for cfg in "${configsvrs[@]}"; do
  until docker-compose -p "mongo" exec -T $cfg mongosh --quiet --eval 'rs.status().ok' 2>/dev/null | grep -q '^1$'; do
    echo "   $cfg config server not initialized yet..."
    sleep 2
  done
  echo "   $cfg config server initialized ✅"
done



# Check each shard replica set
shard_sets=(shard01 shard02 shard03)
for shard in "${shard_sets[@]}"; do
  shard_container="${shard}-a"
  until docker-compose -p "mongo" exec -T $shard_container mongosh --quiet --eval 'rs.status().ok' 2>/dev/null | grep -q '^1$'; do
    echo "   $shard not initialized yet..."
    sleep 2
  done
  echo "   $shard initialized ✅"
done

# Check routers can see all shards
routers=(router01 router02)
for r in "${routers[@]}"; do
  until docker-compose -p "mongo" exec -T $r mongosh --quiet --eval 'sh.status()' 2>/dev/null | grep -q 'shards'; do
    echo "   $r router not ready yet..."
    sleep 2
  done
  echo "   $r router ready ✅"
done

echo "✅ MongoDB cluster fully initialized and ready!"

# Restart promtail AFTER MongoDB is running
echo "🚀 Starting promtail..."
docker start promtail

echo "✅ MongoDB fully reset (data + logs), networks cleaned, and promtail is back online."
