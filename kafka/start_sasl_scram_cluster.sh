#!/usr/bin/env bash
set -euo pipefail

export PATH=$PATH:/home/takashitanaka/.vscode/extensions/redhat.java-1.45.0-linux-x64/jre/21.0.8-linux-x86_64/bin

CLUSTER_ID=xwKCEeWJToei3os4N3JYfQ
WORKDIR=${PWD}
export DIR_PROPERTIES=$WORKDIR/properties
export DIR_JAR=$WORKDIR/jar
export DIR_CONFIG=$WORKDIR/config
export DIR_CERTIFICATES=$WORKDIR/certificates

echo "Working dir        : ${WORKDIR}"
echo "Certificates       : ${DIR_CERTIFICATES}"

BROKER_PASS=brokerpass
CLIENT_PASS=clientpass

SSL_DIR=${DIR_CERTIFICATES}
CA_DIR=${SSL_DIR}/ca
CLIENT_DIR=${SSL_DIR}/client

rm -rf "${DIR_CERTIFICATES}"
rm -rf "${DIR_PROPERTIES}"
rm -rf "${DIR_CONFIG}/*.conf"

mkdir -p "${CA_DIR}" "${CLIENT_DIR}"

generate_ca() {
  echo "🔑 Generating new Cluster CA..."
  openssl req -new -x509 -keyout "${CA_DIR}/ca.key" \
    -out "${CA_DIR}/ca.crt" -days 3650 -nodes \
    -subj "/CN=Kafka-Cluster-CA/OU=Dev/O=Company/L=City,ST=State,C=ID"
  hr "="
}

generate_broker_cert() {
  local broker=$1
  local dir="${SSL_DIR}/${broker}"
  rm -rf "${dir}"
  mkdir -p "${dir}"

  echo "🔑 Keystore for ${broker}"
  keytool -genkeypair \
    -alias "${broker}" -keyalg RSA -keysize 2048 \
    -keystore "${dir}/kafka.${broker}.keystore.jks" \
    -storepass "${BROKER_PASS}" -keypass "${BROKER_PASS}" \
    -dname "CN=${broker}, OU=Dev, O=Company, L=City, ST=State, C=ID" \
    -ext SAN=DNS:kafka-broker-1,DNS:kafka-broker-2,DNS:kafka-broker-3,DNS:localhost,IP:127.0.0.1
    

  keytool -certreq -alias "${broker}" -file "${dir}/${broker}.csr" \
    -keystore "${dir}/kafka.${broker}.keystore.jks" \
    -storepass "${BROKER_PASS}"

  openssl x509 -req -in "${dir}/${broker}.csr" \
    -CA "${CA_DIR}/ca.crt" -CAkey "${CA_DIR}/ca.key" \
    -out "${dir}/${broker}.crt" -days 365 -CAcreateserial -sha256

  keytool -import -trustcacerts -alias CARoot \
    -file "${CA_DIR}/ca.crt" \
    -keystore "${dir}/kafka.${broker}.keystore.jks" \
    -storepass "${BROKER_PASS}" -noprompt

  keytool -import -alias "${broker}" -file "${dir}/${broker}.crt" \
    -keystore "${dir}/kafka.${broker}.keystore.jks" \
    -storepass "${BROKER_PASS}" -noprompt

  keytool -import -file "${CA_DIR}/ca.crt" -alias CARoot \
    -keystore "${dir}/kafka.${broker}.truststore.jks" \
    -storepass "${BROKER_PASS}" -noprompt

  echo "${BROKER_PASS}" > "${dir}/${broker}_keystore_creds"
  echo "${BROKER_PASS}" > "${dir}/${broker}_sslkey_creds"
  echo "${BROKER_PASS}" > "${dir}/${broker}_truststore_creds"

  chmod 600 "${dir}"/*_creds
  hr "="
}

generate_client_cert() {
  echo "🔑 Client keystore"
  keytool -genkeypair \
    -alias kafka-client -keyalg RSA -keysize 2048 \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" -keypass "${CLIENT_PASS}" \
    -dname "CN=kafka-client, OU=Dev, O=Company, L=City, ST=State, C=ID"

  keytool -certreq -alias kafka-client \
    -file "${CLIENT_DIR}/kafka-client.csr" \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}"

  openssl x509 -req -in "${CLIENT_DIR}/kafka-client.csr" \
    -CA "${CA_DIR}/ca.crt" -CAkey "${CA_DIR}/ca.key" \
    -out "${CLIENT_DIR}/kafka-client.crt" -days 365 -CAcreateserial -sha256

  keytool -import -trustcacerts -alias CARoot \
    -file "${CA_DIR}/ca.crt" \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" -noprompt

  keytool -import -alias kafka-client \
    -file "${CLIENT_DIR}/kafka-client.crt" \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" -noprompt

  keytool -import -file "${CA_DIR}/ca.crt" -alias CARoot \
    -keystore "${CLIENT_DIR}/kafka.client.truststore.jks" \
    -storepass "${CLIENT_PASS}" -noprompt

  hr "="
}

export_pem_bundle() {
  echo "🔑 Generating PEM CA bundle..."
  rm -rf "${SSL_DIR}/pem"
  mkdir -p "${SSL_DIR}/pem"

  for broker in kafka-broker-1 kafka-broker-2 kafka-broker-3; do
    keytool -exportcert \
      -keystore "${SSL_DIR}/${broker}/kafka.${broker}.keystore.jks" \
      -storepass "${BROKER_PASS}" \
      -alias "${broker}" -rfc \
      -file "${SSL_DIR}/pem/${broker}.pem"
  done

  rm -f "${SSL_DIR}/pem/kafka-cluster-ca.pem"

  cat "${SSL_DIR}"/pem/kafka-broker-*.pem > "${SSL_DIR}/pem/kafka-cluster-ca.pem"

  echo "PEM bundle at ${SSL_DIR}/pem/kafka-cluster-ca.pem"
  hr "="
}

clean_ports() {
  echo "🚮 Cleaning used ports..."
  for port in 19094 19095 19096 9093 9101 9102 9103 19084 29084 39084 8080 8081; do
    pid=$(ss -ltnp 2>/dev/null | grep ":$port " | awk -F',' '{print $2}' | awk '{print $1}' || true)
    [ -z "$pid" ] && pid=$(lsof -t -i:$port 2>/dev/null || true)

    if [ -n "$pid" ]; then
      echo "🗑️ Killing PID $pid (port $port)"
      kill -9 "$pid" || true
    fi
  done
  hr "="
}


start_docker() {
  echo "📦 Starting Service Group..."
  docker-compose -p "message-broker" -f "$PWD/docker-compose-scram.yml" up -d --remove-orphans
  hr "="
}

format_kafka() {
  echo "📦 Formatting Kafka storage..."
  for broker in 1 2 3; do
    props="${DIR_PROPERTIES}/storage-${broker}.properties"
    : > "$props"

    echo "process.roles=broker,controller" >> "$props"
    echo "node.id=${broker}" >> "$props"
    echo "controller.listener.names=CONTROLLER" >> "$props"
    echo "controller.quorum.voters=1@kafka-broker-1:9093,2@kafka-broker-2:9093,3@kafka-broker-3:9093" >> "$props"
    echo "listeners=SASL_SSL://:9092,PLAINTEXT://:29092,CONTROLLER://:9093" >> "$props"
    echo "listener.security.protocol.map=SASL_SSL:SASL_SSL,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" >> "$props"
    echo "log.dirs=/var/lib/kafka/data" >> "$props"


    docker run --rm \
      -v "${DIR_CERTIFICATES}/kafka-broker-${broker}:/etc/kafka/secrets" \
      -v "${props}:/etc/kafka/storage.properties" \
      confluentinc/cp-kafka:latest \
      kafka-storage format \
        --ignore-formatted \
        --cluster-id "$CLUSTER_ID" \
        --config /etc/kafka/storage.properties
  done
  hr "="
}

prepare_kafka_properties() {
  echo "📝 Prepare server properties"
  local dir="${DIR_PROPERTIES}"
  
  mkdir -p "${dir}"

  for b in 1 2 3; do
    broker="kafka-broker-${b}"
    rm -rf "${dir}/${broker}.properties"

    props="${dir}/${broker}.properties"
    : > "$props"
    
    echo "ssl.truststore.password=${BROKER_PASS}" >> "$props"
    echo "controller.listener.names=CONTROLLER" >> "$props"
    echo "ssl.keystore.password=${BROKER_PASS}" >> "$props"
    echo "ssl.keystore.credentials=${broker}_keystore_creds" >> "$props"
    echo "ssl.key.password=${BROKER_PASS}" >> "$props"
    echo "jmx.port=" >> "$props"
    echo "advertised.listeners=SASL_SSL://${broker}:9092,PLAINTEXT://${broker}:29092" >> "$props"
    echo "inter.broker.listener.name=SASL_SSL" >> "$props"
    echo "min.insync.replicas=1" >> "$props"
    echo "controller.quorum.voters=1@kafka-broker-1:9093,2@kafka-broker-2:9093,3@kafka-broker-3:9093" >> "$props"
    echo "ssl.keystore.location=/etc/kafka/secrets/kafka.${broker}.keystore.jks" >> "$props"
    echo "ssl.key.credentials=${broker}_sslkey_creds" >> "$props"
    echo "ssl.endpoint.identification.algorithm=HTTPS" >> "$props"
    echo "log.dirs=/var/lib/kafka/data" >> "$props"
    echo "heap.opts=-Xms1G -Xmx2G" >> "$props"
    echo "default.password=confluent" >> "$props"
    echo "default.user=kafkabroker" >> "$props"
    echo "ssl.client.auth=requested" >> "$props"
    echo "ssl.truststore.location=/etc/kafka/secrets/kafka.${broker}.truststore.jks" >> "$props"
    echo "listeners=SASL_SSL://:9092,PLAINTEXT://:29092,CONTROLLER://:9093" >> "$props"
    echo "ssl.truststore.filename=kafka.${broker}.truststore.jks" >> "$props"
    echo "sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512" >> "$props"
    echo "override.inter.broker.listener.name=SASL_SSL" >> "$props"
    echo "auto.create.topics.enable=true" >> "$props"
    echo "listener.security.protocol.map=SASL_SSL:SASL_SSL,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" >> "$props"
    echo "node.id=1" >> "$props"
    echo "sasl.enabled.mechanisms=SCRAM-SHA-512" >> "$props"
    echo "ssl.keystore.filename=kafka.${broker}.keystore.jks" >> "$props"
    echo "ssl.truststore.credentials=${broker}_truststore_creds" >> "$props"
    echo "process.roles=broker,controller"  >> "$props"

  done

  hr "="
}

prepare_client_properties() {
  echo "📝 Prepare client properties"

  props="${DIR_PROPERTIES}/client.properties"
  rm -f "$props"

  echo "security.protocol=SASL_SSL" >> "$props"
  echo "sasl.mechanism=SCRAM-SHA-512" >> "$props"
  echo "ssl.truststore.location=/etc/kafka/secrets/kafka.kafka-broker-1.truststore.jks" >> "$props"
  echo "ssl.truststore.password=${BROKER_PASS}" >> "$props"
  echo "ssl.keystore.location=/etc/kafka/secrets/kafka.kafka-broker-1.keystore.jks" >> "$props"
  echo "ssl.keystore.password=${BROKER_PASS}" >> "$props"
  echo "ssl.endpoint.identification.algorithm=" >> "$props"
  echo 'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafkabroker" password="confluent";' >> "$props"
  
  for broker in 1 2 3; do
    docker cp "${props}" $(docker ps -qf "name=kafka-broker-${broker}"):/etc/kafka/properties/client-cli.properties
  done

  # docker cp "${props}" $(docker ps -qf "name=control-center"):/etc/kafka/config/client-cli.properties
}

prepare_control_properties() {
  echo "📝 Prepare control properties"

  props="control.properties"
  rm -f "$props"

  echo "bootstrap.servers=kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092" >> "$props"
  echo "security.protocol=SASL_SSL" >> "$props"
  echo "sasl.mechanism=SCRAM-SHA-512" >> "$props"
  echo "ssl.truststore.location=/etc/kafka/secrets/kafka.client.truststore.jks" >> "$props"
  echo "ssl.truststore.password=${CLIENT_PASS}" >> "$props"
  # echo "ssl.keystore.location=/etc/kafka/secrets/kafka.client.keystore.jks" >> "$props"
  # echo "ssl.keystore.password=${CLIENT_PASS}" >> "$props"
  echo "ssl.endpoint.identification.algorithm=" >> "$props"
  echo 'sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="kafkabroker" password="confluent";' >> "$props"
  
  docker cp "${props}" $(docker ps -qf "name=control-center"):/etc/kafka/properties/control.properties

  docker exec -e KAFKA_OPTS="" control-center chmod 644 /etc/kafka/secrets/kafka.client.truststore.jks
  docker exec -e KAFKA_OPTS="" control-center chmod 644 /etc/kafka/secrets/kafka.client.keystore.jks
  docker exec -e KAFKA_OPTS="" control-center chmod 644 /etc/kafka/properties/client-cli.properties

  docker exec -e KAFKA_OPTS="" control-center chown 1000:1000 /etc/kafka/secrets/kafka.client.truststore.jks
  docker exec -e KAFKA_OPTS="" control-center chown 1000:1000 /etc/kafka/secrets/kafka.client.keystore.jks
  docker exec -e KAFKA_OPTS="" control-center chown 1000:1000 /etc/kafka/properties/client-cli.properties
}

create_test_topic() {
  echo "📝 Create test topic..."
  docker exec -e KAFKA_OPTS="" kafka-broker-1 kafka-topics \
    --bootstrap-server kafka-broker-1:9092 \
    --command-config /etc/kafka/properties/client-cli.properties \
    --create \
    --topic test.internal \
    --partitions 1 \
    --replication-factor 3

  echo "📝 Check for test topic..."
  docker exec -e KAFKA_OPTS="" kafka-broker-1 kafka-topics \
  --bootstrap-server kafka-broker-1:29092 \
  --list
}

create_scram_users() {
  echo "👤 Creating SCRAM users..."
  docker exec -e KAFKA_OPTS="" kafka-broker-1 kafka-configs \
    --bootstrap-server kafka-broker-1:29092 \
    --alter \
    --add-config 'SCRAM-SHA-512=[password=confluent]' \
    --entity-type users \
    --entity-name kafkabroker
}

verify_broker_cert() {
  local broker=$1
  local dir="${SSL_DIR}/${broker}"
  echo "🔍 Verifying keystore for ${broker}..."

  echo "📋 Keystore content:"
  keytool -list -v \
      -keystore "${dir}/kafka.${broker}.keystore.jks" \
      -storepass "${BROKER_PASS}" \
      -alias "${broker}" || true

  echo "📋 Certificate SANs:"
  keytool -printcert -v -file "${dir}/${broker}.crt" || true


  echo "✅ ${broker} certificate OK"
  hr "="
}

verify_client_cert() {
  echo "🔍 Verifying client keystore..."

  echo "📋 Keystore content:"
  keytool -list -v \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" \
    -alias kafka-client || true

  echo "📋 Certificate SANs:"
  keytool -printcert -v -file "${CLIENT_DIR}/kafka-client.crt" || true

  echo "✅ Client certificate OK"
  hr "="
}


verify_pem_bundle() {
  echo "🔍 Verifying PEM bundle..."
  if [ -f "${SSL_DIR}/pem/kafka-cluster-ca.pem" ]; then
    openssl x509 -in "${SSL_DIR}/pem/kafka-cluster-ca.pem" -text -noout | grep -E 'Subject:|Issuer:|DNS:|IP Address:'
    echo "✅ PEM bundle OK"
  else
    echo "❌ PEM bundle not found!"
  fi
  hr "="
}


generate_jaas() {
  if [ $# -ne 2 ]; then
    echo "Usage: generate_jaas <username> <password>"
    return 1
  fi

  local user="$1"
  local pass="$2"
  local output="${DIR_CONFIG}/kafka_server_jaas.conf"

  echo "📝 Generating JAAS config for broker with user=$user"

  mkdir -p "${DIR_CONFIG}"

  {
    echo "KafkaServer {"
    echo "  org.apache.kafka.common.security.scram.ScramLoginModule required"
    echo "  username=\"${user}\""
    echo "  password=\"${pass}\";"
    echo "};"
    echo
    echo "Client {"
    echo "  org.apache.kafka.common.security.scram.ScramLoginModule required"
    echo "  username=\"${user}\""
    echo "  password=\"${pass}\";"
    echo "};"
  } > "$output"

  hr "="
}

clean_all() {
  echo "🧹 Cleaning up containers and volumes..."
  docker-compose -p "message-broker" -f "$PWD/docker-compose-scram.yml" down -v || true
  hr "="
}

hr() {
    local char="${1:--}"
    printf '%*s\n' "$(tput cols)" '' | tr ' ' "$char"
}


hr "="
clean_all
generate_ca
for b in kafka-broker-1 kafka-broker-2 kafka-broker-3; do
  generate_broker_cert "${b}"
  verify_broker_cert "${b}"
done
generate_client_cert
verify_client_cert
export_pem_bundle
verify_pem_bundle
clean_ports
generate_jaas kafkabroker confluent
prepare_kafka_properties
start_docker
prepare_client_properties
# prepare_control_properties
format_kafka
sleep 10
create_scram_users
create_test_topic

echo "✅ All done!"
