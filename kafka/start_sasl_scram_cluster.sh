#!/usr/bin/env bash
set -euo pipefail
print_frame() {
    local lines=("$@")
    local max_len=0

    # Find the maximum line length
    for line in "${lines[@]}"; do
        (( ${#line} > max_len )) && max_len=${#line}
    done

    # Print top border
    printf '%*s\n' $((max_len + 4)) '' | tr ' ' '*'

    # Print each line with side borders
    for line in "${lines[@]}"; do
        printf "* %-${max_len}s *\n" "$line"
    done

    # Print bottom border
    printf '%*s\n' $((max_len + 4)) '' | tr ' ' '*'
}

export PATH=$PATH:/home/takashitanaka/.vscode/extensions/redhat.java-1.45.0-linux-x64/jre/21.0.8-linux-x86_64/bin
LAST_LINE=""
# CLUSTER_ID=xwKCEeWJToei3os4N3JYfQ
WORKDIR=${PWD}
TARGET_COMPOSE="docker-compose.yml"
export CLUSTER_ID=$(docker run --rm confluentinc/cp-kafka:latest kafka-storage random-uuid)
export DIR_PROPERTIES=$WORKDIR/properties
export DIR_JAR=$WORKDIR/jar
export DIR_CONFIG=$WORKDIR/config
export DIR_CERTIFICATES=$WORKDIR/certificates
export DIR_SHELL=$WORKDIR/shell
SSL_DIR=${DIR_CERTIFICATES}
CA_DIR=${SSL_DIR}/ca
CLIENT_DIR=${SSL_DIR}/client

lines=(
    "Apps Name          : MICROSTACK - KAFKA KRAFT MODE STARTER"
    "Created by         : HENDRY TANAKA"
    "Get it touch       : hendrytanaka10@icloud.com"
    "Working dir        : ${WORKDIR}"
    "Certificates       : ${DIR_CERTIFICATES}"
    "Client Cert        : ${CLIENT_DIR}"
    "SSL                : ${SSL_DIR}"
    "Properties         : ${DIR_PROPERTIES}"
    "JAR                : ${DIR_JAR}"
    "CONFIG             : ${DIR_CONFIG}"
    "SHELLS             : ${DIR_SHELL}"
)
print_frame "${lines[@]}"

export BROKER_PASS=brokerpass
export CLIENT_PASS=clientpass

rm -rf "${DIR_CERTIFICATES}"
rm -rf "${DIR_PROPERTIES}"
rm -rf "${DIR_SHELL}"
rm -rf "${DIR_CONFIG}/*.conf"

mkdir -p "${CA_DIR}" "${CLIENT_DIR}"

custom_print() {
  echo "$@"
  LAST_LINE="$*"
}

generate_ca() {
  custom_print "🔑 Generating new Cluster CA"
  openssl req -new -x509 -keyout "${CA_DIR}/ca.key" \
    -out "${CA_DIR}/ca.crt" -days 3650 -nodes \
    -subj "/CN=Kafka-Cluster-CA/OU=Dev/O=Company/L=City,ST=State,C=ID" >/dev/null 2>&1
  update_status "✅ OK"
}

generate_broker_cert() {
  local broker=$1
  local dir="${SSL_DIR}/${broker}"
  rm -rf "${dir}"
  mkdir -p "${dir}"

  custom_print "🔑 Generating keystore for ${broker}"
  keytool -genkeypair \
    -alias "${broker}" -keyalg RSA -keysize 2048 \
    -keystore "${dir}/kafka.${broker}.keystore.jks" \
    -storepass "${BROKER_PASS}" -keypass "${BROKER_PASS}" \
    -dname "CN=${broker}, OU=Dev, O=Company, L=City, ST=State, C=ID" \
    -ext SAN=DNS:kafka-broker-1,DNS:kafka-broker-2,DNS:kafka-broker-3,DNS:localhost,IP:127.0.0.1 >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Generating keystore CSR for ${broker}"
  keytool -certreq -alias "${broker}" -file "${dir}/${broker}.csr" \
    -keystore "${dir}/kafka.${broker}.keystore.jks" \
    -storepass "${BROKER_PASS}" >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Signing keystore CSR for ${broker}"
  openssl x509 -req -in "${dir}/${broker}.csr" \
    -CA "${CA_DIR}/ca.crt" -CAkey "${CA_DIR}/ca.key" \
    -out "${dir}/${broker}.crt" -days 365 -CAcreateserial -sha256 >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Signing keystore CA for ${broker}"
  keytool -import -trustcacerts -alias CARoot \
    -file "${CA_DIR}/ca.crt" \
    -keystore "${dir}/kafka.${broker}.keystore.jks" \
    -storepass "${BROKER_PASS}" -noprompt >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Importing keystore CA for ${broker}"
  keytool -import -alias "${broker}" -file "${dir}/${broker}.crt" \
    -keystore "${dir}/kafka.${broker}.keystore.jks" \
    -storepass "${BROKER_PASS}" -noprompt >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Importing keystore CARoot for ${broker}"
  keytool -import -file "${CA_DIR}/ca.crt" -alias CARoot \
    -keystore "${dir}/kafka.${broker}.truststore.jks" \
    -storepass "${BROKER_PASS}" -noprompt >/dev/null 2>&1
  update_status "✅ OK"

  echo "${BROKER_PASS}" > "${dir}/${broker}_keystore_creds"
  echo "${BROKER_PASS}" > "${dir}/${broker}_sslkey_creds"
  echo "${BROKER_PASS}" > "${dir}/${broker}_truststore_creds"

  custom_print "🔑 Updating certificates permission for ${broker}"
  chmod 600 "${dir}"/*_creds
  update_status "✅ OK"
}

generate_client_cert() {
  custom_print "🔑 Client keystore - generate keystore"
  keytool -genkeypair \
    -alias kafka-client -keyalg RSA -keysize 2048 \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" -keypass "${CLIENT_PASS}" \
    -dname "CN=kafka-client, OU=Dev, O=Company, L=City, ST=State, C=ID" >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - certificate request keystore"
  keytool -certreq -alias kafka-client \
    -file "${CLIENT_DIR}/kafka-client.csr" \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - creating client csr"
  openssl x509 -req -in "${CLIENT_DIR}/kafka-client.csr" \
    -CA "${CA_DIR}/ca.crt" -CAkey "${CA_DIR}/ca.key" \
    -out "${CLIENT_DIR}/kafka-client.crt" -days 365 -CAcreateserial -sha256 >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - add ca-root on keystore"
  keytool -import -trustcacerts -alias CARoot \
    -file "${CA_DIR}/ca.crt" \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" -noprompt >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - create client crt"
  keytool -import -alias kafka-client \
    -file "${CLIENT_DIR}/kafka-client.crt" \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" -noprompt >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - sign crt with ca-root"
  keytool -import -file "${CA_DIR}/ca.crt" -alias CARoot \
    -keystore "${CLIENT_DIR}/kafka.client.truststore.jks" \
    -storepass "${CLIENT_PASS}" -noprompt >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - create client p12"
  keytool -importkeystore \
    -srckeystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -srcstoretype JKS \
    -srcstorepass "${CLIENT_PASS}" \
    -destkeystore "${CLIENT_DIR}/kafka.client.p12" \
    -deststoretype PKCS12 \
    -deststorepass "${CLIENT_PASS}" \
    -srcalias kafka-client \
    -destalias kafka-client \
    -srckeypass "${CLIENT_PASS}" \
    -destkeypass "${CLIENT_PASS}" \
    -noprompt >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - extract client key"
  openssl pkcs12 -in "${CLIENT_DIR}/kafka.client.p12" \
    -nodes -nocerts \
    -out "${CLIENT_DIR}/kafka-client.key" \
    -passin pass:"${CLIENT_PASS}" >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - certificate client pem"
  openssl pkcs12 -in "${CLIENT_DIR}/kafka.client.p12" \
    -nokeys \
    -out "${CLIENT_DIR}/kafka-client.pem" \
    -passin pass:"${CLIENT_PASS}" >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - format to PKCS8"
  openssl pkcs8 -topk8 -inform PEM -outform PEM -in "${CLIENT_DIR}/kafka-client.key" -out "${CLIENT_DIR}/kafka-client.key.pem" -nocrypt >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - convert client cert + key to PKCS12"
  openssl pkcs12 -export \
    -in "${CLIENT_DIR}/kafka-client.pem" \
    -inkey "${CLIENT_DIR}/kafka-client.key.pem" \
    -out "${CLIENT_DIR}/kafka.client.p12" \
    -name kafka-client \
    -CAfile "${SSL_DIR}/pem/kafka-cluster-ca.pem" \
    -caname root \
    -password "pass:${CLIENT_PASS}" >/dev/null 2>&1
  update_status "✅ OK"

  custom_print "🔑 Client keystore - import to JKS"
  keytool -importkeystore \
    -deststorepass "${CLIENT_PASS}" \
    -destkeypass "${CLIENT_PASS}" \
    -destkeystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -srckeystore "${CLIENT_DIR}/kafka.client.p12" \
    -srcstoretype PKCS12 \
    -srcstorepass "${CLIENT_PASS}" \
    -alias kafka-client\
    -noprompt >/dev/null 2>&1
  update_status "✅ OK"

}

export_pem_bundle() {
  rm -rf "${SSL_DIR}/pem"
  mkdir -p "${SSL_DIR}/pem"

  for broker in kafka-broker-1 kafka-broker-2 kafka-broker-3; do
    custom_print "🔑 Generating PEM CA bundle for ${broker}"
    keytool -exportcert \
      -keystore "${SSL_DIR}/${broker}/kafka.${broker}.keystore.jks" \
      -storepass "${BROKER_PASS}" \
      -alias "${broker}" -rfc \
      -file "${SSL_DIR}/pem/${broker}.pem" >/dev/null 2>&1
    update_status "✅ OK"
  done

  rm -f "${SSL_DIR}/pem/kafka-cluster-ca.pem"

  cat "${SSL_DIR}"/pem/kafka-broker-*.pem > "${SSL_DIR}/pem/kafka-cluster-ca.pem"

  # echo "PEM bundle at ${SSL_DIR}/pem/kafka-cluster-ca.pem"
}

clean_ports() {
  custom_print "🚮 Cleaning used ports"
  for port in 19094 19095 19096 9093 9101 9102 9103 19084 29084 39084 8080 8081; do
    pid=$(ss -ltnp 2>/dev/null | grep ":$port " | awk -F',' '{print $2}' | awk '{print $1}' || true)
    [ -z "$pid" ] && pid=$(lsof -t -i:$port 2>/dev/null || true)

    if [ -n "$pid" ]; then
      echo "🗑️ Killing PID $pid (port $port)"
      kill -9 "$pid" || true
    fi
  done
  update_status "✅ OK"
}


start_docker() {
  custom_print "📦 Starting Service Group"
  # docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up -d --remove-orphans 2>/dev/null
  # docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up -d --remove-orphans
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up kafka-format -d --remove-orphans
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up kafka-broker-1, kafka-broker-2, kafka-broker-3 -d --remove-orphans
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up kafka-init -d --remove-orphans
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up kafka-format -d --remove-orphans
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up kafka-format -d --remove-orphans
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up kafka-format -d --remove-orphans
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" up kafka-format -d --remove-orphans
  update_status "✅ OK"

  custom_print "📦 Clear formatter"
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" rm -f -s kafka-format 2>/dev/null
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" rm -f -s kafka-init 2>/dev/null
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" rm -f -s schema-registry-healthcheck 2>/dev/null
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" rm -f -s prometheus-healthcheck 2>/dev/null
  # echo -ne "\033[1A\033[2K"
  # echo -ne "\033[1A\033[2K"
  # echo -ne "\033[1A\033[2K"
  # echo -ne "\033[1A\033[2K"
  update_status "✅ OK"
}

format_kafka() {
  custom_print "📦 Create Kafka storage format command" 
  mkdir -p "${DIR_SHELL}"
  props_shell="${DIR_SHELL}/kafka-broker-format.sh"
  : > "$props_shell"

  echo "#!/bin/sh" >> "$props_shell"
  echo "set -euo pipefail" >> "$props_shell"
  echo 'for broker in 1 2 3; do' >> "$props_shell"
  echo '  DATA_DIR="/var/lib/kafka/data-$broker"' >> "$props_shell"
  echo '  META_DIR="/var/lib/kafka/data-$broker/meta.properties"' >> "$props_shell"
  echo '  if [ -f "$META_DIR" ]; then' >> "$props_shell"
  echo '    echo "✅ Broker $broker already formatted (found $META_DIR)"' >> "$props_shell"
  echo '    continue' >> "$props_shell"
  echo '  fi' >> "$props_shell"
  echo '  chown -R appuser:appuser "$DATA_DIR"' >> "$props_shell"
  echo '  kafka-storage format \' >> "$props_shell"
  echo '    --ignore-formatted \' >> "$props_shell"
  echo '    --cluster-id '"$CLUSTER_ID"' \' >> "$props_shell"
  echo '    --config "/etc/kafka/properties/storage-$broker.properties"' >> "$props_shell"
  echo 'done' >> "$props_shell"
  chmod +x "$props_shell"
  update_status "✅ OK"

  for broker in 1 2 3; do
    custom_print "📦 Create Kafka storage format properties for [kafka-broker-${broker}]" 
    props="${DIR_PROPERTIES}/storage-${broker}.properties"
    : > "$props"

    echo "process.roles=broker,controller" >> "$props"
    echo "node.id=${broker}" >> "$props"
    echo "controller.listener.names=CONTROLLER" >> "$props"
    echo "controller.quorum.voters=1@kafka-broker-1:9093,2@kafka-broker-2:9093,3@kafka-broker-3:9093" >> "$props"
    echo "listeners=SASL_SSL://:9092,PLAINTEXT://:29092,CONTROLLER://:9093" >> "$props"
    echo "listener.security.protocol.map=SASL_SSL:SASL_SSL,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT" >> "$props"
    echo "log.dirs=/var/lib/kafka/data-${broker}" >> "$props"


    # docker run --rm -i \
    #   -v "${DIR_CERTIFICATES}/kafka-broker-${broker}:/etc/kafka/secrets" \
    #   -v "${props}:/etc/kafka/storage.properties" \
    #   confluentinc/cp-kafka:latest \
    #   kafka-storage format \
    #     --ignore-formatted \
    #     --cluster-id "$CLUSTER_ID" \
    #     --config /etc/kafka/storage.properties 2>/dev/null || true
    # echo -ne "\033[1A\033[2K"
    update_status "✅ OK"
  done
}

prepare_kafka_properties() {
  custom_print "📝 Prepare server properties"
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

  update_status "✅ OK"
}

prepare_client_properties() {
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
    custom_print "📝 Prepare client properties for [kafka-broker-${broker}]"
    # docker cp "${props}" $(docker ps -qf "name=kafka-broker-${broker}"):/etc/kafka/properties/client-cli.properties  >/dev/null 2>&1
    update_status "✅ OK"
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
  custom_print "📝 Create test topic"
  docker exec -e KAFKA_OPTS="" kafka-broker-1 kafka-topics \
    --bootstrap-server kafka-broker-1:9092 \
    --command-config /etc/kafka/properties/client-cli.properties \
    --create \
    --topic test.internal \
    --partitions 1 \
    --replication-factor 3 >/dev/null 2>&1 | grep -v '^WARN' || true
  echo -ne "\033[1A\033[2K"
  echo -ne "\033[1A\033[2K"
  update_status "✅ OK"

  custom_print "📝 Check for test topic"
  docker exec -e KAFKA_OPTS="" kafka-broker-1 kafka-topics \
    --bootstrap-server kafka-broker-1:29092 \
    --list >/dev/null 2>&1 || true
  echo -ne "\033[1A\033[2K"
  update_status "✅ OK"
  
}

create_scram_users() {
  custom_print "👤 Creating SCRAM users"

  mkdir -p "${DIR_SHELL}"
  props_shell="${DIR_SHELL}/kafka-create-user.sh"
  : > "$props_shell"

  echo "#!/bin/sh" >> "$props_shell"
  echo "set -euo pipefail" >> "$props_shell"
  echo "for i in 1 2 3 4 5; do" >> "$props_shell"
  echo "  kafka-configs --bootstrap-server kafka-broker-1:29092 --alter --add-config 'SCRAM-SHA-512=[password=confluent]' --entity-type users --entity-name kafkabroker && break" >> "$props_shell"
  echo "  echo \"Retrying in 5s...\"" >> "$props_shell"
  echo "  sleep 5" >> "$props_shell"
  echo "done" >> "$props_shell"
  echo "echo \"✅ SCRAM user created\"" >> "$props_shell"
  chmod +x "$props_shell"
  
  # docker exec -e KAFKA_OPTS="" kafka-broker-1 kafka-configs \
  #   --bootstrap-server kafka-broker-1:29092 \
  #   --alter \
  #   --add-config 'SCRAM-SHA-512=[password=confluent]' \
  #   --entity-type users \
  #   --entity-name kafkabroker 2>&1 || true
  # echo -ne "\033[1A\033[2K"


  update_status "✅ OK"
}

verify_broker_cert() {
  local broker=$1
  local dir="${SSL_DIR}/${broker}"
  custom_print "🔍 Verifying keystore for ${broker}"

  keytool -list -v \
      -keystore "${dir}/kafka.${broker}.keystore.jks" \
      -storepass "${BROKER_PASS}" \
      -alias "${broker}" >/dev/null 2>&1 || true
  update_status "✅ OK"
  # keytool -printcert -v -file "${dir}/${broker}.crt" >/dev/null 2>&1 || true
}

verify_client_cert() {
  custom_print "🔍 Verifying client keystore"
  keytool -list -v \
    -keystore "${CLIENT_DIR}/kafka.client.keystore.jks" \
    -storepass "${CLIENT_PASS}" \
    -alias kafka-client >/dev/null 2>&1 || true
  update_status "✅ OK"

  # keytool -printcert -v -file "${CLIENT_DIR}/kafka-client.crt" >/dev/null 2>&1 || true
}


verify_pem_bundle() {
  custom_print "🔍 Verifying PEM bundle"
  if [ -f "${SSL_DIR}/pem/kafka-cluster-ca.pem" ]; then
    openssl x509 -in "${SSL_DIR}/pem/kafka-cluster-ca.pem" -text -noout | grep -E 'Subject:|Issuer:|DNS:|IP Address:' >/dev/null 2>&1
    update_status "✅ OK"
  else
    update_status "❌ PEM bundle not found!"
  fi
}


generate_jaas() {
  if [ $# -ne 2 ]; then
    echo "Usage: generate_jaas <username> <password>"
    return 1
  fi

  local user="$1"
  local pass="$2"
  local output="${DIR_CONFIG}/kafka_server_jaas.conf"

  custom_print "📝 Generating JAAS config for broker with user=$user"

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

  update_status "✅ OK"
}

clean_all() {
  custom_print "🧹 Cleaning up containers and volumes"
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" down -v >/dev/null 2>&1 || true
  update_status "✅ OK"
}

reset_cluster() {
  custom_print "🧹 Resetting entire Kafka cluster (data + certs + volumes)"

  # 1️⃣ Stop containers & remove volumes
  docker-compose -p "message-broker" -f "$PWD/$TARGET_COMPOSE" down -v >/dev/null 2>&1 || true

  # 2️⃣ Remove host-mounted Kafka data directories
  for broker in 1 2 3; do
    local data_dir="${PWD}/kafka-data/broker-${broker}"  # adjust path if different
    if [ -d "$data_dir" ]; then
      rm -rf "$data_dir"
      echo "🗑️  Removed old data directory: $data_dir"
    fi
  done

  update_status "✅ OK"
}

hr() {
  local char="${1:--}"
  printf '%*s\n' "$(tput cols)" '' | tr ' ' "$char"
}

update_status() {
  local status="$1"
  local width=$(( $(tput cols) / 2 ))
  local dots=$((width - ${#LAST_LINE} - ${#status}))
  (( dots < 1 )) && dots=1
  echo -ne "\033[1A\033[2K"
  echo -ne "\r$LAST_LINE"
  printf ".%.0s" $(seq 1 $dots)
  echo "$status"
}

clean_all
reset_cluster
generate_ca
for b in kafka-broker-1 kafka-broker-2 kafka-broker-3; do
  generate_broker_cert "${b}"
  verify_broker_cert "${b}"
done
export_pem_bundle
generate_client_cert
verify_client_cert
verify_pem_bundle
clean_ports
generate_jaas kafkabroker confluent
prepare_kafka_properties
prepare_client_properties
format_kafka
create_scram_users
start_docker

# sleep 10
# create_test_topic

echo "✅ All done!"
