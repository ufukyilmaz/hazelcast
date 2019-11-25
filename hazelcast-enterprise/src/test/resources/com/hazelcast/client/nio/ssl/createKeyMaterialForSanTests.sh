#!/bin/bash

# This script generates keystores which are used in TLS host verification tests
# Possible improvement - generate the key material by using a Maven keytool plugin.


KEYSTORE_PASSWORD=123456

# key material for TlsHostVerifierTest.java

function createKeystore
{
  if [ $# -gt 2 ]; then
    keytool -genkeypair -keystore $1 -storepass $KEYSTORE_PASSWORD -keypass $KEYSTORE_PASSWORD -storetype PKCS12 -validity 7300 -keyalg RSA -keysize 1024 \
      -alias test -dname "$2" -ext "$3"
  else
    keytool -genkeypair -keystore $1 -storepass $KEYSTORE_PASSWORD -keypass $KEYSTORE_PASSWORD -storetype PKCS12 -validity 7300 -keyalg RSA -keysize 1024 \
      -alias test -dname "$2"
  fi
  keytool -export -alias test -keystore $1 -storepass $KEYSTORE_PASSWORD -file $1.crt
  keytool -importcert -noprompt -alias $1 -keystore truststore.p12 -storepass $KEYSTORE_PASSWORD -file $1.crt
  rm $1.crt
}

createKeystore tls-host-loopback-san.p12 "O=Hazelcast" "SAN=ip:127.0.0.1,ip:::1,dns:localhost,dns:localhost.localdomain"
createKeystore tls-host-loopback-san-dns.p12 "O=Hazelcast" "SAN=dns:localhost"
createKeystore tls-host-not-our-san.p12 "cn=public-dns" "SAN=ip:8.8.8.8"
createKeystore tls-host-loopback-cn.p12 "cn=127.0.0.1" "SAN=email:info@hazelcast.com"
createKeystore tls-host-no-entry.p12 "O=Hazelcast"
  