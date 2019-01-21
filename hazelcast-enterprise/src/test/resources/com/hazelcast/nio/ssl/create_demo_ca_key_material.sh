#!/bin/bash

export PASSOUT=pass:123456

mkdir -p {root-ca,sub-ca}/{certs,db,private}

chmod 700 {root-ca,sub-ca}/private
touch {root-ca,sub-ca}/db/index
openssl rand -hex 16  > root-ca/db/serial
openssl rand -hex 16  > sub-ca/db/serial
echo 1001 > root-ca/db/crlnumber
echo 1001 > sub-ca/db/crlnumber

cat << 'EOT' > root-ca/root-ca.conf
[default]
name                    = root-ca
domain_suffix           = hazelcast-test.download
aia_url                 = http://$name.$domain_suffix/$name.crt
crl_url                 = http://$name.$domain_suffix/$name.crl
ocsp_url                = http://ocsp.$name.$domain_suffix:9080
default_ca              = ca_default
name_opt                = utf8,esc_ctrl,multiline,lname,align

[ca_dn]
countryName             = "US"
organizationName        = "Hazelcast Test"
commonName              = "Hazelcast Test Root CA"

[ca_default]
home                    = root-ca
database                = $home/db/index
serial                  = $home/db/serial
crlnumber               = $home/db/crlnumber
certificate             = $home/$name.crt
private_key             = $home/private/$name.key
RANDFILE                = $home/private/random
new_certs_dir           = $home/certs
unique_subject          = no
copy_extensions         = none
default_days            = 7300
default_crl_days        = 365
default_md              = sha256
policy                  = policy_c_o_match

[policy_c_o_match]
countryName             = match
stateOrProvinceName     = optional
organizationName        = match
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional

[req]
default_bits            = 4096
encrypt_key             = yes
default_md              = sha256
utf8                    = yes
string_mask             = utf8only
prompt                  = no
distinguished_name      = ca_dn
req_extensions          = ca_ext

[ca_ext]
basicConstraints        = critical,CA:true
keyUsage                = critical,keyCertSign,cRLSign
subjectKeyIdentifier    = hash

[sub_ca_ext]
authorityInfoAccess     = @issuer_info
authorityKeyIdentifier  = keyid:always
basicConstraints        = critical,CA:true,pathlen:0
crlDistributionPoints   = @crl_info
extendedKeyUsage        = clientAuth,serverAuth
keyUsage                = critical,keyCertSign,cRLSign
nameConstraints         = @name_constraints
subjectKeyIdentifier    = hash

[crl_info]
URI.0                   = $crl_url

[issuer_info]
caIssuers;URI.0         = $aia_url
OCSP;URI.0              = $ocsp_url

[name_constraints]
permitted;DNS.0=example.com
permitted;DNS.1=example.org
excluded;IP.0=0.0.0.0/0.0.0.0
excluded;IP.1=0:0:0:0:0:0:0:0/0:0:0:0:0:0:0:0

[ocsp_ext]
authorityKeyIdentifier  = keyid:always
basicConstraints        = critical,CA:false
extendedKeyUsage        = OCSPSigning
noCheck                 = yes
keyUsage                = critical,digitalSignature
subjectKeyIdentifier    = hash

EOT


cat << 'EOT' > sub-ca/sub-ca.conf
[default]
name                    = sub-ca
domain_suffix           = hazelcast-test.download
aia_url                 = http://$name.$domain_suffix/$name.crt
crl_url                 = http://$name.$domain_suffix/$name.crl
ocsp_url                = http://ocsp.$name.$domain_suffix:9081
default_ca              = ca_default
name_opt                = utf8,esc_ctrl,multiline,lname,align

[ca_dn]
countryName             = "US"
organizationName        = "Hazelcast Test"
commonName              = "Hazelcast Test Sub CA"

[ca_default]
home                    = sub-ca
database                = $home/db/index
serial                  = $home/db/serial
crlnumber               = $home/db/crlnumber
certificate             = $home/$name.crt
private_key             = $home/private/$name.key
RANDFILE                = $home/private/random
new_certs_dir           = $home/certs
unique_subject          = no
copy_extensions         = copy
default_days            = 7300
default_crl_days        = 30
default_md              = sha256
policy                  = policy_c_o_match

[policy_c_o_match]
countryName             = match
stateOrProvinceName     = optional
organizationName        = match
organizationalUnitName  = optional
commonName              = supplied
emailAddress            = optional

[req]
default_bits            = 2048
encrypt_key             = yes
default_md              = sha256
utf8                    = yes
string_mask             = utf8only
prompt                  = no
distinguished_name      = ca_dn

[server_ext]
authorityInfoAccess     = @issuer_info
authorityKeyIdentifier  = keyid:always
basicConstraints        = critical,CA:false
crlDistributionPoints   = @crl_info
extendedKeyUsage        = clientAuth,serverAuth
keyUsage                = critical,digitalSignature,keyEncipherment
subjectKeyIdentifier    = hash

[client_ext]
authorityInfoAccess     = @issuer_info
authorityKeyIdentifier  = keyid:always
basicConstraints        = critical,CA:false
crlDistributionPoints   = @crl_info
extendedKeyUsage        = clientAuth
keyUsage                = critical,digitalSignature
subjectKeyIdentifier    = hash

[crl_info]
URI.0                   = $crl_url

[issuer_info]
caIssuers;URI.0         = $aia_url
OCSP;URI.0              = $ocsp_url

[ocsp_ext]
authorityKeyIdentifier  = keyid:always
basicConstraints        = critical,CA:false
extendedKeyUsage        = OCSPSigning
keyUsage                = critical,digitalSignature
subjectKeyIdentifier    = hash

EOT

# Create Root CA
openssl req -new \
    -config root-ca/root-ca.conf \
    -out root-ca/root-ca.csr \
    -passout $PASSOUT \
    -keyout root-ca/private/root-ca.key

openssl ca -selfsign \
    -config root-ca/root-ca.conf \
    -passin $PASSOUT \
    -in root-ca/root-ca.csr \
    -out root-ca/root-ca.crt \
    -batch \
    -extensions ca_ext

openssl ca -gencrl \
    -config root-ca/root-ca.conf \
    -passin $PASSOUT \
    -out root-ca/root-ca.crl

# Create Sub CA

openssl req -new \
    -config sub-ca/sub-ca.conf \
    -out sub-ca/sub-ca.csr \
    -passout $PASSOUT \
    -keyout sub-ca/private/sub-ca.key

openssl ca \
    -config root-ca/root-ca.conf \
    -in sub-ca/sub-ca.csr \
    -passin $PASSOUT \
    -out sub-ca/sub-ca.crt \
    -batch \
    -extensions sub_ca_ext

# Create Server certificate
mkdir server
openssl req -new \
    -config sub-ca/sub-ca.conf \
    -out server/server.csr \
    -passout $PASSOUT \
    -subj "/C=US/O=Hazelcast Test/CN=server" \
    -keyout server/server.key

openssl ca \
    -config sub-ca/sub-ca.conf \
    -in server/server.csr \
    -passin $PASSOUT \
    -out server/server.crt \
    -batch \
    -extensions server_ext

# LE-like structures:
mkdir letsencrypt

cp server/server.crt letsencrypt/cert.pem
cat server/server.crt sub-ca/sub-ca.crt root-ca/root-ca.crt > letsencrypt/fullchain.pem
cat sub-ca/sub-ca.crt root-ca/root-ca.crt > letsencrypt/chain.pem

openssl pkcs8 \
    -in server/server.key \
    -passin $PASSOUT \
    -topk8 -nocrypt \
    -out letsencrypt/privkey.pem

openssl pkcs8 \
    -in letsencrypt/privkey.pem \
    -topk8 -v1 PBE-SHA1-3DES \
    -out letsencrypt/privkey.enc.pem \
    -passout pass:hazelcast

openssl pkcs12 -export \
    -out letsencrypt/server.p12 \
    -name server \
    -inkey letsencrypt/privkey.pem \
    -in letsencrypt/cert.pem \
    -certfile letsencrypt/chain.pem \
    -passout $PASSOUT

keytool -importkeystore \
    -srckeystore letsencrypt/server.p12 \
    -srcstoretype PKCS12 \
    -destkeystore letsencrypt/server.jks \
    -deststoretype JKS \
    -srcstorepass 123456 \
    -deststorepass 123456

keytool -importcert -trustcacerts \
    -alias letsencrypt-ca \
    -file letsencrypt/chain.pem \
    -keypass 123456 \
    -keystore letsencrypt/server.jks \
    -storepass 123456 \
    -storetype JKS \
    -noprompt

rm letsencrypt/server.p12
