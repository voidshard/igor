#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export SSL_CERT=ssl.cert
export SSL_KEY=ssl.key
export SSL_PEM=ssl.pem
export SSL_CSR=ssl.csr

# Generate ssl certs at random
#  from https://raymii.org/s/snippets/OpenSSL_generate_CSR_non-interactivemd.html
#
#    /C=NL: 2 letter ISO country code (Netherlands)
#    /ST=: State, Zuid Holland (South holland)
#    /L=: Location, city (Rotterdam)
#    /O=: Organization (Sparkling Network)
#    /OU=: Organizational Unit, Department (IT Department, Sales)
#    /CN=: Common Name, for a website certificate this is the FQDN. (ssl.raymii.org)
#
openssl req -nodes -newkey rsa:2048 -keyout ${DIR}/${SSL_KEY} -out ${DIR}/${SSL_CSR} -subj "/C=XX/ST=X/L=X/O=X/OU=X/CN=localhost"
openssl x509 -req -days 366 -in  ${DIR}/${SSL_CSR} -signkey ${DIR}/${SSL_KEY} -out  ${DIR}/${SSL_CERT}
cat ${DIR}/${SSL_CERT} ${DIR}/${SSL_KEY} >  ${DIR}/${SSL_PEM}

