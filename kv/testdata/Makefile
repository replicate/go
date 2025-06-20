.PHONY: all
all: gen

.PHONY: clean
clean:
	rm -vf ca.key ca.crt server.key server.crt

.PHONY: gen
gen: ca.key ca.crt server.key server.crt

ca.key:
	openssl \
	    genrsa \
	    -aes256 \
	    -out ca.key \
	    4096

ca.crt:
	openssl req \
	    -x509 \
	    -new \
	    -nodes \
	    -key ca.key \
	    -sha256 \
	    -days 36500 \
	    -out ca.crt \
	    -subj "/C=US/ST=Pennsylvania/L=Pittsburgh/O=Consolidated Frobnicate/OU=Mafipulation"

server.key server.crt:
	openssl req \
	    -x509 \
	    -nodes \
	    -key ca.key \
	    -keyout server.key \
	    -out server.crt \
	    -days 36500 \
	    -addext "subjectAltName = DNS:Server" \
	    -subj "/C=US/ST=Pennsylvania/L=Pittsburgh/O=Consolidated Frobnicate/OU=Mafipulation"
