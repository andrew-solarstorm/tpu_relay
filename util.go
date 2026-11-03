package tpu_relay

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/pem"
	"math/big"
	"time"
)

const DEFAULT_COMMON_NAME = "Solana node"

func newDummyX509Certificate() (certPem, privPem []byte, err error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, err
	}

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, nil, err
	}

	tmpl := x509.Certificate{
		Version:            3,
		SerialNumber:       serialNumber,
		Subject:            pkix.Name{CommonName: DEFAULT_COMMON_NAME},
		Issuer:             pkix.Name{CommonName: DEFAULT_COMMON_NAME},
		SignatureAlgorithm: x509.PureEd25519,
		NotBefore:          time.Date(1975, time.January, 1, 0, 0, 0, 0, time.UTC),
		NotAfter:           time.Date(4096, time.January, 1, 0, 0, 0, 0, time.UTC),
	}

	tmpl.ExtraExtensions = append(tmpl.ExtraExtensions, createSubjectAltNameExtension())

	derBytes, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, publicKey, privateKey)
	if err != nil {
		return nil, nil, err
	}

	privateKeyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, nil, err
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateKeyBytes}),
		nil
}

func createSubjectAltNameExtension() pkix.Extension {
	return pkix.Extension{
		Id:    asn1.ObjectIdentifier{2, 5, 29, 17},
		Value: []byte{48, 6, 135, 4, 0, 0, 0, 0}, // tag 7, "0.0.0.0"
	}
}

func createTlsConfig() (*tls.Config, error) {
	certPem, privPem, err := newDummyX509Certificate()
	if err != nil {
		return nil, err
	}
	cert, err := tls.X509KeyPair(certPem, privPem)
	if err != nil {
		return nil, err
	}

	return &tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"solana-tpu"},
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS13,
		ServerName:         "server",
	}, nil
}
