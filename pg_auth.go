package main

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"regexp"
)

const clientNonceLen = 18

func (c *PgConn) Auth(user string) error {
	if c.server.enableAuth == false {
		return c.NoAuth()
	}
	//addr := strings.Split(c.wire.conn.RemoteAddr().String(), ":")[0]
	//if addr == "localhost" || addr == "127.0.0.1" || addr == "::1" {
	//	return c.NoAuth()
	//}
	return c.ScramSha256Auth(user)
}

func (c *PgConn) NoAuth() error {
	return c.wire.WriteAuthOK()
}

func (c *PgConn) ScramSha256Auth(user string) error {
	authSaslMsg := NewAuthenticationSASLMessage([]string{"SCRAM-SHA-256"})
	if err := c.wire.WriteMessage(authSaslMsg); err != nil {
		return err
	}
	var saslInitialData []byte
	var msg *Message
	var err error
	if msg, err = c.wire.ReadMessage(); err != nil {
		return err
	} else {
		if saslInitialMsg, err := ParseSASLInitialResponseMessage(msg); err != nil {
			return nil
		} else {
			if saslInitialMsg.Mechanism != "SCRAM-SHA-256" {
				logrus.Errorf("invalid mechanism: %s", saslInitialMsg.Mechanism)
				return errors.New("invalid mechanism")
			}
			saslInitialData = saslInitialMsg.Initial
		}
	}
	if saslInitialData == nil {
		return errors.New("invalid initial data")
	}
	clientNonce, err := readClientNonce(saslInitialData)
	if err != nil {
		return err
	}
	serverNonce := make([]byte, clientNonceLen)
	_, _ = rand.Read(serverNonce)
	serverNonceStr := base64.RawStdEncoding.EncodeToString(serverNonce)
	clientAndServerNonce := clientNonce + serverNonceStr
	pgpassword, err := c.server.GetPassword(user)
	if err != nil {
		return c.SendErrorResponse(fmt.Sprintf("password authentication failed for user %s", user))
	}
	//parse password
	groups := regexp.MustCompile(`^SCRAM-SHA-256\$(\d+):(.*?)\$(.*?):(.*?)$`).FindStringSubmatch(pgpassword)
	if len(groups) != 5 {
		logrus.Warnf("invalid password format: %s", pgpassword)
		return c.SendErrorResponse(fmt.Sprintf("password authentication failed for user %s", user))
	}
	salt := groups[2]
	iterations := groups[1]
	serverFirstResp := fmt.Sprintf("r=%s,s=%s,i=%s", clientAndServerNonce, salt, iterations)
	if err := c.wire.WriteMessage(NewMessage('R', append(cint32(11), []byte(serverFirstResp)...))); err != nil {
		return err
	}
	var clientFinalData map[string]string
	if msg, err = c.wire.ReadMessage(); err != nil {
		return err
	} else {
		if saslFinalMsg, err := ParseSASLResponseMessage(msg); err != nil {
			return nil
		} else {
			logrus.Infoln(saslFinalMsg)
			clientFinalData = parseSaslData(saslFinalMsg.Data)
		}
	}
	storedKey, _ := base64.StdEncoding.DecodeString(groups[3])
	clientProof, _ := base64.StdEncoding.DecodeString(clientFinalData["p"])
	serverKey, _ := base64.StdEncoding.DecodeString(groups[4])
	authMessage := "n=,r=" + clientNonce + "," + serverFirstResp + "," + fmt.Sprintf("c=biws,r=%s", clientAndServerNonce)
	clientSignature := computeHMAC(storedKey[:], []byte(authMessage))
	clientKey := make([]byte, len(clientSignature))
	for i := 0; i < len(clientSignature); i++ {
		clientKey[i] = clientProof[i] ^ clientSignature[i]
	}
	storedKeyComputed := sha256.Sum256(clientKey)
	if !bytes.Equal(storedKey, storedKeyComputed[:]) {
		return c.SendErrorResponse(fmt.Sprintf("password authentication failed for user %s", user))
	}
	serverSignature := computeServerSignature(serverKey, []byte(authMessage))
	if err = c.wire.WriteMessage(NewMessage('R', append(cint32(11), []byte("v="+serverSignature)...))); err != nil {
		return err
	}
	return c.wire.WriteAuthOK()
}

func parseSaslData(data []byte) map[string]string {
	m := make(map[string]string)
	for _, kv := range bytes.Split(data, []byte{','}) {
		kv := bytes.SplitN(kv, []byte{'='}, 2)
		if len(kv) != 2 {
			continue
		}
		m[string(kv[0])] = string(kv[1])
	}
	return m
}

func readClientNonce(data []byte) (string, error) {
	m := parseSaslData(data)
	if v, ok := m["r"]; ok {
		return v, nil
	}
	return "", errors.New("not found")
}

func computeServerSignature(serverKey []byte, authMessage []byte) string {
	serverSignature := computeHMAC(serverKey, authMessage)
	return base64.StdEncoding.EncodeToString(serverSignature)
}
func computeHMAC(key, msg []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}
