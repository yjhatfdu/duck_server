package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/xdg-go/scram"
	"regexp"
	"strconv"
	"strings"
)

const clientNonceLen = 18

func (c *PgConn) Auth(user string) error {
	if c.server.enableAuth == false {
		return c.NoAuth()
	}
	addr := strings.Split(c.wire.conn.RemoteAddr().String(), ":")[0]
	if addr == "localhost" || addr == "127.0.0.1" || addr == "::1" {
		return c.NoAuth()
	}
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
	var msg *Message
	var err error
	var saslInitialData []byte
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
	scramServer, err := scram.SHA256.NewServer(func(q string) (scram.StoredCredentials, error) {
		var pass string
		err := c.server.conn.QueryRow("select password from duckserver.users where username = $1", user).Scan(&pass)
		if err != nil {
			return scram.StoredCredentials{}, err
		}
		groups := regexp.MustCompile(`^SCRAM-SHA-256\$(\d+):(.*?)\$(.*?):(.*?)$`).FindStringSubmatch(pass)
		if len(groups) != 5 {
			return scram.StoredCredentials{}, errors.New("invalid password format")
		}
		salt, _ := base64.StdEncoding.DecodeString(groups[2])
		iterations := groups[1]
		iterationsint, _ := strconv.Atoi(iterations)
		storedKey, _ := base64.StdEncoding.DecodeString(groups[3])
		serverKey, _ := base64.StdEncoding.DecodeString(groups[4])
		return scram.StoredCredentials{
			StoredKey: storedKey,
			ServerKey: serverKey,
			KeyFactors: scram.KeyFactors{
				Salt:  string(salt),
				Iters: iterationsint,
			},
		}, nil
	})
	if err != nil {
		logrus.Infof("error: %v", err)
		return c.SendErrorResponse(fmt.Sprintf("password authentication failed for user %s", user))
	}
	conversation := scramServer.NewConversation()

	defer conversation.Done()
	resp, err := conversation.Step(string(saslInitialData))
	if err != nil {
		logrus.Infof("error: %v", err)
		return c.SendErrorResponse(fmt.Sprintf("password authentication failed for user %s", user))
	}
	if err := c.wire.WriteMessage(NewMessage('R', append(cint32(11), []byte(resp)...))); err != nil {
		return err
	}
	if msg, err = c.wire.ReadMessage(); err != nil {
		return err
	} else {
		if saslFinalMsg, err := ParseSASLResponseMessage(msg); err != nil {
			return nil
		} else {
			resp, err := conversation.Step(string(saslFinalMsg.Data))
			if err != nil {
				logrus.Infof("error: %v", err)
				return c.SendErrorResponse(fmt.Sprintf("password authentication failed for user %s", user))
			}
			if err = c.wire.WriteMessage(NewMessage('R', append(cint32(12), []byte(resp)...))); err != nil {
				return err
			}
		}
	}
	return c.wire.WriteAuthOK()
}

func computeHMAC(key, msg []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(msg)
	return mac.Sum(nil)
}
