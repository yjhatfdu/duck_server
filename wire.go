package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const WireBufferSize = 4096

type Wire struct {
	conn     net.Conn
	buf      [WireBufferSize]byte
	writeBuf [WireBufferSize]byte
	lastMsg  *Message
	rd       io.Reader
	io.Writer
}

func (w *Wire) Read(p []byte) (int, error) {
	if w.rd == nil {
		panic("read from nil reader")
	}
	return io.ReadFull(w.rd, p)
}

func (w *Wire) ReadInt32() (int32, error) {
	_, err := w.Read(w.buf[0:4])
	b := w.buf
	_ = b[3] // bounds check hint to compiler; see golang.org/issue/14808
	return int32(b[3]) | int32(b[2])<<8 | int32(b[1])<<16 | int32(b[0])<<24, err
}

func (w *Wire) ReadStartUpMessage() (FirstMessage, error) {
	l, err := w.ReadInt32()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, l-4)
	_, err = w.Read(buf)
	version := binary.BigEndian.Uint32(buf)
	if version == StartupMessageVersion {
		sm := StartUpMessage{Data: buf}
		err = sm.Parse()
		return &sm, err
	}
	if version == CancelRequestCode {
		cm := CancelRequestMessage{Version: int32(version)}
		copy(cm.Key[:], buf[4:12])
		return &cm, nil
	}
	if version == SSLRequestCode {
		// doesn't support ssl now
		if _, err := w.Write([]byte{byte('N')}); err != nil {
			return nil, err
		}
		return w.ReadStartUpMessage()
	}
	return nil, fmt.Errorf("invalid version")
}

func (w *Wire) WriteAuthOK() error {
	_, err := w.Write([]byte{'R', 0, 0, 0, 8, 0, 0, 0, 0})
	return err
}

func (w *Wire) ReadMessage() (*Message, error) {
	if w.lastMsg != nil {
		if err := w.lastMsg.Skip(); err != nil {
			return nil, err
		}
	}
	buf := w.buf[0:5]
	_, err := w.Read(buf)
	if err != nil {
		return nil, err
	}
	t := MessageType(buf[0])
	_ = buf[4]
	l := int32(buf[4]) | int32(buf[3])<<8 | int32(buf[2])<<16 | int32(buf[1])<<24
	var m *Message
	if w.lastMsg != nil {
		m = w.lastMsg
		m.Length = l
		m.Typ = t
		m.buf = nil
	} else {
		m = &Message{
			Length: l,
			wire:   w,
			Typ:    t,
		}
		w.lastMsg = m
	}
	//logrus.Infof("read message: %v", m.String())
	return m, nil
}

func (w *Wire) WriteMessage(msg IMessage) error {
	//logrus.Infof("write message: %v", msg.String())
	return msg.Write(w)
}

func (w *Wire) ReadMessageInType(t MessageType) (*Message, error) {
	m, err := w.ReadMessage()
	if err != nil {
		return nil, err
	}
	if m.Typ != t {
		return nil, fmt.Errorf("message type not match")
	}
	return m, nil
}

func (w *Wire) ReadAuthMessage() (AuthenticationMessage, error) {
	m, err := w.ReadMessageInType('R')
	if err != nil {
		return nil, err
	}
	_, err = m.Read()
	if err != nil {
		return nil, err
	}
	t := binary.BigEndian.Uint32(m.buf)
	switch t {
	case 3:
		return &CleartextPasswordMessage{Message: m}, nil
	case 5:
		return &MD5PasswordMessage{Message: m, Salt: m.buf[4:]}, nil
	case 10:
		return ParseSaslMessage(m)
	case 11:
		return ParseSaslContinueMessage(m)
	}
	return nil, fmt.Errorf("unsupported auth type %d", t)
}
