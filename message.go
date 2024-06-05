package main

import (
	"bytes"
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
)

// MessageType https://www.postgresql.org/docs/16/protocol-message-formats.html
type MessageType byte

const (
	// Authentication : AuthenticationOk, AuthenticationCleartextPassword, AuthenticationMD5Password, AuthenticationSCMCredential, AuthenticationGSS, AuthenticationGSSContinue, AuthenticationSSPI, AuthenticationSASL, AuthenticationSASLContinue, AuthenticationSASLFinal
	Authentication           MessageType = 'R'
	BackendKeyData           MessageType = 'K'
	Bind                     MessageType = 'B'
	BindComplete             MessageType = '2'
	CancelRequest            MessageType = 1
	Close                    MessageType = 'C'
	CloseComplete            MessageType = '3'
	CommandComplete          MessageType = 'C'
	CopyData                 MessageType = 'd'
	CopyDone                 MessageType = 'c'
	CopyFail                 MessageType = 'f'
	CopyInResponse           MessageType = 'G'
	CopyOutResponse          MessageType = 'H'
	CopyBothResponse         MessageType = 'W'
	DataRow                  MessageType = 'D'
	Describe                 MessageType = 'D'
	EmptyQueryResponse       MessageType = 'I'
	ErrorResponse            MessageType = 'E'
	Execute                  MessageType = 'E'
	Flush                    MessageType = 'H'
	FunctionCall             MessageType = 'F'
	FunctionCallResponse     MessageType = 'V'
	GSSENCRequest            MessageType = '8'
	GSSResponse              MessageType = 'p'
	NegotiateProtocolVersion MessageType = 'v'
	NoData                   MessageType = 'n'
	NoticeResponse           MessageType = 'N'
	NotificationResponse     MessageType = 'A'
	ParameterDescription     MessageType = 't'
	ParameterStatus          MessageType = 'S'
	Parse                    MessageType = 'P'
	ParseComplete            MessageType = '1'
	PasswordMessage          MessageType = 'p'
	PortalSuspended          MessageType = 's'
	Query                    MessageType = 'Q'
	ReadyForQuery            MessageType = 'Z'
	RowDescription           MessageType = 'T'
	SASLInitialResponse      MessageType = 'p'
	SASLResponse             MessageType = 'p'
	SSLRequest               MessageType = '8'
	Sync                     MessageType = 'S'
	Terminate                MessageType = 'X'
	StartUp                  MessageType = 0
)

type IMessage interface {
	Write(writer io.Writer) error
	String() string
	Type() MessageType
	Skip() error
	Read() ([]byte, error)
}

const StartupMessageVersion = 196608
const CancelRequestCode = 80877102
const SSLRequestCode = 80877103

type FirstMessage interface {
	FirstMessageType() int
}

type StartUpMessage struct {
	Data       []byte
	Version    int32
	Parameters map[string]string
}

func (m *StartUpMessage) FirstMessageType() int {
	return StartupMessageVersion
}

func (m *StartUpMessage) Read() ([]byte, error) {
	return m.Data, nil
}

func (m *StartUpMessage) Parse() error {
	m.Version = int32(binary.BigEndian.Uint32(m.Data))
	if m.Version == StartupMessageVersion {
		m.Parameters = make(map[string]string)
		currentKey := ""
		lastIndex := 0
		for i, c := range m.Data[4:] {
			if c == 0 {
				if i == lastIndex {
					break
				}
				if currentKey == "" {
					currentKey = string(m.Data[4+lastIndex : 4+i])
				} else {
					m.Parameters[currentKey] = string(m.Data[4+lastIndex : 4+i])
					currentKey = ""
				}
				lastIndex = i + 1
			}
		}
	} else {
		return fmt.Errorf("invalid version")
	}
	return nil
}

func (m *StartUpMessage) String() string {
	return fmt.Sprintf("Version: %d, Parameters: %v", m.Version, m.Parameters)
}

func (m *StartUpMessage) Skip() error {
	return nil
}

func (m *StartUpMessage) Type() MessageType {
	switch m.Version {
	case StartupMessageVersion:
		return StartUp
	case CancelRequestCode:
		return CancelRequest
	}
	return StartUp
}

type CancelRequestMessage struct {
	Key     [8]byte
	Version int32
}

func (c CancelRequestMessage) FirstMessageType() int {
	return CancelRequestCode
}

type Message struct {
	wire   *Wire
	buf    []byte
	Length int32
	Typ    MessageType
}

func (m *Message) Skip() error {
	if m.wire == nil || m.buf != nil {
		return nil
	}
	_, err := io.CopyN(io.Discard, m.wire, int64(m.Length-4))
	return err
}

func (m *Message) Read() ([]byte, error) {
	if m.buf != nil {
		return m.buf, nil
	}
	if m.wire == nil {
		return m.buf, nil
	}
	var buf []byte
	if m.wire != nil && m.Length <= WireBufferSize+4 {
		buf = m.wire.buf[:m.Length-4]
	} else {
		buf = make([]byte, m.Length-4)
	}
	_, err := m.wire.Read(buf)
	if err != nil {
		return nil, err
	}
	m.buf = buf
	return buf, nil
}

func (m *Message) String() string {
	var d []byte
	if m.buf != nil {
		d = m.buf
	} else {
		var err error
		d, err = m.Read()
		if err != nil {
			return fmt.Sprintf("Type: %s, Length: %d, Error: %s", string(rune(m.Typ)), m.Length, err)
		}
	}
	return fmt.Sprintf("Type: %s, Length: %d, Content: %s", string(rune(m.Typ)), m.Length, string(d))
}

func (m *Message) Write(writer io.Writer) error {
	var header []byte
	if m.wire == nil {
		header = make([]byte, 5)
	} else {
		header = m.wire.writeBuf[:5]
	}

	header[0] = byte(m.Typ)
	binary.BigEndian.PutUint32(header[1:], uint32(m.Length))
	_, err := writer.Write(header)
	if err != nil {
		return err
	}
	if m.buf != nil {
		_, err = writer.Write(m.buf)
		return err
	} else {
		_, err := io.CopyN(writer, m.wire, int64(m.Length-4))
		return err
	}
}
func (m *Message) Type() MessageType {
	return m.Typ
}

const TransactionStatusIdle = 'I'
const TransactionStatusInTransaction = 'T'
const TransactionStatusFailed = 'E'

type ReadyForQueryMessage struct {
	Message
	Status byte
}

func (m *ReadyForQueryMessage) Write(writer io.Writer) error {
	var buf []byte
	if m.wire == nil {
		buf = make([]byte, 6)
	} else {
		buf = m.wire.writeBuf[:6]
	}
	buf[0] = byte(ReadyForQuery)
	binary.BigEndian.PutUint32(buf[1:], uint32(5))
	buf[5] = m.Status
	_, err := writer.Write(buf)
	return err
}

func (m *ReadyForQueryMessage) Type() MessageType {
	return ReadyForQuery
}

func (m *ReadyForQueryMessage) String() string {
	return fmt.Sprintf("Type: %s, Length: %d, Content: %s", "ReadyForQuery", 6, string(m.Status))
}

func NewMessage(typ MessageType, payload []byte) *Message {
	return &Message{Typ: typ, Length: int32(len(payload) + 4), buf: payload}
}

type AuthenticationMessage interface {
	AuthType() int
}

type CleartextPasswordMessage struct {
	*Message
}

func (m *CleartextPasswordMessage) AuthType() int {
	return 3
}

type MD5PasswordMessage struct {
	*Message
	Salt []byte
}

func (m *MD5PasswordMessage) AuthType() int {
	return 5
}

type SaslMessage struct {
	Message
	Mechanism []string
}

func (m *SaslMessage) AuthType() int {
	return 10
}

func ParseSaslMessage(message *Message) (*SaslMessage, error) {
	if message.buf == nil {
		_, err := message.Read()
		if err != nil {
			return nil, err
		}
	}
	magic := binary.BigEndian.Uint32(message.buf)
	if magic != 10 {
		return nil, fmt.Errorf("invalid SASL message")

	}
	buf := message.buf[4 : len(message.buf)-1]
	mech := make([]string, 0)
	for _, m := range bytes.Split(buf, []byte{0}) {
		mech = append(mech, string(m))
	}
	return &SaslMessage{Message: *message, Mechanism: mech}, nil
}

type SaslContinueMessage struct {
	*Message
	Data []byte
}

func (m *SaslContinueMessage) AuthType() int {
	return 11
}

func ParseSaslContinueMessage(message *Message) (*SaslContinueMessage, error) {
	if message.buf == nil {
		_, err := message.Read()
		if err != nil {
			return nil, err
		}
	}
	magic := binary.BigEndian.Uint32(message.buf)
	if magic != 11 {
		return nil, fmt.Errorf("invalid SASL message")
	}
	return &SaslContinueMessage{Message: message, Data: message.buf[4:]}, nil
}

type SaslFinalMessage struct {
	*Message
	Data []byte
}

func (m *SaslFinalMessage) AuthType() int {
	return 12
}

func ParseSaslFinalMessage(message *Message) (*SaslFinalMessage, error) {
	if message.buf == nil {
		_, err := message.Read()
		if err != nil {
			return nil, err
		}
	}
	magic := binary.BigEndian.Uint32(message.buf)
	if magic != 12 {
		return nil, fmt.Errorf("invalid SASL message")
	}
	return &SaslFinalMessage{Message: message, Data: message.buf[4:]}, nil
}

func cstr(s string) []byte {
	return append([]byte(s), 0)
}

func goString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	if b[0] == 0 {
		return ""
	}
	zeroIdx := bytes.IndexByte(b, 0)
	if zeroIdx == -1 {
		return string(b)
	}
	return string(b[:zeroIdx])
}

func cint32[T int | int32 | int64 | int8](i T) []byte {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(i))
	return buf
}

func cint16[T int8 | int16 | int32 | int64 | int](i T) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(i))
	return buf
}

type QueryMessage struct {
	*Message
	Query string
}

func ParseQueryMessage(message *Message) (QueryMessage, error) {
	d, err := message.Read()
	if err != nil {
		return QueryMessage{}, err
	}
	return QueryMessage{Message: message, Query: goString(d)}, nil
}

type ParseMessage struct {
	*Message
	Name          string
	Query         string
	ParameterOIDs []int32
}

func ParseParseMessage(message *Message) (ParseMessage, error) {
	d, err := message.Read()
	if err != nil {
		return ParseMessage{}, err
	}
	name := goString(d)
	d = d[len(name)+1:]
	query := goString(d)
	d = d[len(query)+1:]
	oidCount := int(binary.BigEndian.Uint16(d))
	d = d[2:]
	oids := make([]int32, 0)
	for i := 0; i < oidCount; i++ {
		oids = append(oids, int32(binary.BigEndian.Uint32(d)))
		d = d[4:]
	}
	return ParseMessage{Message: message, Name: name, Query: query, ParameterOIDs: oids}, nil
}

type BindMessage struct {
	*Message
	PortalName      string
	Statement       string
	ParameterOIDs   []int32
	ParameterValues []driver.Value
}

func tryParseValue(s string) driver.Value {
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	return s
}

func ParseBindMessage(message *Message) (BindMessage, error) {
	d, err := message.Read()
	if err != nil {
		return BindMessage{}, err
	}
	portalName := goString(d)
	d = d[len(portalName)+1:]
	statement := goString(d)
	d = d[len(statement)+1:]
	formatCount := int(binary.BigEndian.Uint16(d))
	d = d[2:]
	format := make([]int16, 0)
	for i := 0; i < formatCount; i++ {
		format = append(format, int16(binary.BigEndian.Uint16(d)))
		d = d[2:]
	}
	valueCount := int(binary.BigEndian.Uint16(d))
	d = d[2:]
	values := make([]driver.Value, 0)
	for i := 0; i < valueCount; i++ {
		l := int32(binary.BigEndian.Uint32(d))
		d = d[4:]
		if l == -1 {
			values = append(values, nil)
		} else {
			values = append(values, tryParseValue(string(d[:l])))
			d = d[l:]
		}
	}
	return BindMessage{Message: message, PortalName: portalName, Statement: statement, ParameterValues: values}, nil
}

type ExecuteMessage struct {
	*Message
	PortalName string
	MaxRows    int32
}

func ParseExecuteMessage(message *Message) (ExecuteMessage, error) {
	d, err := message.Read()
	if err != nil {
		return ExecuteMessage{}, err
	}
	portalName := goString(d)
	d = d[len(portalName)+1:]
	maxRows := int32(binary.BigEndian.Uint32(d))
	return ExecuteMessage{Message: message, PortalName: portalName, MaxRows: maxRows}, nil

}

type DescribeMessage struct {
	*Message
	Type byte
	Name string
}

func ParseDescribeMessage(message *Message) (DescribeMessage, error) {
	d, err := message.Read()
	if err != nil {
		return DescribeMessage{}, err
	}
	return DescribeMessage{Message: message, Type: d[0], Name: goString(d[1:])}, nil
}
