package kinetic

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"

	proto "github.com/golang/protobuf/proto"
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type networkService struct {
	conn   net.Conn
	seq    int64
	connId int64
	option ClientOptions
}

func newNetworkService(op ClientOptions) (*networkService, error) {
	target := fmt.Sprintf("%s:%d", op.Host, op.Port)
	conn, err := net.Dial("tcp", target)
	if err != nil {
		return nil, err
	}

	s := &networkService{conn: conn, seq: 1, connId: 0, option: op}

	_, _, _, err = s.receive()
	if err != nil {
		klog.Error("Can't establish connection to %s", op.Host)
		return nil, err
	}

	go s.run()

	return s, nil
}

func (s *networkService) run() {

}

func (s *networkService) send(msg *kproto.Message, value []byte) error {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	header := make([]byte, 9)
	header[0] = 'F' // Magic number
	binary.BigEndian.PutUint32(header[1:5], uint32(len(msgBytes)))
	binary.BigEndian.PutUint32(header[5:9], uint32(len(value)))

	var cnt int
	cnt, err = s.conn.Write(header)
	if err != nil {
		klog.Error("Write header fail")
		return err
	}
	if cnt != len(header) {
		klog.Fatal("Write header fail")
	}

	cnt, err = s.conn.Write(msgBytes)
	if err != nil {
		klog.Error("Write message fail")
		return err
	}
	if cnt != len(msgBytes) {
		klog.Fatal("Write message fail")
	}

	cnt, err = s.conn.Write(value)
	if err != nil {
		klog.Error("Write message fail")
		return err
	}
	if cnt != len(value) {
		klog.Fatal("Write value fail")
	}

	return nil
}

func (s *networkService) receive() (*kproto.Message, *kproto.Command, []byte, error) {
	header := make([]byte, 9)

	_, err := io.ReadFull(s.conn, header[0:])
	if err != nil {
		klog.Error("Receive protocol header error : ", err)
		return nil, nil, nil, err
	}

	magic := header[0]
	if magic != 'F' {
		klog.Panic("Received package has invalid magic number")
	}

	protoLen := int(binary.BigEndian.Uint32(header[1:5]))
	valueLen := int(binary.BigEndian.Uint32(header[5:9]))

	protoBuf := make([]byte, protoLen)
	_, err = io.ReadFull(s.conn, protoBuf)
	if err != nil {
		klog.Error("Receive protocol Message error : ", err)
		return nil, nil, nil, err
	}

	msg := &kproto.Message{}
	err = proto.Unmarshal(protoBuf, msg)
	if err != nil {
		klog.Error("Received packet can't unmarshal to Kinetic Message", err)
		return nil, nil, nil, err
	}

	if validate_hmac(msg, s.option.Hmac) == false {
		klog.Error("Received packet has invalid HMAC")
		return nil, nil, nil, err
	}

	cmd := &kproto.Command{}
	err = proto.Unmarshal(msg.CommandBytes, cmd)
	if err != nil {
		klog.Error("Received packet can't unmarshal to Kinetic Command: ", err)
		return nil, nil, nil, err
	}

	if cmd.Header != nil && cmd.Header.ConnectionID != nil {
		s.connId = cmd.GetHeader().GetConnectionID()
	}

	if valueLen > 0 {
		valueBuf := make([]byte, valueLen)
		_, err = io.ReadFull(s.conn, valueBuf)
		if err != nil {
			klog.Error("Recive value error : ", err)
			return nil, nil, nil, err
		}

		return msg, cmd, valueBuf, nil
	}

	return msg, cmd, nil, nil
}

func (s *networkService) close() {
	s.conn.Close()
	klog.Info("Connection to %s closed", s.option.Host)
}
