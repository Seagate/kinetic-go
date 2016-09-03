package kinetic

import (
	kproto "github.com/yongzhy/kinetic-go/proto"
)

type LogType int32

const (
	_                 LogType = iota
	LOG_UTILIZATIONS  LogType = iota
	LOG_TEMPERATURES  LogType = iota
	LOG_CAPACITIES    LogType = iota
	LOG_CONFIGURATION LogType = iota
	LOG_STATISTICS    LogType = iota
	LOG_MESSAGES      LogType = iota
	LOG_LIMITS        LogType = iota
	LOG_DEVICE        LogType = iota
)

var strLogType = map[LogType]string{
	LOG_UTILIZATIONS:  "LOG_UTILIZATIONS",
	LOG_TEMPERATURES:  "LOG_TEMPERATURES",
	LOG_CAPACITIES:    "LOG_CAPACITIES",
	LOG_CONFIGURATION: "LOG_CONFIGURATION",
	LOG_STATISTICS:    "LOG_STATISTICS",
	LOG_MESSAGES:      "LOG_MESSAGES",
	LOG_LIMITS:        "LOG_LIMITS",
	LOG_DEVICE:        "LOG_DEVICE",
}

func (l LogType) String() string {
	str, ok := strLogType[l]
	if ok {
		return str
	}
	return "Unknown LogType"
}

func convertLogTypeToProto(l LogType) kproto.Command_GetLog_Type {
	ret := kproto.Command_GetLog_INVALID_TYPE
	switch l {
	case LOG_UTILIZATIONS:
		ret = kproto.Command_GetLog_UTILIZATIONS
	case LOG_TEMPERATURES:
		ret = kproto.Command_GetLog_TEMPERATURES
	case LOG_CAPACITIES:
		ret = kproto.Command_GetLog_CAPACITIES
	case LOG_CONFIGURATION:
		ret = kproto.Command_GetLog_CONFIGURATION
	case LOG_STATISTICS:
		ret = kproto.Command_GetLog_STATISTICS
	case LOG_MESSAGES:
		ret = kproto.Command_GetLog_MESSAGES
	case LOG_LIMITS:
		ret = kproto.Command_GetLog_LIMITS
	case LOG_DEVICE:
		ret = kproto.Command_GetLog_DEVICE
	}
	return ret
}

func convertLogTypeFromProto(l kproto.Command_GetLog_Type) LogType {
	var ret LogType
	switch l {
	case kproto.Command_GetLog_UTILIZATIONS:
		ret = LOG_UTILIZATIONS
	case kproto.Command_GetLog_TEMPERATURES:
		ret = LOG_TEMPERATURES
	case kproto.Command_GetLog_CAPACITIES:
		ret = LOG_CAPACITIES
	case kproto.Command_GetLog_CONFIGURATION:
		ret = LOG_CONFIGURATION
	case kproto.Command_GetLog_STATISTICS:
		ret = LOG_STATISTICS
	case kproto.Command_GetLog_MESSAGES:
		ret = LOG_MESSAGES
	case kproto.Command_GetLog_LIMITS:
		ret = LOG_LIMITS
	case kproto.Command_GetLog_DEVICE:
		ret = LOG_DEVICE
	}
	return ret
}

type UtilizationLog struct {
	Name  string
	Value float32
}

type TemperatureLog struct {
	Name    string
	Current float32
	Minimum float32
	Maximum float32
	Target  float32
}

type CapacityLog struct {
	CapacityInBytes uint64
	PortionFull     float32
}

type ConfigurationInterface struct {
	Name     string
	MAC      []byte
	Ipv4Addr []byte
	Ipv6Addr []byte
}

type ConfigurationLog struct {
	Vendor                  string
	Model                   string
	SerialNumber            []byte
	WorldWideName           []byte
	Version                 string
	CompilationDate         string
	SourceHash              string
	ProtocolVersion         string
	ProtocolCompilationDate string
	ProtocolSourceHash      string
	Interface               []ConfigurationInterface
	Port                    int32
	TlsPort                 int32
}

type StatisticsLog struct {
	// TODO: Would it better just use the protocol Command_MessageType?
	Type  MessageType
	Count uint64
	Bytes uint64
}

type LimitsLog struct {
	MaxKeySize                  uint32
	MaxValueSize                uint32
	MaxVersionSize              uint32
	MaxTagSize                  uint32
	MaxConnections              uint32
	MaxOutstandingReadRequests  uint32
	MaxOutstandingWriteRequests uint32
	MaxMessageSize              uint32
	MaxKeyRangeCount            uint32
	MaxIdentityCount            uint32
	MaxPinSize                  uint32
	MaxOperationCountPerBatch   uint32
	MaxBatchCountPerDevice      uint32
}

type DeviceLog struct {
	Name []byte
}

type Log struct {
	Utilizations  []UtilizationLog
	Temperatures  []TemperatureLog
	Capacity      CapacityLog
	Configuration ConfigurationLog
	Statistics    []StatisticsLog
	Messages      []byte
	Limits        LimitsLog
	Device        DeviceLog
}

func getUtilizationLogFromProto(getlog *kproto.Command_GetLog) []UtilizationLog {
	utils := getlog.GetUtilizations()
	if utils != nil {
		ulog := make([]UtilizationLog, len(utils))
		for k, v := range utils {
			ulog[k] = UtilizationLog{
				Name:  v.GetName(),
				Value: v.GetValue(),
			}
		}
		return ulog
	} else {
		return nil
	}
}

func getTemperatureLogFromProto(getlog *kproto.Command_GetLog) []TemperatureLog {
	temps := getlog.GetTemperatures()
	if temps != nil {
		templog := make([]TemperatureLog, len(temps))
		for k, v := range temps {
			templog[k] = TemperatureLog{
				Name:    v.GetName(),
				Current: v.GetCurrent(),
				Minimum: v.GetMinimum(),
				Maximum: v.GetMaximum(),
				Target:  v.GetTarget(),
			}
		}
		return templog
	} else {
		return nil
	}
}

func getCapacityLogFromProto(getlog *kproto.Command_GetLog) CapacityLog {
	var log CapacityLog
	capacity := getlog.GetCapacity()
	if capacity != nil {
		log = CapacityLog{
			CapacityInBytes: capacity.GetNominalCapacityInBytes(),
			PortionFull:     capacity.GetPortionFull(),
		}
	}
	return log
}

func getConfigurationInterfaceFromProto(conf *kproto.Command_GetLog_Configuration) []ConfigurationInterface {
	pinf := conf.GetInterface()
	if pinf != nil {
		inf := make([]ConfigurationInterface, len(pinf))
		for k, v := range pinf {
			inf[k] = ConfigurationInterface{
				Name:     v.GetName(),
				MAC:      v.GetMAC(),
				Ipv4Addr: v.GetIpv4Address(),
				Ipv6Addr: v.GetIpv6Address(),
			}
		}
		return inf
	} else {
		return nil
	}
}

func getConfigurationLogFromProto(getlog *kproto.Command_GetLog) ConfigurationLog {
	var log ConfigurationLog
	conf := getlog.GetConfiguration()
	if conf != nil {
		log = ConfigurationLog{
			Vendor:                  conf.GetVendor(),
			Model:                   conf.GetModel(),
			SerialNumber:            conf.GetSerialNumber(),
			WorldWideName:           conf.GetWorldWideName(),
			Version:                 conf.GetVersion(),
			CompilationDate:         conf.GetCompilationDate(),
			SourceHash:              conf.GetSourceHash(),
			ProtocolVersion:         conf.GetProtocolVersion(),
			ProtocolCompilationDate: conf.GetProtocolCompilationDate(),
			ProtocolSourceHash:      conf.GetProtocolSourceHash(),
			Interface:               getConfigurationInterfaceFromProto(conf),
			Port:                    conf.GetPort(),
			TlsPort:                 conf.GetTlsPort(),
		}
	}
	return log
}

func getStatisticsLogFromProto(getlog *kproto.Command_GetLog) []StatisticsLog {
	statics := getlog.GetStatistics()
	if statics != nil {
		slog := make([]StatisticsLog, len(statics))
		for k, v := range statics {
			slog[k] = StatisticsLog{
				Type:  convertMessageTypeFromProto(v.GetMessageType()),
				Count: v.GetCount(),
				Bytes: v.GetBytes(),
			}
		}
		return slog
	} else {
		return nil
	}
}

func getLogMessageFromProto(getlog *kproto.Command_GetLog) []byte {
	return getlog.GetMessages()
}

func getLimitsLogFromProto(getlog *kproto.Command_GetLog) LimitsLog {
	var log LimitsLog
	limits := getlog.GetLimits()
	if limits != nil {
		log = LimitsLog{
			MaxKeySize:                  limits.GetMaxKeySize(),
			MaxValueSize:                limits.GetMaxValueSize(),
			MaxVersionSize:              limits.GetMaxVersionSize(),
			MaxTagSize:                  limits.GetMaxTagSize(),
			MaxConnections:              limits.GetMaxConnections(),
			MaxOutstandingReadRequests:  limits.GetMaxOutstandingReadRequests(),
			MaxOutstandingWriteRequests: limits.GetMaxOutstandingWriteRequests(),
			MaxMessageSize:              limits.GetMaxMessageSize(),
			MaxKeyRangeCount:            limits.GetMaxKeyRangeCount(),
			MaxIdentityCount:            limits.GetMaxIdentityCount(),
			MaxPinSize:                  limits.GetMaxPinSize(),
			MaxOperationCountPerBatch:   limits.GetMaxOperationCountPerBatch(),
			MaxBatchCountPerDevice:      limits.GetMaxBatchCountPerDevice(),
		}
	}
	return log
}

func getDeviceLogFromProto(getlog *kproto.Command_GetLog) DeviceLog {
	//TODO: Need more details
	return DeviceLog{
		Name: getlog.GetDevice().GetName(),
	}
}

func getLogFromProto(resp *kproto.Command) Log {
	var logs Log

	getlog := resp.GetBody().GetGetLog()

	if getlog != nil {
		logs = Log{
			Utilizations:  getUtilizationLogFromProto(getlog),
			Temperatures:  getTemperatureLogFromProto(getlog),
			Capacity:      getCapacityLogFromProto(getlog),
			Configuration: getConfigurationLogFromProto(getlog),
			Statistics:    getStatisticsLogFromProto(getlog),
			Messages:      getLogMessageFromProto(getlog),
			Limits:        getLimitsLogFromProto(getlog),
			Device:        getDeviceLogFromProto(getlog),
		}
	}
	return logs
}
