package log

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/kr/pretty"
	"github.com/logrusorgru/aurora"
	"github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"
)

var PanicLevel = logrus.PanicLevel
var FatalLevel = logrus.FatalLevel
var ErrorLevel = logrus.ErrorLevel
var WarnLevel = logrus.WarnLevel
var InfoLevel = logrus.InfoLevel
var DebugLevel = logrus.DebugLevel
var TraceLevel = logrus.TraceLevel
var logger = logrus.New()

const defaultTimestampFormat = time.RFC3339

// Logger provides configuration for a logger.
type Logger struct {
	Level      string
	Formatter  string
	OutputFile string
	JSONFormat JSONFormatConfig
	TextFormat TextFormatConfig
}

// Entry is a logrus.Entry
type Entry = logrus.Entry

// JSONFormatConfig provides configuration for the JSON logger format.
type JSONFormatConfig struct {
	DisableTimestamp bool
	TimestampFormat  string
}

// TextFormatConfig provides configuration for the text logger format.
type TextFormatConfig struct {
	// Set to true to bypass checking for a TTY before outputting colors.
	ForceColors bool

	// Force disabling colors.
	DisableColors bool

	// Disable timestamp logging. useful when output is redirected to logging
	// system that already adds timestamps.
	DisableTimestamp bool

	// TimestampFormat to use for display when a full timestamp is printed
	TimestampFormat string

	// The fields are sorted by default for a consistent output. For applications
	// that log extremely frequently and don't use the JSON formatter this may not
	// be desired.
	DisableSorting bool

	Indent string
}

// DefaultLoggerConfig returns a Logger instance with default values.
func DefaultLoggerConfig() Logger {
	return Logger{
		Level:     "info",
		Formatter: "text",
		TextFormat: TextFormatConfig{
			TimestampFormat: defaultTimestampFormat,
		},
	}
}

type jsonFormatter struct {
	conf JSONFormatConfig
	fmt  *logrus.JSONFormatter
}

func (f *jsonFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	if f.fmt == nil {
		f.fmt = &logrus.JSONFormatter{
			DisableTimestamp: f.conf.DisableTimestamp,
			TimestampFormat:  f.conf.TimestampFormat,
		}
	}
	return f.fmt.Format(entry)
}

var jsonmar = jsonpb.Marshaler{
	Indent: "  ",
}

type textFormatter struct {
	TextFormatConfig
	json jsonFormatter
}

func checkIfTerminal(w io.Writer) bool {
	switch v := w.(type) {
	case *os.File:
		return terminal.IsTerminal(int(v.Fd()))
	default:
		return false
	}
}

func isColorTerminal(w io.Writer) bool {
	return checkIfTerminal(w) && (runtime.GOOS != "windows")
}

func (f *textFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	isColored := (f.ForceColors || isColorTerminal(entry.Logger.Out)) && !f.DisableColors
	if !isColored {
		return f.json.Format(entry)
	}

	b := entry.Buffer
	if b == nil {
		b = &bytes.Buffer{}
	}

	if !f.DisableTimestamp {
		entry.Data["time"] = entry.Time.Format(f.TimestampFormat)
	}

	var levelColor aurora.Color

	switch entry.Level {
	case logrus.DebugLevel:
		levelColor = aurora.MagentaFg
	case logrus.WarnLevel:
		levelColor = aurora.BrownFg
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		levelColor = aurora.RedFg
	default:
		levelColor = aurora.CyanFg
	}

	fmt.Fprintf(b, "%s%-20s %s\n", f.Indent, aurora.Colorize("message", levelColor), entry.Message)

	for _, k := range f.sortKeys(entry) {
		v := entry.Data[k]

		switch x := v.(type) {
		case string:
		case int:
		case int8:
		case int16:
		case int32:
		case int64:
		case uint8:
		case uint16:
		case uint32:
		case uint64:
		case complex64:
		case complex128:
		case float32:
		case float64:
		case bool:
		case proto.Message:
			if reflect.ValueOf(x).IsNil() {
				// do nothing
			} else if s, err := jsonmar.MarshalToString(x); err == nil {
				v = s
			} else {
				v = pretty.Sprint(x)
			}
		case fmt.Stringer:
		case error:
		default:
			v = pretty.Sprint(x)
		}

		if vString, ok := v.(string); ok {
			vParts := strings.Split(vString, "\n")
			padding := 21
			v = strings.Join(vParts, "\n"+strings.Repeat(" ", padding))
		}

		fmt.Fprintf(b, "%s%-20s %v\n", f.Indent, aurora.Colorize(k, levelColor), v)
	}

	b.WriteByte('\n')
	return b.Bytes(), nil
}

func (f *textFormatter) sortKeys(entry *logrus.Entry) []string {

	// Gather keys so they can be sorted
	keys := make([]string, 0, len(entry.Data))
	for k := range entry.Data {
		// "ns" (namespace) always comes first, so skip that one.
		if k != "ns" {
			keys = append(keys, k)
		}
	}

	if !f.DisableSorting {
		sort.Strings(keys)
	}
	return keys
}

// ConfigureLogger configures the global and local logrus logger
func ConfigureLogger(conf Logger) {
	switch strings.ToLower(conf.Level) {
	case "debug":
		logrus.SetLevel(logrus.DebugLevel)
		logger.SetLevel(logrus.DebugLevel)
	case "info":
		logrus.SetLevel(logrus.InfoLevel)
		logger.SetLevel(logrus.InfoLevel)
	case "warn", "warning":
		logrus.SetLevel(logrus.WarnLevel)
		logger.SetLevel(logrus.WarnLevel)
	case "error":
		logrus.SetLevel(logrus.ErrorLevel)
		logger.SetLevel(logrus.ErrorLevel)
	default:
		logrus.Warningf("Unknown log level: '%s'; defaulting to 'info'", conf.Level)
		logrus.SetLevel(logrus.InfoLevel)
		logger.SetLevel(logrus.InfoLevel)
	}

	switch strings.ToLower(conf.Formatter) {
	case "json":
		logrus.SetFormatter(&jsonFormatter{
			conf: conf.JSONFormat,
		})
		logger.SetFormatter(&jsonFormatter{
			conf: conf.JSONFormat,
		})

	// Default to text
	default:
		if strings.ToLower(conf.Formatter) != "text" {
			logrus.Warningf("Unknown log formatter: '%s'; defaulting to 'text'", conf.Formatter)
		}
		logrus.SetFormatter(&textFormatter{
			conf.TextFormat,
			jsonFormatter{
				conf: conf.JSONFormat,
			},
		})
		logger.SetFormatter(&textFormatter{
			conf.TextFormat,
			jsonFormatter{
				conf: conf.JSONFormat,
			},
		})
	}

	if conf.OutputFile != "" {
		logFile, err := os.OpenFile(
			conf.OutputFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666,
		)
		if err != nil {
			logrus.Errorf("Can't open log output file: %s", conf.OutputFile)
		} else {
			logrus.SetOutput(logFile)
			logger.SetOutput(logFile)
		}
	}
}

// Debug log message
func Debug(args ...interface{}) {
	logger.Debug(args...)
}

// Debugln log message
func Debugln(args ...interface{}) {
	logger.Debugln(args...)
}

// Debugf log message
func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args...)
}

// Info log message
func Info(args ...interface{}) {
	logger.Info(args...)
}

// Infoln log message
func Infoln(args ...interface{}) {
	logger.Infoln(args...)
}

// Infof log message
func Infof(format string, args ...interface{}) {
	logger.Infof(format, args...)
}

// Warning log message
func Warning(args ...interface{}) {
	logger.Warning(args...)
}

// Warningln log message
func Warningln(args ...interface{}) {
	logger.Warningln(fmt.Sprint(args...))
}

// Warningf log message
func Warningf(format string, args ...interface{}) {
	logger.Warningf(format, args...)
}

// Error log message
func Error(args ...interface{}) {
	logger.Error(args...)
}

// Errorln log message
func Errorln(args ...interface{}) {
	logger.Errorln(args...)
}

// Errorf log message
func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args...)
}

// Fields type, used to pass to `WithFields`.
type Fields = logrus.Fields

// WithFields creates an entry from the standard logger and adds multiple fields to it.
func WithFields(fields Fields) *logrus.Entry {
	return logger.WithFields(fields)
}

// GetLogger returns the configured logger instance
func GetLogger() *logrus.Logger {
	return logger
}

// Sub is a shortcut for log.WithFields(log.Fields{"namespace": ns}), it creates a new logger
// which inherits the parent's configuration but changes the namespace.
func Sub(ns string) *logrus.Entry {
	return logger.WithFields(Fields{"namespace": ns})
}
