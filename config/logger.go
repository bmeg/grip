package config

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
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh/terminal"
)

const defaultTimestampFormat = time.RFC3339

// Logger provides configuration for a logger.
type Logger struct {
	Level      string
	Formatter  string
	OutputFile string
	JSONFormat JSONFormatConfig
	TextFormat TextFormatConfig
}

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
	fmt  *log.JSONFormatter
}

func (f *jsonFormatter) Format(entry *log.Entry) ([]byte, error) {
	if f.fmt == nil {
		f.fmt = &log.JSONFormatter{
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

func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
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
	case log.DebugLevel:
		levelColor = aurora.MagentaFg
	case log.WarnLevel:
		levelColor = aurora.BrownFg
	case log.ErrorLevel, log.FatalLevel, log.PanicLevel:
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

func (f *textFormatter) sortKeys(entry *log.Entry) []string {

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

// ConfigureLogger configures the global logrus logger
func ConfigureLogger(conf Logger) {
	switch strings.ToLower(conf.Level) {
	case "debug":
		log.SetLevel(log.DebugLevel)
	case "info":
		log.SetLevel(log.InfoLevel)
	case "warn", "warning":
		log.SetLevel(log.WarnLevel)
	case "error":
		log.SetLevel(log.ErrorLevel)
	default:
		log.Warningf("Unknown log level: '%s'; defaulting to 'info'", conf.Level)
		log.SetLevel(log.InfoLevel)
	}

	switch strings.ToLower(conf.Formatter) {
	case "json":
		log.SetFormatter(&jsonFormatter{
			conf: conf.JSONFormat,
		})

	// Default to text
	default:
		if strings.ToLower(conf.Formatter) != "text" {
			log.Warningf("Unknown log formatter: '%s'; defaulting to 'text'", conf.Formatter)
		}
		log.SetFormatter(&textFormatter{
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
			log.Error("Can't open log output", "output", conf.OutputFile)
		} else {
			log.SetOutput(logFile)
		}
	}
}
