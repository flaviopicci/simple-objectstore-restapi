package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/flaviopicci/simple-objectstore-restapi/internals/filestore"
	"github.com/flaviopicci/simple-objectstore-restapi/internals/memstore"
	"github.com/flaviopicci/simple-objectstore-restapi/internals/rest"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"net/http"
	"os"
	"os/signal"
	"path"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const defaultListenAddr = "0.0.0.0"
const defaultListenPort = 8080

var listenAddrRe = regexp.MustCompile(`([\w.-]+:)?([0-9]+)?`)

func main() {
	// Define command line parameters
	pflag.BoolP("verbose", "v", false, "Print verbose output")
	pflag.StringP("config", "c", "", "Path to the configuration file")

	pflag.StringP("listen-address", "l", "", "Address to listen to in the form of <port> or <address>:<port>")
	pflag.BoolP("persist", "p", false, "Whether to use persistent storage to store objects")
	pflag.String("data-path", "", "Path to folder of persistent data")

	pflag.Parse()

	// Bind Viper parameters with command line ones
	v := viper.NewWithOptions(viper.EnvKeyReplacer(envReplacer{old: ".", new: "_"}))
	v.SetDefault("config", "config")
	v.SetDefault("listen_address", fmt.Sprintf("%s:%d", defaultListenAddr, defaultListenPort))
	v.SetDefault("data_path", ".")

	_ = v.BindPFlag("verbose", pflag.Lookup("verbose"))
	_ = v.BindPFlag("config", pflag.Lookup("config"))

	_ = v.BindPFlag("listen_address", pflag.Lookup("listen-address"))
	_ = v.BindPFlag("persist", pflag.Lookup("persist"))
	_ = v.BindPFlag("data_path", pflag.Lookup("data-path"))

	// Bind Viper parameters with env variables prefixed with `OBJSTORE_`
	v.SetEnvPrefix("objstore_")
	v.AutomaticEnv()

	// Create logger
	verbose := v.GetBool("verbose")
	logger := getLogger(verbose)

	// Load config file
	configFile := v.GetString("config")
	v.SetConfigName(strings.Split(path.Base(configFile), ".")[0])
	v.AddConfigPath(path.Dir(configFile))
	v.AddConfigPath(".")
	v.AddConfigPath("configs")

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			logger.Debug("No configuration file found")
		} else {
			logger.Fatalf("Could not read config file(s): %v", err)
		}
	}

	// Choose storage type, in memory or persistent
	var store rest.ObjectStore
	if v.GetBool("persist") {
		dataPath := v.GetString("data_path")
		err := os.MkdirAll(dataPath, 0755)
		if err != nil {
			logger.Fatalf("Cannot create storage folder: %v", err)
		}
		if store, err = filestore.NewStore(v.GetString("data_path")); err != nil {
			logger.Fatalf("Cannot initialize file storage: %v", err)
		}
		logger.Infof("Use persistent storage in %q", dataPath)
	} else {
		store = memstore.NewStore()
		logger.Info("Using in memory store")
	}

	// Create HTTP router
	r := rest.NewRouter(store, 0, logger)

	// Get listen address
	rawListenAddress := v.GetString("listen_address")
	listenAddrParts := listenAddrRe.FindStringSubmatch(rawListenAddress)
	host := listenAddrParts[1]
	port := listenAddrParts[2]

	if host == "" {
		host = defaultListenAddr + ":"
	} else if host == "*:" {
		host = "0.0.0.0:"
	}
	if port == "" {
		port = strconv.Itoa(defaultListenPort)
	}

	// Start webserver
	serverAddr := host + port
	srv := &http.Server{
		Addr:         serverAddr,
		ReadTimeout:  time.Second * 30,
		WriteTimeout: time.Minute * 30,
		IdleTimeout:  time.Second * 5,
		Handler:      r,
	}

	go func() {
		logger.Infof("Webserver listening at %v", serverAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("Failed to start webserver: %v", err)
		}
	}()

	logger.Info("Ready to handle requests")

	c := make(chan os.Signal, 1)
	// Graceful shutdown when quit via SIGINT (Ctrl+C) or SIGTERM (Ctrl+/)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	<-c

	logger.Info("Terminating on user input")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	logger.Info("Bye.")
}

// envReplacer utility to replace character when binding env variables to viper variables
type envReplacer struct {
	old string
	new string
}

func (r envReplacer) Replace(s string) string {
	return strings.ReplaceAll(s, r.old, r.new)
}

const RFC3339Millis = "2006-01-02T15:04:05.999Z07:00"

// getLogger creates a logrus.Logger with appropriate parameters and log level
func getLogger(verbose bool) *logrus.Logger {
	var logLevel logrus.Level
	if verbose {
		logLevel = logrus.DebugLevel
	} else {
		logLevel = logrus.InfoLevel
	}

	return &logrus.Logger{
		Out: os.Stdout,
		Formatter: &logrus.TextFormatter{
			TimestampFormat: RFC3339Millis,
		},
		Hooks: make(logrus.LevelHooks),
		Level: logLevel,
	}
}
