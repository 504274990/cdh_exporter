package main

import (
	"cdh_exporter/collector"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"
	"github.com/prometheus/exporter-toolkit/web"
	"gopkg.in/alecthomas/kingpin.v2"
	"net/http"
	"os"
)

const version string = "1.0.0"

var (
	listenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface.").Default(":9232").String()
	metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
)

func main() {
	promlogConfig := &promlog.Config{}
	logger := promlog.New(promlogConfig)
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.Version(version)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	reg := prometheus.NewRegistry()
	reg.MustRegister(collector.NewServiceCollector())
	reg.MustRegister(collector.NewServiceTimeseriesExporter())
	h := promhttp.HandlerFor(reg, promhttp.HandlerOpts{})

	level.Info(logger).Log("msg", "Starting cdh_exporter", "version", version)
	http.Handle("/metrics", h)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
            <head><title>Cdh Exporter</title></head>
            <body>
            <h1>Cdh Exporter</h1>
            <p><a href='` + *metricsPath + `'>Metrics</a></p>
            </body>
            </html>`))
	})

	level.Info(logger).Log("msg", "Listening on", "address", *listenAddress)
	server := &http.Server{Addr: *listenAddress}
	if err := web.ListenAndServe(server, "", logger); err != nil {
		level.Error(logger).Log("msg", "Error starting HTTP server", "err", err)
		os.Exit(1)
	}
}
