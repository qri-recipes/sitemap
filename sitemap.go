package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

var (
	// logger
	log       = logrus.New()
	cfgPath   string
	sigKilled bool
)

func init() {
	flag.StringVar(&cfgPath, "config", "sitemap.config.json", "path to JSON configuration file")
}

func main() {
	flag.Parse()

	crawl := NewCrawl(JSONConfigFromFilepath(cfgPath))

	stop := make(chan bool)
	go stopOnSigKill(stop)

	if err := crawl.Start(stop); err != nil {
		log.Errorf("crawl failed: %s", err.Error())
	}

	if err := crawl.writeJSON(crawl.cfg.DestPath); err != nil {
		log.Errorf("error writing file: %S", err.Error())
	}

	log.Infof("crawl took: %f hours. wrote %d urls", time.Since(crawl.start).Hours(), crawl.urlsWritten)
}

func stopOnSigKill(stop chan bool) {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	for {
		<-ch
		if sigKilled == true {
			os.Exit(1)
		}
		sigKilled = true

		go func() {
			log.Infof(strings.Repeat("*", 72))
			log.Infof("  received kill signal. stopping & writing file. this'll take a second")
			log.Infof("  press ^C again to exit")
			log.Infof(strings.Repeat("*", 72))
			stop <- true
		}()
	}
}
