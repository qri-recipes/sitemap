package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/PuerkitoBio/fetchbot"
)

func newFetchbot(id int, c *Crawl, mux fetchbot.Handler) *fetchbot.Fetcher {
	f := fetchbot.New(logHandler(id, mux))
	f.DisablePoliteness = !c.cfg.Polite
	f.CrawlDelay = time.Duration(c.cfg.CrawlDelayMilliseconds) * time.Millisecond
	f.UserAgent = c.cfg.UserAgent
	return f
}

// muxer creates a new muxer for a crawler
func newMux(c *Crawl, stop chan bool) *fetchbot.Mux {
	// Create the muxer
	mux := fetchbot.NewMux()

	// Handle all errors the same
	mux.HandleErrors(fetchbot.HandlerFunc(func(ctx *fetchbot.Context, res *http.Response, err error) {
		c.urlLock.Lock()
		c.urls[ctx.Cmd.URL().String()] = &Url{Error: err.Error()}
		c.urlLock.Unlock()
	}))

	// Handle GET requests for html responses, to parse the body and enqueue all links as HEAD requests.
	mux.Response().Method("GET").Handler(fetchbot.HandlerFunc(
		func(ctx *fetchbot.Context, res *http.Response, err error) {

			u := &Url{Url: ctx.Cmd.URL().String()}

			var st time.Time
			if timedCmd, ok := ctx.Cmd.(*TimedCmd); ok {
				st = timedCmd.Started
			}

			if err := u.HandleGetResponse(st, res, c.cfg.RecordResponseHeaders); err != nil {
				log.Debugf("error handling get response: %s - %s", ctx.Cmd.URL().String(), err.Error())
				return
			}

			links := CandidateLinks(u.Links, c.urlStringIsCandidate)
			unwritten := make([]string, len(links))

			c.urlLock.Lock()
			c.urls[u.Url] = u
			c.finished++
			c.urlsWritten++

			i := 0
			for _, l := range links {
				if c.urls[l] == nil {
					unwritten[i] = l
					i++
				}
			}
			unwritten = unwritten[:i]

			c.urlLock.Unlock()

			if !c.stopping {
				go func() {
					for _, l := range unwritten {
						c.next <- l
					}
					log.Infof("seeded %d/%d links for source: %s", len(unwritten), len(u.Links), u.Url)
				}()
			}

			if c.finished == c.cfg.StopAfterEntries {
				stop <- true
			}

			if c.cfg.BackupWriteInterval > 0 && (c.urlsWritten%c.cfg.BackupWriteInterval == 0) {
				go func() {
					path := fmt.Sprintf("%s.backup", c.cfg.DestPath)
					log.Infof("writing backup sitemap: %s", path)
					if err := c.writeJSON(path); err != nil {
						log.Errorf("error writing backup sitemap: %s", err.Error())
					}
					c.batchCount++
				}()
			}

			go func() {
				var rawurl string

				c.queLock.Lock()
				c.queued[u.Url] = false
				for {
					rawurl = <-c.next
					if !c.queued[rawurl] && c.urlStringIsCandidate(rawurl) {
						break
					}
				}
				c.queLock.Unlock()

				if err := c.addUrlToQueue(ctx.Q, rawurl); err != nil {
					log.Infof("error queing %s", rawurl)
					return
				}

				log.Debugf("enqued %s", rawurl)
			}()
		}))

	return mux
}

// logHandler prints the fetch information and dispatches the call to the wrapped Handler.
func logHandler(crawlerId int, wrapped fetchbot.Handler) fetchbot.Handler {
	return fetchbot.HandlerFunc(func(ctx *fetchbot.Context, res *http.Response, err error) {
		if err == nil {
			log.Infof("[%d] %s %d %s", res.StatusCode, ctx.Cmd.Method(), crawlerId, ctx.Cmd.URL())
		}
		wrapped.Handle(ctx, res, err)
	})
}

// stopHandler stops the fetcher if the stopurl is reached. Otherwise it dispatches
// the call to the wrapped Handler.
func stopHandler(stopurl string, stop chan bool, wrapped fetchbot.Handler) fetchbot.Handler {
	return fetchbot.HandlerFunc(func(ctx *fetchbot.Context, res *http.Response, err error) {
		if ctx.Cmd.URL().String() == stopurl {
			log.Infof(">>>>> STOP URL %s\n", ctx.Cmd.URL())
			// generally not a good idea to stop/block from a handler goroutine
			// so do it in a separate goroutine
			go func() {
				stop <- true
			}()
			return
		}
		wrapped.Handle(ctx, res, err)
	})
}
