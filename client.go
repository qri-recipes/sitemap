package main

import (
	"fmt"
	"net/http"
	"net/url"
	"time"
)

func newClient(c *Crawl) *http.Client {
	if !c.cfg.RecordRedirects {
		return http.DefaultClient
	}

	return &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			prev := via[len(via)-1]

			prevurl := canonicalURLString(prev.URL)

			r, _ := url.Parse(req.URL.String())
			canurlstr := canonicalURLString(r)

			if prevurl != canurlstr {
				log.Infof("[%d] %s %s -> %s", req.Response.StatusCode, prev.Method, prevurl, canurlstr)
				c.urlLock.Lock()
				c.urls[prevurl] = &Url{
					Url:        prevurl,
					Timestamp:  time.Now(),
					Status:     req.Response.StatusCode,
					RedirectTo: canurlstr,
				}
				c.finished++
				c.urlsWritten++
				c.urlLock.Unlock()
			}

			if len(via) >= 10 {
				for i, c := range via {
					log.Infof("%d %#v", i, c.URL.String())
				}
				return fmt.Errorf("stopped after 10 redirects")
			}
			return nil
		},
	}
}
