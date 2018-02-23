package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/PuerkitoBio/fetchbot"
)

const queueBufferSize = 5000

// Crawl holds state of a general crawl, including any number of
// "fetchbots", set by config.Parallism
type Crawl struct {
	// time crawler started
	start time.Time

	// flag indicating crawler is stopping
	stopping bool

	// domains is a list of domains to fetch from
	domains []*url.URL

	// cfg embeds this crawl's configuration
	cfg Config

	// crawlers is a slice of all the executing crawlers
	crawlers []*fetchbot.Fetcher
	// queues holds a list
	queues []*fetchbot.Queue
	// next is a channel of candidate urls to add to the crawler
	next chan string

	// urlLock protects access to urls domains map
	urlLock sync.Mutex
	// crawled is the list of stuff that's been crawled
	urls map[string]*Url

	// queLock protects access to the queued map
	queLock sync.Mutex
	// queued keeps track of urls that are currently queued
	queued map[string]bool

	// finished is a count of the total number of urls finished
	finished int
	// how long should pass before we re-visit a url, 0 means
	// urls are never stale (only visit once)
	staleDuration time.Duration
	// how many batches have been written
	batchCount int
	// how many urls have been written this batch
	urlsWritten int
}

func NewCrawl(options ...func(*Config)) *Crawl {
	cfg := DefaultConfig()
	for _, opt := range options {
		opt(&cfg)
	}

	c := &Crawl{
		urls:          map[string]*Url{},
		queued:        map[string]bool{},
		next:          make(chan string, queueBufferSize),
		cfg:           cfg,
		staleDuration: time.Duration(cfg.StaleDurationHours) * time.Hour,
	}

	c.loadSitemapFile(c.cfg.SrcPath)

	c.domains = make([]*url.URL, len(cfg.Domains))
	for i, rawurl := range cfg.Domains {
		u, err := url.Parse(rawurl)
		if err != nil {
			err = fmt.Errorf("error parsing configured domain: %s", err.Error())
			log.Error(err.Error())
			return c
		}
		c.domains[i] = u
	}

	return c
}

func CandidateLinks(ls []string, candidate func(string) bool) []string {
	if ls == nil || len(ls) == 0 {
		return nil
	}

	added := 0
	links := make([]string, len(ls))
	for _, l := range ls {
		if candidate(l) {
			links[added] = l
			added++
		}
	}
	return links[:added]
}

// Start initiates the crawler
func (c *Crawl) Start(stop chan bool) error {
	var (
		mux fetchbot.Handler
		t   *time.Ticker
	)

	mux = newMux(c, stop)

	if c.cfg.StopURL != "" {
		mux = stopHandler(c.cfg.StopURL, stop, mux)
	}

	c.crawlers = make([]*fetchbot.Fetcher, c.cfg.Parallelism)
	for i := 0; i < c.cfg.Parallelism; i++ {
		fb := newFetchbot(i, c, mux)
		c.crawlers[i] = fb
	}

	start := time.Now()
	qs, stopCrawling, done := c.startCrawling(c.crawlers)
	c.queues = qs

	if c.cfg.UnfetchedScanFreqMilliseconds > 0 {
		d := time.Millisecond * time.Duration(c.cfg.UnfetchedScanFreqMilliseconds)
		log.Infof("checking for unfetched urls every %f seconds", d.Seconds())
		t = time.NewTicker(d)
		go func() {
			for range t.C {
				if !c.stopping {
					if len(c.next) == queueBufferSize {
						log.Infof("next queue is full, skipping scan")
						continue
					}

					log.Infof("scanning for unfetched links")
					ufl := c.gatherUnfetchedLinks(250, stop)
					for linkurl, _ := range ufl {
						c.next <- linkurl
					}
					log.Infof("seeded %d unfetched links from interval", len(ufl))
				}
			}
		}()
	}

	go func() {
		<-stop

		if c.cfg.BackupWriteInterval > 0 {
			path := fmt.Sprintf("%s.backup", c.cfg.DestPath)
			log.Infof("writing backup sitemap: %s", path)
			if err := c.writeJSON(path); err != nil {
				log.Errorf("error writing backup sitemap: %s", err.Error())
			}
		}

		log.Infof("%d urls remain in que for checking and processing", len(c.next))
		if t != nil {
			t.Stop()
		}

		stopCrawling <- true
	}()
	go c.seedFromNextChan(qs, c.next)

	log.Infof("crawl started at %s", start)
	<-done
	return nil
}

// startCrawling initializes the crawler, queue, stopCrawler channel, and
// crawlingURLS slice
func (c *Crawl) startCrawling(crawlers []*fetchbot.Fetcher) (qs []*fetchbot.Queue, stopCrawling, done chan bool) {
	log.Infof("starting crawl with %d crawlers for %d domains", len(crawlers), len(c.domains))
	freq := time.Duration(c.cfg.CrawlDelayMilliseconds) * time.Millisecond
	log.Infof("crawler request frequency: ~%f req/minute", float64(len(crawlers))/freq.Minutes())
	stopCrawling = make(chan bool)
	done = make(chan bool)
	qs = make([]*fetchbot.Queue, len(crawlers))

	wg := sync.WaitGroup{}

	for i, fetcher := range crawlers {
		// Start processing
		qs[i] = fetcher.Start()
		time.Sleep(time.Second)
		wg.Add(1)

		go func(i int) {
			qs[i].Block()
			log.Infof("finished crawler: %d", i)
			wg.Done()
		}(i)
	}

	go func() {
		<-stopCrawling
		c.stopping = true
		log.Info("stopping crawlers")
		for _, q := range qs {
			q.Close()
		}
	}()

	go func() {
		wg.Wait()
		log.Info("done")
		done <- true
	}()

	i := 0

	links := CandidateLinks(c.cfg.Seeds, c.urlStringIsCandidate)
	log.Infof("adding %d/%d seed urls", len(links), len(c.cfg.Seeds))
	alreadyFetched := 0
	childLinks := 0

SEEDS:
	for _, rawurl := range links {
		if err := c.addUrlToQueue(qs[i], rawurl); err != nil {
			// if the url has already been fetched, try it's links instead
			if err == errAlreadyFetched {
				alreadyFetched++
				u := c.urls[rawurl]
				for _, linkurl := range u.Links {
					if err := c.addUrlToQueue(qs[i], linkurl); err == nil {
						childLinks++
						continue SEEDS
					}
				}

				// if still nothing, set the url's status to 0 to trigger a re-fetch
				u.Status = 0
				err = c.addUrlToQueue(qs[i], u.Url)
			} else if err == errInvalidFetchURL {
				continue SEEDS
			}
			log.Infof("error adding url to queue: %s", err.Error())
		}
		i++
		if i == len(qs) {
			i = 0
		}
	}

	if alreadyFetched > 0 {
		log.Infof("%d seed urls were already fetched. for %d of those urls, an unfetched link was used instead", alreadyFetched, childLinks)
	}

	return
}

var (
	errAlreadyFetched  = fmt.Errorf("already fetched")
	errInvalidFetchURL = fmt.Errorf("invalid url for fetching")
	errInQueue         = fmt.Errorf("url is already queued for fetching")
)

func (c *Crawl) addUrlToQueue(q *fetchbot.Queue, rawurl string) error {
	u, err := url.Parse(rawurl)
	if err != nil {
		return err
	}

	if err := isWebpageURL(u); err != nil {
		return errInvalidFetchURL
	}

	c.queLock.Lock()
	defer c.queLock.Unlock()

	normurl := canonicalURLString(u)
	if c.queued[normurl] {
		return errInQueue
	}

	c.queued[normurl] = true
	tg, err := NewTimedGet(normurl)
	if err != nil {
		return err
	}
	if err := q.Send(tg); err != nil {
		return err
	}
	return nil
}

func (c *Crawl) seedFromNextChan(queues []*fetchbot.Queue, next chan string) {
	i := 0
	for {
		rawurl := <-next
		if err := c.addUrlToQueue(queues[i], rawurl); err == nil {
			log.Infof("enqued %d: %s", i, rawurl)
			i++
		}
		if i == len(queues) {
			break
		}
	}
}

func (c *Crawl) gatherUnfetchedLinks(max int, stop chan bool) map[string]bool {
	c.urlLock.Lock()
	defer c.urlLock.Unlock()

	links := make(map[string]bool, max)
	i := 0

	for _, u := range c.urls {
		if u != nil && u.Status == 200 {
			for _, linkurl := range u.Links {
				if c.urlStringIsCandidate(linkurl) {
					linku := c.urls[linkurl]
					if (linku == nil || linku.Status != http.StatusOK) && links[linkurl] == false {
						links[linkurl] = true
						i++
						if i == max {
							log.Infof("found max of %d unfetched links", max)
							return links
						}
					}
				}
			}
		}
	}

	if i > 0 {
		log.Infof("found %d unfetched links", i)
	} else if stop != nil {
		log.Infof("no unfetched links found. sending stop")
		stop <- true
	}

	return links
}

// urlStringIsCandidate scans the slice of crawlingURLS to see if we should GET
// the passed-in url
func (c *Crawl) urlStringIsCandidate(rawurl string) bool {
	u, err := url.Parse(rawurl)
	if err != nil {
		return false
	}
	for _, d := range c.domains {
		if d.Host == u.Host {
			return true
		}
	}
	return false
}
