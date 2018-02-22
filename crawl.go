package main

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/PuerkitoBio/fetchbot"
)

// Crawl holds state of a general crawl, including any number of
// "fetchbots", set by config.Parallism
type Crawl struct {
	// time crawler started
	start time.Time

	// internal channel to publish stop on
	stop chan bool

	// domains is a list of domains to fetch from
	domains []*url.URL

	// confoguration settings
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
		next:          make(chan string, 10000),
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

	c.stop = stop
	mux := newMux(c, stop)

	c.crawlers = make([]*fetchbot.Fetcher, c.cfg.Parallelism)
	for i := 0; i < c.cfg.Parallelism; i++ {
		fb := newFetchbot(i, c, mux)
		fb.UserAgent = c.cfg.UserAgent
		c.crawlers[i] = fb
	}

	start := time.Now()
	qs, stopCrawling, done := c.startCrawling(c.crawlers)
	c.queues = qs

	t := time.NewTicker(time.Second * 20)
	go func() {
		for range t.C {
			log.Infof("adding unfetched links from interval")
			ufl := c.gatherUnfetchedLinks(250, true)
			for linkurl, _ := range ufl {
				c.next <- linkurl
			}
			log.Infof("seeded %d unfetched links from interval", len(ufl))
		}
	}()

	go func() {
		<-stop

		t.Stop()

		path := fmt.Sprintf("%s.stopped", c.cfg.DestPath)
		log.Infof("writing stop batch: %s", path)
		if err := c.writeJSON(path); err != nil {
			log.Errorf("error writing json batch: %s", err.Error())
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
func (c *Crawl) startCrawling(crawlers []*fetchbot.Fetcher) (qs []*fetchbot.Queue, stop, done chan bool) {
	log.Infof("starting crawl with %d crawlers", len(crawlers))
	stop = make(chan bool)
	done = make(chan bool)
	qs = make([]*fetchbot.Queue, len(crawlers))

	wg := sync.WaitGroup{}

	for i, fetcher := range crawlers {
		// Start processing
		qs[i] = fetcher.Start()
		wg.Add(1)

		go func(i int) {
			qs[i].Block()
			log.Infof("finished crawler: %d", i)
			wg.Done()
		}(i)

		// sleep for 2 seconds between each crawler starting
		time.Sleep(time.Millisecond * 250)
	}

	go func() {
		<-stop
		for _, q := range qs {
			q.Close()
		}
	}()

	go func() {
		wg.Wait()
		done <- true
	}()

	i := 0

	links := CandidateLinks(c.cfg.Seeds, c.urlStringIsCandidate)
	log.Infof("adding %d/%d seed urls", len(links), len(c.cfg.Seeds))

SEEDS:
	for _, rawurl := range links {
		if err := c.addUrlToQueue(qs[i], rawurl); err != nil {
			// if the url has already been fetched, try it's links instead
			if err == errAlreadyFetched {
				u := c.urls[rawurl]
				for _, linkurl := range u.Links {
					if err := c.addUrlToQueue(qs[i], linkurl); err == nil {
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

	// // load up next chan with unfetched links
	// ufl := c.gatherUnfetchedLinks(250, false)
	// log.Infof("adding %d unfetched links", len(ufl))
	// for linkurl, _ := range ufl {
	// 	c.next <- linkurl
	// }

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

	// c.urlLock.Lock()
	// defer c.urlLock.Unlock()
	c.queLock.Lock()
	defer c.queLock.Unlock()

	normurl := canonicalURLString(u)
	// if u := c.urls[normurl]; u != nil {
	// 	if u.Status != 200 || (c.staleDuration != 0 && time.Since(u.Timestamp) > c.staleDuration) {
	// 		return errAlreadyFetched
	// 	}
	// }
	if c.queued[normurl] {
		return errInQueue
	}

	c.queued[normurl] = true
	// c.urls[normurl] = &Url{Url: normurl}
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

func (c *Crawl) gatherUnfetchedLinks(max int, sendStop bool) map[string]bool {
	log.Infof("LOCK - gatherUnfetchedLinks")
	c.urlLock.Lock()
	defer func() {
		log.Infof("UNLOCK - gatherUnfetchedLinks")
		c.urlLock.Unlock()
	}()

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
	} else if sendStop {
		log.Infof("no unfetched links found. sending stop")
		c.stop <- true
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
