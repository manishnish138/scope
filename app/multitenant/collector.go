package multitenant

// Collect reports from probes per-tenant, and supply them to queriers on demand

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync"

	"context"

	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
	"github.com/weaveworks/common/user"
	"github.com/weaveworks/scope/report"
)

// if StoreInterval is set, reports are merged into here and held until flushed to store
type pendingEntry struct {
	sync.Mutex
	report *report.Report
	older  []*report.Report
}

// We are building up a report in memory; merge into that and it will be saved shortly
// NOTE: may retain a reference to rep; must not be used by caller after this.
func (c *awsCollector) addToLive(ctx context.Context, userid string, rep report.Report) {
	entry := &pendingEntry{}
	if e, found := c.pending.LoadOrStore(userid, entry); found {
		entry = e.(*pendingEntry)
	}
	entry.Lock()
	if entry.report == nil {
		entry.report = &rep
	} else {
		entry.report.UnsafeMerge(rep)
	}
	entry.Unlock()
}

func (c *awsCollector) reportsFromLive(ctx context.Context, userid string) ([]report.Report, error) {
	if c.cfg.StoreInterval != 0 {
		// We are a collector
		e, found := c.pending.Load(userid)
		if !found {
			return nil, nil
		}
		entry := e.(*pendingEntry)
		entry.Lock()
		ret := make([]report.Report, len(entry.older)+1)
		ret[0] = entry.report.Copy() // Copy contents because this report is being unsafe-merged to
		for i, v := range entry.older {
			ret[i+1] = *v // older reports are immutable
		}
		entry.Unlock()
		return ret, nil
	}

	// We are a querier: fetch the most up-to-date reports from collectors
	// TODO: resolve c.collectorAddress periodically instead of every time we make a call
	addrs := resolve(c.cfg.CollectorAddr)
	ret := make([]report.Report, 0, len(addrs))
	// make a call to each collector and fetch its data for this userid
	// TODO: do them in parallel
	for _, addr := range addrs {
		body, err := oneCall(ctx, addr, userid)
		if err != nil {
			log.Warnf("error calling '%s': %v", addr, err)
			continue
		}
		rpt, err := report.MakeFromBinary(ctx, body, true, true)
		if err != nil {
			log.Warnf("error decoding: %v", err)
			continue
		}
		ret = append(ret, *rpt)
	}

	return ret, nil
}

func resolve(name string) []string {
	_, addrs, err := net.LookupSRV("", "", name)
	if err != nil {
		log.Warnf("Cannot resolve '%s': %v", name, err)
		return []string{}
	}
	endpoints := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		port := strconv.Itoa(int(addr.Port))
		endpoints = append(endpoints, net.JoinHostPort(addr.Target, port))
	}
	return endpoints
}

func oneCall(ctx context.Context, endpoint, userid string) (io.ReadCloser, error) {
	fullPath := endpoint + "/api/report"
	req, err := http.NewRequest("GET", fullPath, nil)
	if err != nil {
		return nil, fmt.Errorf("Error making request %s: %w", fullPath, err)
	}
	req = req.WithContext(ctx)
	req.Header.Set(user.OrgIDHeaderName, userid)
	req, ht := nethttp.TraceRequest(opentracing.GlobalTracer(), req)
	defer ht.Finish()

	client := &http.Client{Transport: &nethttp.Transport{}}
	res, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Error getting %s: %w", fullPath, err)
	}

	return res.Body, nil
}
