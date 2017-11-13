package sql_exporter

import (
	"context"
	"strings"
	"time"

	log "github.com/golang/glog"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Exporter is a prometheus.Gatherer that gathers SQL metrics from targets and merges them with the default registry.
type Exporter interface {
	prometheus.Gatherer
}

type exporter struct {
	jobs            []Job
	targets         []Target
	defaultGatherer prometheus.Gatherer
}

// NewExporter returns a new SQL Exporter for the provided config.
func NewExporter(configFile string, defaultGatherer prometheus.Gatherer) (Exporter, error) {
	// read config
	config, err := LoadConfig(configFile)
	if err != nil {
		return nil, err
	}

	e := exporter{
		jobs:            make([]Job, 0, len(config.Jobs)),
		targets:         make([]Target, 0, len(config.Jobs)*3),
		defaultGatherer: defaultGatherer,
	}
	//	e.mg.Add(defaultGatherer)

	for _, jc := range config.Jobs {
		job, err := NewJob(jc)
		if err != nil {
			return nil, err
		}
		e.jobs = append(e.jobs, job)
		// Must add targets one by one because while Target is a Gatherer, []Target is not a []Gatherer. :o(
		//		for _, t := range job.Targets() {
		//			e.mg.Add(t)
		//		}
	}
	return &e, nil
}

// Gather implements prometheus.Gatherer.
func (e *exporter) Gather() ([]*dto.MetricFamily, error) {
	gatherers := make(prometheus.Gatherers, 0, len(e.targets)+1)
	gatherers = append(gatherers, e.defaultGatherer)
	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
	ctx = context.Background()
	for _, j := range e.jobs {
		for _, t := range j.Targets() {
			gatherers = append(gatherers, NewGathererAdapter(ctx, t))
		}
	}
	return gatherers.Gather()
}

type gathererAdapter struct {
	ctx    context.Context
	target Target
}

func NewGathererAdapter(ctx context.Context, target Target) prometheus.Gatherer {
	return &gathererAdapter{
		ctx:    ctx,
		target: target,
	}
}

func (ga *gathererAdapter) Gather() ([]*dto.MetricFamily, error) {
	return ga.target.Gather(ga.ctx)
}

// MergingGatherer merges the output from multiple prometheus.Gatherer instances, swallowing and logging any type
// conflicts.
type MergingGatherer struct {
	gatherers []prometheus.Gatherer
}

func (mg *MergingGatherer) Add(gatherers ...prometheus.Gatherer) {
	mg.gatherers = append(mg.gatherers, gatherers...)
}

// Gather implements prometheus.Gatherer.
func (mg *MergingGatherer) Gather() ([]*dto.MetricFamily, error) {
	log.V(2).Info("Gathering metrics")
	var mfs []*dto.MetricFamily

	for _, g := range mg.gatherers {
		mfs1 := mfs
		mfs2, err := g.Gather()
		if err != nil {
			log.Errorf("Error while collecting from %s: %s", g, err)
		}

		n1 := len(mfs1)
		n2 := len(mfs2)
		mfs = make([]*dto.MetricFamily, 0, n1+n2)
		var i1, i2 int

		for i1 < n1 && i2 < n2 {
			mf1 := mfs1[i1]
			mf2 := mfs2[i2]
			switch strings.Compare(mf1.GetName(), mf2.GetName()) {
			case -1:
				mfs = append(mfs, mf1)
				i1++
			case 0:
				if mf1.Help != mf2.Help {
					log.Errorf("Conflicting help strings for metric %s: %q vs %q", mf1.Name, mf1.Help, mf2.Help)
				}
				if mf1.Type != mf2.Type {
					log.Errorf("Conflicting types for metric %s: %s vs %s", mf1.Name, mf1.Type, mf2.Type)
				}
				mf := *mf1
				mf.Metric = append(mf.Metric, mf2.Metric...)
				mfs = append(mfs, &mf)
				i1++
				i2++
			case 1:
				mfs = append(mfs, mf2)
				i2++
			}
		}

		for ; i1 < n1; i1++ {
			mfs = append(mfs, mfs1[i1])
		}
		for ; i2 < n2; i2++ {
			mfs = append(mfs, mfs2[i2])
		}
	}
	return mfs, nil
}
