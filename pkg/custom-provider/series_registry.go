/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package provider

import (
	"sync"

	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	apimeta "k8s.io/apimachinery/pkg/api/meta"

	"github.com/golang/glog"
	prom "github.com/john-delivuk/k8s-prometheus-adapter/pkg/client"
	pmodel "github.com/prometheus/common/model"
)

// NB: container metrics sourced from cAdvisor don't consistently follow naming conventions,
// so we need to whitelist them and handle them on a case-by-case basis.  Metrics ending in `_total`
// *should* be counters, but may actually be guages in this case.

// SeriesType represents the kind of series backing a metric.
type SeriesType int

const (
	CounterSeries SeriesType = iota
	SecondsCounterSeries
	GaugeSeries
)

// SeriesRegistry provides conversions between Prometheus series and MetricInfo
type SeriesRegistry interface {
	// ListAllMetrics lists all metrics known to this registry
	ListAllMetrics() []provider.CustomMetricInfo
	// SeriesForMetric looks up the minimum required series information to make a query for the given metric
	// against the given resource (namespace may be empty for non-namespaced resources)
	QueryForMetric(info provider.CustomMetricInfo, namespace string, resourceNames ...string) (query prom.Selector, found bool)
	// MatchValuesToNames matches result values to resource names for the given metric and value set
	MatchValuesToNames(metricInfo provider.CustomMetricInfo, values pmodel.Vector) (matchedValues map[string]pmodel.SampleValue, found bool)
}

type seriesInfo struct {
	// seriesName is the name of the corresponding Prometheus series
	seriesName string

	// converter is the SeriesConverter used to name this series
	converter SeriesConverter
}

// overridableSeriesRegistry is a basic SeriesRegistry
type basicSeriesRegistry struct {
	mu sync.RWMutex

	// info maps metric info to information about the corresponding series
	info         map[provider.CustomMetricInfo]seriesInfo
	externalInfo map[string]seriesInfo
	// metrics is the list of all known metrics
	metrics []provider.CustomMetricInfo

	mapper apimeta.RESTMapper

	metricLister MetricListerWithNotification
}

// NewBasicSeriesRegistry creates a SeriesRegistry driven by the data from the provided MetricLister.
func NewBasicSeriesRegistry(lister MetricListerWithNotification, mapper apimeta.RESTMapper) SeriesRegistry {
	var registry = basicSeriesRegistry{
		mapper:       mapper,
		metricLister: lister,
	}

	lister.AddNotificationReceiver(registry.filterAndStoreMetrics)

	return &registry
}

func (r *basicSeriesRegistry) filterMetrics(result MetricUpdateResult) MetricUpdateResult {
	converters := make([]SeriesConverter, 0)
	series := make([][]prom.Series, 0)

	for i, converter := range result.converters {
		converters = append(converters, converter)
		series = append(series, result.series[i])
	}

	return MetricUpdateResult{
		converters: converters,
		series:     series,
	}
}

func (r *basicSeriesRegistry) filterAndStoreMetrics(result MetricUpdateResult) {
	result = r.filterMetrics(result)

	newSeriesSlices := result.series
	converters := result.converters

	// if len(newSeriesSlices) != len(converters) {
	// 	return fmt.Errorf("need one set of series per converter")
	// }

	newInfo := make(map[provider.CustomMetricInfo]seriesInfo)
	for i, newSeries := range newSeriesSlices {
		converter := converters[i]
		for _, series := range newSeries {
			identity, err := converter.IdentifySeries(series)

			if err != nil {
				glog.Errorf("unable to name series %q, skipping: %v", series.String(), err)
				continue
			}

			// TODO: warn if it doesn't match any resources
			resources := identity.resources
			namespaced := identity.namespaced
			name := identity.name

			for _, resource := range resources {
				info := provider.CustomMetricInfo{
					GroupResource: resource,
					Namespaced:    namespaced,
					Metric:        name,
				}

				// namespace metrics aren't counted as namespaced
				if resource == nsGroupResource {
					info.Namespaced = false
				}

				// we don't need to re-normalize, because the metric namer should have already normalized for us
				newInfo[info] = seriesInfo{
					seriesName: series.Name,
					converter:  converter,
				}
			}
		}
	}

	// regenerate metrics
	newMetrics := make([]provider.CustomMetricInfo, 0, len(newInfo))
	for info := range newInfo {
		newMetrics = append(newMetrics, info)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.info = newInfo
	r.metrics = newMetrics
}

func (r *basicSeriesRegistry) ListAllMetrics() []provider.CustomMetricInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.metrics
}

func (r *basicSeriesRegistry) QueryForMetric(metricInfo provider.CustomMetricInfo, namespace string, resourceNames ...string) (prom.Selector, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(resourceNames) == 0 {
		glog.Errorf("no resource names requested while producing a query for metric %s", metricInfo.String())
		return "", false
	}

	metricInfo, _, err := metricInfo.Normalized(r.mapper)
	if err != nil {
		glog.Errorf("unable to normalize group resource while producing a query: %v", err)
		return "", false
	}

	info, infoFound := r.info[metricInfo]
	if !infoFound {
		glog.V(10).Infof("metric %v not registered", metricInfo)
		return "", false
	}

	query, err := info.converter.QueryForSeries(info.seriesName, metricInfo.GroupResource, namespace, resourceNames...)
	if err != nil {
		glog.Errorf("unable to construct query for metric %s: %v", metricInfo.String(), err)
		return "", false
	}

	return query, true
}

func (r *basicSeriesRegistry) MatchValuesToNames(metricInfo provider.CustomMetricInfo, values pmodel.Vector) (matchedValues map[string]pmodel.SampleValue, found bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	metricInfo, _, err := metricInfo.Normalized(r.mapper)
	if err != nil {
		glog.Errorf("unable to normalize group resource while matching values to names: %v", err)
		return nil, false
	}

	info, infoFound := r.info[metricInfo]
	if !infoFound {
		return nil, false
	}

	resourceLbl, err := info.converter.ResourceConverter().LabelForResource(metricInfo.GroupResource)
	if err != nil {
		glog.Errorf("unable to construct resource label for metric %s: %v", metricInfo.String(), err)
		return nil, false
	}

	res := make(map[string]pmodel.SampleValue, len(values))
	for _, val := range values {
		if val == nil {
			// skip empty values
			continue
		}
		res[string(val.Metric[resourceLbl])] = val.Value
	}

	return res, true
}
