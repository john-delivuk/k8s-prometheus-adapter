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
	"context"
	"fmt"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"
	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider/helpers"
	pmodel "github.com/prometheus/common/model"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/metrics/pkg/apis/custom_metrics"

	prom "github.com/john-delivuk/k8s-prometheus-adapter/pkg/client"
)

type prometheusProvider struct {
	mapper     apimeta.RESTMapper
	kubeClient dynamic.Interface
	promClient prom.Client

	SeriesRegistry
}

// NewPrometheusProvider creates an CustomMetricsProvider capable of responding to Kubernetes requests for custom metric data.
func NewPrometheusProvider(mapper apimeta.RESTMapper, kubeClient dynamic.Interface, promClient prom.Client, converters []SeriesConverter, updateInterval time.Duration) (provider.CustomMetricsProvider, Runnable) {
	// TODO: Consider injecting these objects and calling .Run() on the runnables before calling this function.
	basicLister := NewBasicMetricLister(promClient, converters, updateInterval)
	periodicLister, _ := NewPeriodicMetricLister(basicLister, updateInterval)
	seriesRegistry := NewBasicSeriesRegistry(periodicLister, mapper)

	return &prometheusProvider{
		mapper:     mapper,
		kubeClient: kubeClient,
		promClient: promClient,

		SeriesRegistry: seriesRegistry,
	}, periodicLister
}

func (p *prometheusProvider) metricFor(value pmodel.SampleValue, name types.NamespacedName, info provider.CustomMetricInfo) (*custom_metrics.MetricValue, error) {
	ref, err := helpers.ReferenceFor(p.mapper, name, info)
	if err != nil {
		return nil, err
	}

	return &custom_metrics.MetricValue{
		DescribedObject: ref,
		MetricName:      info.Metric,
		// TODO(directxman12): use the right timestamp
		Timestamp: metav1.Time{time.Now()},
		Value:     *resource.NewMilliQuantity(int64(value*1000.0), resource.DecimalSI),
	}, nil
}

func (p *prometheusProvider) metricsFor(valueSet pmodel.Vector, info provider.CustomMetricInfo, namespace string, names []string) (*custom_metrics.MetricValueList, error) {
	values, found := p.MatchValuesToNames(info, valueSet)
	if !found {
		return nil, provider.NewMetricNotFoundError(info.GroupResource, info.Metric)
	}
	res := []custom_metrics.MetricValue{}

	for _, name := range names {
		if _, found := values[name]; !found {
			continue
		}

		value, err := p.metricFor(values[name], types.NamespacedName{Namespace: namespace, Name: name}, info)
		if err != nil {
			return nil, err
		}
		res = append(res, *value)
	}

	return &custom_metrics.MetricValueList{
		Items: res,
	}, nil
}

func (p *prometheusProvider) buildQuery(info provider.CustomMetricInfo, namespace string, names ...string) (pmodel.Vector, error) {
	query, found := p.QueryForMetric(info, namespace, names...)
	if !found {
		return nil, provider.NewMetricNotFoundError(info.GroupResource, info.Metric)
	}

	// TODO: use an actual context
	queryResults, err := p.promClient.Query(context.TODO(), pmodel.Now(), query)
	if err != nil {
		glog.Errorf("unable to fetch metrics from prometheus: %v", err)
		// don't leak implementation details to the user
		return nil, apierr.NewInternalError(fmt.Errorf("unable to fetch metrics"))
	}

	if queryResults.Type != pmodel.ValVector {
		glog.Errorf("unexpected results from prometheus: expected %s, got %s on results %v", pmodel.ValVector, queryResults.Type, queryResults)
		return nil, apierr.NewInternalError(fmt.Errorf("unable to fetch metrics"))
	}

	return *queryResults.Vector, nil
}

func (p *prometheusProvider) GetMetricByName(name types.NamespacedName, info provider.CustomMetricInfo) (*custom_metrics.MetricValue, error) {
	// construct a query
	queryResults, err := p.buildQuery(info, name.Namespace, name.Name)
	if err != nil {
		return nil, err
	}

	// associate the metrics
	if len(queryResults) < 1 {
		return nil, provider.NewMetricNotFoundForError(info.GroupResource, info.Metric, name.Name)
	}

	namedValues, found := p.MatchValuesToNames(info, queryResults)
	if !found {
		return nil, provider.NewMetricNotFoundError(info.GroupResource, info.Metric)
	}

	if len(namedValues) > 1 {
		glog.V(2).Infof("Got more than one result (%v results) when fetching metric %s for %q, using the first one with a matching name...", len(queryResults), info.String(), name)
	}

	resultValue, nameFound := namedValues[name.Name]
	if !nameFound {
		glog.Errorf("None of the results returned by when fetching metric %s for %q matched the resource name", info.String(), name)
		return nil, provider.NewMetricNotFoundForError(info.GroupResource, info.Metric, name.Name)
	}

	// return the resulting metric
	return p.metricFor(resultValue, name, info)
}

func (p *prometheusProvider) GetMetricBySelector(namespace string, selector labels.Selector, info provider.CustomMetricInfo) (*custom_metrics.MetricValueList, error) {
	// fetch a list of relevant resource names
	resourceNames, err := helpers.ListObjectNames(p.mapper, p.kubeClient, namespace, selector, info)
	if err != nil {
		glog.Errorf("unable to list matching resource names: %v", err)
		// don't leak implementation details to the user
		return nil, apierr.NewInternalError(fmt.Errorf("unable to list matching resources"))
	}

	// construct the actual query
	queryResults, err := p.buildQuery(info, namespace, resourceNames...)
	if err != nil {
		return nil, err
	}

	// return the resulting metrics
	return p.metricsFor(queryResults, info, namespace, resourceNames)
}
