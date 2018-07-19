package provider

// import (
// 	"context"
// 	"time"

// 	pmodel "github.com/prometheus/common/model"

// 	"github.com/kubernetes-incubator/custom-metrics-apiserver/pkg/provider"

// 	apimeta "k8s.io/apimachinery/pkg/api/meta"
// 	"k8s.io/apimachinery/pkg/labels"
// 	"k8s.io/client-go/dynamic"
// 	"k8s.io/metrics/pkg/apis/external_metrics"

// 	prom "github.com/directxman12/k8s-prometheus-adapter/pkg/client"
// )

// //TODO: Make sure everything has the proper licensing disclosure at the top.
// //TODO: I'd like to move these files into another directory, but the compiler was giving me
// //some static around unexported types. I'm going to leave things as-is for now, but it
// //might be worthwhile to, once the shared components are discovered, move some things around.

// //TODO: Some of these members may not be necessary.
// //Some of them are definitely duplicated between the
// //external and custom providers. They should probably share
// //the same instances of these objects (especially the SeriesRegistry)
// //to cut down on unnecessary chatter/bookkeeping.
// type externalPrometheusProvider struct {
// 	mapper          apimeta.RESTMapper
// 	kubeClient      dynamic.Interface
// 	promClient      prom.Client
// 	queryBuilder    ExternalMetricQueryBuilder
// 	metricConverter conv.MetricConverter

// 	seriesRegistry SeriesRegistry
// }

// //TODO: It probably makes more sense to, once this is functional and complete, roll the
// //prometheusProvider and externalPrometheusProvider up into a single type
// //that implements both interfaces or provide a thin wrapper that composes them.
// //Just glancing at start.go looks like it would be much more straightforward
// //to do one of those two things instead of trying to run the two providers
// //independently.

// func NewExternalPrometheusProvider(mapper apimeta.RESTMapper, kubeClient dynamic.Interface, promClient prom.Client, namers []MetricNamer, updateInterval time.Duration, metricConverter conv.MetricConverter, seriesRegistry SeriesRegistry) (provider.ExternalMetricsProvider, Runnable) {

// 	return &externalPrometheusProvider{
// 		mapper:          mapper,
// 		kubeClient:      kubeClient,
// 		promClient:      promClient,
// 		metricConverter: metricConverter,
// 		seriesRegistry:  seriesRegistry,
// 	}, lister
// }

// func (p *externalPrometheusProvider) GetExternalMetric(namespace string, metricName string, metricSelector labels.Selector) (*external_metrics.ExternalMetricValueList, error) {
// 	selector, found := p.seriesRegistry.QueryForExternalMetric(metricInfo, metricSelector)

// 	if !found {
// 		return &external_metrics.ExternalMetricValueList{
// 			Items: []external_metrics.ExternalMetricValue{},
// 		}, nil
// 	}
// 	// query := p.queryBuilder.BuildPrometheusQuery(namespace, metricName, metricSelector, queryMetadata)

// 	//TODO: I don't yet know what a context is, but apparently I should use a real one.
// 	queryResults, err := p.promClient.Query(context.TODO(), pmodel.Now(), selector)

// 	if err != nil {
// 		//TODO: Is this how folks normally deal w/ errors? Just propagate them upwards?
// 		//I should go look at what the customProvider does.
// 		return nil, err
// 	}

// 	return p.metricConverter.Convert(queryMetadata, queryResults)
// }

// func (p *externalPrometheusProvider) ListAllExternalMetrics() []provider.ExternalMetricInfo {
// 	return p.seriesRegistry.ListAllExternalMetrics()
// }