package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"net/url"
	"os"

	"github.com/nuetzliches/hookaido/internal/config"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

func initTracing(ctx context.Context, obs config.ObservabilityConfig, onError func(error)) (func(context.Context) error, error) {
	opts := make([]otlptracehttp.Option, 0, 10)
	if obs.TracingCollector != "" {
		opts = append(opts, otlptracehttp.WithEndpointURL(obs.TracingCollector))
	}
	if obs.TracingURLPath != "" {
		opts = append(opts, otlptracehttp.WithURLPath(obs.TracingURLPath))
	}
	if obs.TracingCompression != "" {
		if obs.TracingCompression == "gzip" {
			opts = append(opts, otlptracehttp.WithCompression(otlptracehttp.GzipCompression))
		} else {
			opts = append(opts, otlptracehttp.WithCompression(otlptracehttp.NoCompression))
		}
	}
	if obs.TracingTimeoutSet {
		opts = append(opts, otlptracehttp.WithTimeout(obs.TracingTimeout))
	}
	if len(obs.TracingHeaders) > 0 {
		h := make(map[string]string, len(obs.TracingHeaders))
		for _, header := range obs.TracingHeaders {
			h[header.Name] = header.Value
		}
		opts = append(opts, otlptracehttp.WithHeaders(h))
	}
	if obs.TracingInsecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	if obs.TracingProxyURL != "" {
		proxyURL, err := url.Parse(obs.TracingProxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid tracing proxy_url: %w", err)
		}
		opts = append(opts, otlptracehttp.WithProxy(func(*http.Request) (*url.URL, error) {
			return proxyURL, nil
		}))
	}
	tlsCfg, err := buildTracingTLSConfig(obs)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		opts = append(opts, otlptracehttp.WithTLSClientConfig(tlsCfg))
	}
	if obs.TracingRetry != nil {
		opts = append(opts, otlptracehttp.WithRetry(otlptracehttp.RetryConfig{
			Enabled:         obs.TracingRetry.Enabled,
			InitialInterval: obs.TracingRetry.InitialInterval,
			MaxInterval:     obs.TracingRetry.MaxInterval,
			MaxElapsedTime:  obs.TracingRetry.MaxElapsedTime,
		}))
	}

	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName("hookaido"),
			semconv.ServiceVersion(version),
		),
	)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	if onError != nil {
		otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
			onError(err)
		}))
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	return tp.Shutdown, nil
}

func wrapTracingHandler(enabled bool, name string, h http.Handler) http.Handler {
	if !enabled {
		return h
	}
	return otelhttp.NewHandler(h, name)
}

func tracingHTTPClient(enabled bool) *http.Client {
	if !enabled {
		return nil
	}
	return &http.Client{
		Transport: otelhttp.NewTransport(http.DefaultTransport),
	}
}

func buildTracingTLSConfig(obs config.ObservabilityConfig) (*tls.Config, error) {
	hasTLS := obs.TracingTLSCAFile != "" ||
		obs.TracingTLSCertFile != "" ||
		obs.TracingTLSKeyFile != "" ||
		obs.TracingTLSServerName != "" ||
		obs.TracingTLSInsecureSkipVerify
	if !hasTLS {
		return nil, nil
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: obs.TracingTLSInsecureSkipVerify,
	}
	if obs.TracingTLSServerName != "" {
		tlsCfg.ServerName = obs.TracingTLSServerName
	}

	if obs.TracingTLSCAFile != "" {
		caPEM, err := os.ReadFile(obs.TracingTLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("read tracing tls.ca_file: %w", err)
		}

		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if ok := pool.AppendCertsFromPEM(caPEM); !ok {
			return nil, fmt.Errorf("parse tracing tls.ca_file: no certificates found")
		}
		tlsCfg.RootCAs = pool
	}

	if obs.TracingTLSCertFile != "" || obs.TracingTLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(obs.TracingTLSCertFile, obs.TracingTLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load tracing client certificate: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}
