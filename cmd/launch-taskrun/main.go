// Copyright The Conforma Contributors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	ceclient "github.com/cloudevents/sdk-go/v2/client"
	cehttp "github.com/cloudevents/sdk-go/v2/protocol/http"
	tektonv1 "github.com/tektoncd/pipeline/pkg/apis/pipeline/v1"
	tektonclientset "github.com/tektoncd/pipeline/pkg/client/clientset/versioned"
	tektontypedv1 "github.com/tektoncd/pipeline/pkg/client/clientset/versioned/typed/pipeline/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coretypedv1 "k8s.io/client-go/kubernetes/typed/core/v1"

	gozap "go.uber.org/zap"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/conforma/knative-service/cmd/launch-taskrun/k8s"
	"github.com/conforma/knative-service/cmd/launch-taskrun/konflux"
)

// --- Interfaces for testability ---
type K8sConfigMapGetter interface {
	Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.ConfigMap, error)
}

type K8sCoreV1 interface {
	ConfigMaps(namespace string) K8sConfigMapGetter
}

type K8sClient interface {
	CoreV1() K8sCoreV1
}

type TektonTaskRunCreator interface {
	Create(ctx context.Context, taskRun *tektonv1.TaskRun, opts metav1.CreateOptions) (*tektonv1.TaskRun, error)
}

type TektonV1 interface {
	TaskRuns(namespace string) TektonTaskRunCreator
}

type TektonClient interface {
	TektonV1() TektonV1
}

type ControllerRuntimeClient interface {
	Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error
	List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error
}

// --- Logger interface and zapLogger ---
type Logger interface {
	Info(msg string, fields ...gozap.Field)
	Warn(msg string, fields ...gozap.Field)
	Error(err error, msg string, fields ...gozap.Field)
}

type zapLogger struct {
	l *gozap.Logger
}

func (z *zapLogger) Info(msg string, fields ...gozap.Field) { z.l.Info(msg, fields...) }
func (z *zapLogger) Warn(msg string, fields ...gozap.Field) { z.l.Warn(msg, fields...) }
func (z *zapLogger) Error(err error, msg string, fields ...gozap.Field) {
	z.l.Error(msg, append(fields, gozap.Error(err))...)
}

// --- ConfigMap Cache ---
type configMapCache struct {
	mu    sync.RWMutex
	cache map[string]*cachedConfigMap
	ttl   time.Duration
}

type cachedConfigMap struct {
	config    *TaskRunConfig
	timestamp time.Time
}

func newConfigMapCache(ttl time.Duration) *configMapCache {
	return &configMapCache{
		cache: make(map[string]*cachedConfigMap),
		ttl:   ttl,
	}
}

func (c *configMapCache) get(key string) (*TaskRunConfig, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if cached, exists := c.cache[key]; exists {
		if time.Since(cached.timestamp) < c.ttl {
			return cached.config, true
		}
		// Cache expired, remove it
		delete(c.cache, key)
	}
	return nil, false
}

func (c *configMapCache) set(key string, config *TaskRunConfig) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache[key] = &cachedConfigMap{
		config:    config,
		timestamp: time.Now(),
	}
}

// clear removes all entries from the cache
// This method is currently unused but kept for potential future use
//
//nolint:unused // Utility function kept for future use
func (c *configMapCache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache = make(map[string]*cachedConfigMap)
}

// --- Real implementations ---
type realK8sClient struct{ client *kubernetes.Clientset }

func (r *realK8sClient) CoreV1() K8sCoreV1 { return &realK8sCoreV1{client: r.client.CoreV1()} }

type realK8sCoreV1 struct{ client coretypedv1.CoreV1Interface }

func (r *realK8sCoreV1) ConfigMaps(ns string) K8sConfigMapGetter {
	return &realK8sConfigMapGetter{client: r.client.ConfigMaps(ns)}
}

type realK8sConfigMapGetter struct {
	client coretypedv1.ConfigMapInterface
}

func (r *realK8sConfigMapGetter) Get(ctx context.Context, name string, opts metav1.GetOptions) (*corev1.ConfigMap, error) {
	return r.client.Get(ctx, name, opts)
}

type realTektonClient struct{ client *tektonclientset.Clientset }

func (r *realTektonClient) TektonV1() TektonV1 { return &realTektonV1{client: r.client.TektonV1()} }

type realTektonV1 struct {
	client tektontypedv1.TektonV1Interface
}

func (r *realTektonV1) TaskRuns(ns string) TektonTaskRunCreator {
	return &realTektonTaskRunCreator{client: r.client.TaskRuns(ns)}
}

type realTektonTaskRunCreator struct {
	client tektontypedv1.TaskRunInterface
}

func (r *realTektonTaskRunCreator) Create(ctx context.Context, taskRun *tektonv1.TaskRun, opts metav1.CreateOptions) (*tektonv1.TaskRun, error) {
	return r.client.Create(ctx, taskRun, opts)
}

// --- CloudEvents client abstraction ---
type CloudEventsClient interface {
	StartReceiver(ctx context.Context, fn interface{}) error
}

type realCloudEventsClient struct {
	client cloudevents.Client
}

func (r *realCloudEventsClient) StartReceiver(ctx context.Context, fn interface{}) error {
	return r.client.StartReceiver(ctx, fn)
}

type realControllerRuntimeClient struct {
	client client.Client
}

func (r *realControllerRuntimeClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	return r.client.Get(ctx, key, obj, opts...)
}

func (r *realControllerRuntimeClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	return r.client.List(ctx, list, opts...)
}

// --- Service and business logic ---

type CloudEventData struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace"`
	} `json:"metadata"`
	Spec json.RawMessage `json:"spec"`
}

type TaskRunConfig struct {
	// Core VSA Configuration
	PolicyConfiguration     string `json:"POLICY_CONFIGURATION"`
	PublicKey               string `json:"PUBLIC_KEY"`
	IgnoreRekor             string `json:"IGNORE_REKOR"`
	VsaSigningKeySecretName string `json:"VSA_SIGNING_KEY_SECRET_NAME"`
	VsaUploadUrl            string `json:"VSA_UPLOAD_URL"`
	TaskName                string `json:"TASK_NAME"`

	// Performance & Behavior Configuration
	Strict  string `json:"STRICT"`
	Workers string `json:"WORKERS"`
	Debug   string `json:"DEBUG"`

	// Operational Configuration
	CacheTTLMinutes      string `json:"CACHE_TTL_MINUTES"`
	TektonTimeoutSeconds string `json:"TEKTON_TIMEOUT_SECONDS"`
	VsaExpirationHours   string `json:"VSA_EXPIRATION_HOURS"`

	// Resilience Configuration
	TektonRetryAttempts     string `json:"TEKTON_RETRY_ATTEMPTS"`
	TektonRetryDelaySeconds string `json:"TEKTON_RETRY_DELAY_SECONDS"`
	K8sRetryAttempts        string `json:"K8S_RETRY_ATTEMPTS"`
	K8sRetryDelaySeconds    string `json:"K8S_RETRY_DELAY_SECONDS"`
	CircuitBreakerThreshold string `json:"CIRCUIT_BREAKER_THRESHOLD"`
	CircuitBreakerTimeout   string `json:"CIRCUIT_BREAKER_TIMEOUT_SECONDS"`

	// Resource Configuration
	TaskCpuRequest    string `json:"TASK_CPU_REQUEST"`
	TaskMemoryRequest string `json:"TASK_MEMORY_REQUEST"`
	TaskMemoryLimit   string `json:"TASK_MEMORY_LIMIT"`
}

// CircuitBreakerState tracks the state of external service calls
type CircuitBreakerState struct {
	mu          sync.RWMutex
	failures    int
	lastFailure time.Time
	isOpen      bool
}

type Service struct {
	k8sClient      K8sClient
	tektonClient   TektonClient
	crtlClient     ControllerRuntimeClient
	logger         Logger
	configMapName  string
	configCache    *configMapCache
	circuitBreaker *CircuitBreakerState
}

type ServiceConfig struct {
	ConfigMapName string
	CacheTTL      time.Duration
}

func NewServiceWithDependencies(k8s K8sClient, tekton TektonClient, crtlClient ControllerRuntimeClient, logger Logger, config ServiceConfig) *Service {
	if config.ConfigMapName == "" {
		config.ConfigMapName = "taskrun-config"
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = 5 * time.Minute // Default 5 minute TTL
	}
	return &Service{
		k8sClient:      k8s,
		tektonClient:   tekton,
		crtlClient:     crtlClient,
		logger:         logger,
		configMapName:  config.ConfigMapName,
		configCache:    newConfigMapCache(config.CacheTTL),
		circuitBreaker: &CircuitBreakerState{},
	}
}

func NewService(config ServiceConfig) (*Service, error) {
	k8sConfig, err := k8s.NewK8sConfig()
	if err != nil {
		return nil, err
	}
	k8sClient, err := kubernetes.NewForConfig(k8sConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create k8s client: %w", err)
	}
	tektonClient, err := tektonclientset.NewForConfig(k8sConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create tekton client: %w", err)
	}
	crtlClient, err := k8s.NewControllerRuntimeClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create controller-runtime client: %w", err)
	}
	return NewServiceWithDependencies(
		&realK8sClient{client: k8sClient},
		&realTektonClient{client: tektonClient},
		&realControllerRuntimeClient{client: crtlClient},
		&zapLogger{l: gozap.NewExample()},
		config,
	), nil
}

func (s *Service) handleCloudEvent(ctx context.Context, event cloudevents.Event) error {
	s.logger.Info("Received CloudEvent", gozap.String("type", event.Type()))
	var eventData CloudEventData
	if err := event.DataAs(&eventData); err != nil {
		return fmt.Errorf("failed to parse event data: %w", err)
	}
	if eventData.Kind != "Snapshot" || eventData.APIVersion != "appstudio.redhat.com/v1alpha1" {
		s.logger.Info("Ignoring resource", gozap.String("apiVersion", eventData.APIVersion), gozap.String("kind", eventData.Kind))
		return nil
	}
	s.logger.Info("Processing Snapshot", gozap.String("name", eventData.Metadata.Name), gozap.String("namespace", eventData.Metadata.Namespace))
	snapshot := &konflux.Snapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name:      eventData.Metadata.Name,
			Namespace: eventData.Metadata.Namespace,
		},
	}
	// Assign the raw spec data directly
	snapshot.Spec = eventData.Spec
	return s.processSnapshot(ctx, snapshot)
}

func (s *Service) processSnapshot(ctx context.Context, snapshot *konflux.Snapshot) error {
	startTime := time.Now()
	s.logger.Info("Starting to process snapshot", gozap.String("name", snapshot.Name), gozap.String("namespace", snapshot.Namespace))

	// Read service namespace from environment variable
	configNamespace := os.Getenv("POD_NAMESPACE")
	if configNamespace == "" {
		configNamespace = "default"
		s.logger.Info("Falling back to default namespace", gozap.String("namespace", configNamespace))
	} else {
		s.logger.Info("Using POD_NAMESPACE env var for namespace", gozap.String("namespace", configNamespace))
	}

	config, err := s.readConfigMap(ctx, configNamespace)
	if err != nil {
		s.logger.Error(err, "Failed to read configmap")
		return fmt.Errorf("failed to read configmap: %w", err)
	}
	s.logger.Info("Successfully read configmap", gozap.String("namespace", configNamespace))
	taskRun, err := s.createTaskRun(snapshot, config, configNamespace)
	if err != nil {
		s.logger.Error(err, "Failed to create taskrun")
		return fmt.Errorf("failed to create taskrun: %w", err)
	}
	if taskRun == nil {
		// No error was returned, but also no TaskRun was created.
		// Consider it processed successfully.
		totalDuration := time.Since(startTime)
		s.logger.Info("No VSA creation needed for this snapshot",
			gozap.Duration("processing_duration_ms", totalDuration))
		return nil
	}
	s.logger.Info("Successfully created taskrun spec", gozap.String("taskrunName", taskRun.Name))

	// Create TaskRun with retry logic and configurable timeout
	var createdTaskRun *tektonv1.TaskRun
	err = s.retryWithBackoff(config, "create-taskrun", func() error {
		// Add timeout for Tekton API call (configurable)
		timeoutSeconds := 5 // Default
		if config.TektonTimeoutSeconds != "" {
			if parsed, parseErr := strconv.Atoi(config.TektonTimeoutSeconds); parseErr == nil && parsed > 0 {
				timeoutSeconds = parsed
			}
		}
		trCtx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSeconds)*time.Second)
		defer cancel()

		var createErr error
		createdTaskRun, createErr = s.tektonClient.TektonV1().TaskRuns(configNamespace).Create(trCtx, taskRun, metav1.CreateOptions{})
		return createErr
	})
	if err != nil {
		s.logger.Error(err, "Failed to create taskrun in cluster after retries")
		return fmt.Errorf("failed to create taskrun in cluster after retries: %w", err)
	}

	// Log performance metrics
	totalDuration := time.Since(startTime)
	s.logger.Info("Successfully created TaskRun",
		gozap.String("name", createdTaskRun.Name),
		gozap.String("namespace", createdTaskRun.Namespace),
		gozap.String("snapshot", snapshot.Name),
		gozap.Duration("processing_duration_ms", totalDuration))
	return nil
}

func (s *Service) readConfigMap(ctx context.Context, namespace string) (*TaskRunConfig, error) {
	// Check cache first
	cachedConfig, found := s.configCache.get(namespace)
	if found {
		s.logger.Info("Using cached config for namespace", gozap.String("namespace", namespace))
		return cachedConfig, nil
	}

	// If not in cache, fetch from K8s
	configMap, err := s.k8sClient.CoreV1().ConfigMaps(namespace).Get(ctx, s.configMapName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap %s: %w", s.configMapName, err)
	}
	config := &TaskRunConfig{}
	if val, exists := configMap.Data["POLICY_CONFIGURATION"]; exists {
		config.PolicyConfiguration = val
	}
	if val, exists := configMap.Data["PUBLIC_KEY"]; exists {
		config.PublicKey = val
	}
	if val, exists := configMap.Data["IGNORE_REKOR"]; exists {
		config.IgnoreRekor = val
	}
	if val, exists := configMap.Data["VSA_SIGNING_KEY_SECRET_NAME"]; exists {
		config.VsaSigningKeySecretName = val
	}
	if val, exists := configMap.Data["VSA_UPLOAD_URL"]; exists {
		config.VsaUploadUrl = val
	}
	if val, exists := configMap.Data["TASK_NAME"]; exists {
		config.TaskName = val
	}
	if val, exists := configMap.Data["STRICT"]; exists {
		config.Strict = val
	}
	if val, exists := configMap.Data["WORKERS"]; exists {
		config.Workers = val
	}
	if val, exists := configMap.Data["DEBUG"]; exists {
		config.Debug = val
	}
	if val, exists := configMap.Data["CACHE_TTL_MINUTES"]; exists {
		config.CacheTTLMinutes = val
	}
	if val, exists := configMap.Data["TEKTON_TIMEOUT_SECONDS"]; exists {
		config.TektonTimeoutSeconds = val
	}
	if val, exists := configMap.Data["VSA_EXPIRATION_HOURS"]; exists {
		config.VsaExpirationHours = val
	}
	if val, exists := configMap.Data["TEKTON_RETRY_ATTEMPTS"]; exists {
		config.TektonRetryAttempts = val
	}
	if val, exists := configMap.Data["TEKTON_RETRY_DELAY_SECONDS"]; exists {
		config.TektonRetryDelaySeconds = val
	}
	if val, exists := configMap.Data["K8S_RETRY_ATTEMPTS"]; exists {
		config.K8sRetryAttempts = val
	}
	if val, exists := configMap.Data["K8S_RETRY_DELAY_SECONDS"]; exists {
		config.K8sRetryDelaySeconds = val
	}
	if val, exists := configMap.Data["CIRCUIT_BREAKER_THRESHOLD"]; exists {
		config.CircuitBreakerThreshold = val
	}
	if val, exists := configMap.Data["CIRCUIT_BREAKER_TIMEOUT_SECONDS"]; exists {
		config.CircuitBreakerTimeout = val
	}
	if val, exists := configMap.Data["TASK_CPU_REQUEST"]; exists {
		config.TaskCpuRequest = val
	}
	if val, exists := configMap.Data["TASK_MEMORY_REQUEST"]; exists {
		config.TaskMemoryRequest = val
	}
	if val, exists := configMap.Data["TASK_MEMORY_LIMIT"]; exists {
		config.TaskMemoryLimit = val
	}

	// Cache the fetched config
	s.configCache.set(namespace, config)
	s.logger.Info("Fetched and cached config for namespace", gozap.String("namespace", namespace))
	return config, nil
}

// Circuit breaker and resilience methods
func (s *Service) checkCircuitBreaker(config *TaskRunConfig, operation string) bool {
	s.circuitBreaker.mu.RLock()
	defer s.circuitBreaker.mu.RUnlock()

	if !s.circuitBreaker.isOpen {
		return false // Circuit is closed, allow operation
	}

	// Check if circuit breaker timeout has passed
	timeoutSeconds := 30 // Default
	if config.CircuitBreakerTimeout != "" {
		if parsed, parseErr := strconv.Atoi(config.CircuitBreakerTimeout); parseErr == nil && parsed > 0 {
			timeoutSeconds = parsed
		}
	}

	if time.Since(s.circuitBreaker.lastFailure) > time.Duration(timeoutSeconds)*time.Second {
		s.logger.Info("Circuit breaker timeout expired, allowing operation",
			gozap.String("operation", operation))
		return false // Allow operation to test if service is back
	}

	s.logger.Warn("Circuit breaker is open, blocking operation",
		gozap.String("operation", operation),
		gozap.Int("failures", s.circuitBreaker.failures))
	return true // Block operation
}

func (s *Service) recordFailure(config *TaskRunConfig, operation string) {
	s.circuitBreaker.mu.Lock()
	defer s.circuitBreaker.mu.Unlock()

	s.circuitBreaker.failures++
	s.circuitBreaker.lastFailure = time.Now()

	threshold := 5 // Default
	if config.CircuitBreakerThreshold != "" {
		if parsed, parseErr := strconv.Atoi(config.CircuitBreakerThreshold); parseErr == nil && parsed > 0 {
			threshold = parsed
		}
	}

	if s.circuitBreaker.failures >= threshold && !s.circuitBreaker.isOpen {
		s.circuitBreaker.isOpen = true
		s.logger.Error(nil, "ALERT: Circuit breaker opened - external service degraded",
			gozap.String("alert_type", "circuit_breaker_opened"),
			gozap.String("service", "external_dependency"),
			gozap.String("operation", operation),
			gozap.Int("consecutive_failures", s.circuitBreaker.failures),
			gozap.Int("failure_threshold", threshold),
			gozap.Time("last_failure", s.circuitBreaker.lastFailure))
	}
}

func (s *Service) recordSuccess(operation string) {
	s.circuitBreaker.mu.Lock()
	defer s.circuitBreaker.mu.Unlock()

	if s.circuitBreaker.isOpen {
		s.logger.Info("RECOVERY: Circuit breaker closed - external service recovered",
			gozap.String("alert_type", "circuit_breaker_closed"),
			gozap.String("service", "external_dependency"),
			gozap.String("operation", operation),
			gozap.Int("previous_failures", s.circuitBreaker.failures),
			gozap.Duration("downtime_duration", time.Since(s.circuitBreaker.lastFailure)))
	}

	// Reset circuit breaker state on success
	s.circuitBreaker.failures = 0
	s.circuitBreaker.isOpen = false
}

func (s *Service) retryWithBackoff(config *TaskRunConfig, operation string, fn func() error) error {
	// Check circuit breaker first
	if s.checkCircuitBreaker(config, operation) {
		return fmt.Errorf("circuit breaker is open for operation: %s", operation)
	}

	maxAttempts := 3 // Default
	if config.TektonRetryAttempts != "" {
		if parsed, parseErr := strconv.Atoi(config.TektonRetryAttempts); parseErr == nil && parsed > 0 {
			maxAttempts = parsed
		}
	}

	retryDelay := 2 * time.Second // Default
	if config.TektonRetryDelaySeconds != "" {
		if parsed, parseErr := strconv.Atoi(config.TektonRetryDelaySeconds); parseErr == nil && parsed > 0 {
			retryDelay = time.Duration(parsed) * time.Second
		}
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err
			s.recordFailure(config, operation)

			if attempt < maxAttempts {
				s.logger.Warn("Operation failed, retrying",
					gozap.String("operation", operation),
					gozap.Int("attempt", attempt),
					gozap.Int("maxAttempts", maxAttempts),
					gozap.Duration("retryDelay", retryDelay),
					gozap.Error(err))
				time.Sleep(retryDelay)
				continue
			}
			// Final attempt failed
			s.logger.Error(lastErr, "Operation failed after all retry attempts",
				gozap.String("operation", operation),
				gozap.Int("attempts", maxAttempts))
			return lastErr
		}
		// Success
		s.recordSuccess(operation)
		if attempt > 1 {
			s.logger.Info("Operation succeeded after retry",
				gozap.String("operation", operation),
				gozap.Int("attempt", attempt))
		}
		return nil
	}
	return lastErr
}

func (s *Service) findEcp(snapshot *konflux.Snapshot) (string, error) {
	ctx := context.Background()
	return konflux.FindEnterpriseContractPolicy(ctx, s.crtlClient, s.logger, snapshot)
}

func (s *Service) createTaskRun(snapshot *konflux.Snapshot, config *TaskRunConfig, taskNamespace string) (*tektonv1.TaskRun, error) {
	// Validate required fields
	if config.TaskName == "" {
		return nil, fmt.Errorf("TASK_NAME is required but not set in configmap")
	}

	// Use the raw JSON spec directly
	specJSON := snapshot.Spec

	// Extract the primary image from the snapshot spec
	var snapshotSpec struct {
		Components []struct {
			ContainerImage string `json:"containerImage"`
		} `json:"components"`
	}
	if err := json.Unmarshal(specJSON, &snapshotSpec); err != nil {
		return nil, fmt.Errorf("failed to unmarshal snapshot spec to extract components: %w", err)
	}

	// log the specJSON
	s.logger.Info("SpecJSON", gozap.String("specJSON", string(specJSON)))
	// Helper function to create ParamValue with validation
	createParamValue := func(value string) tektonv1.ParamValue {
		if value == "" {
			value = "true" // Default to "true" for boolean-like empty values
		}
		return tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: value}
	}

	// Helper for numeric parameters with specific defaults
	createNumericParamValue := func(value, defaultValue string) tektonv1.ParamValue {
		if value == "" {
			value = defaultValue
		}
		return tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: value}
	}

	ecp, err := s.findEcp(snapshot)
	if err != nil {
		// If the findEcp lookup fails it generally means there was no ReleasePlan
		// or no ReleasePlanAdmission found for the Snapshot's Application. In that
		// situation we expect that the Snapshot is not likely to be released.
		//
		// This might change in future, but initially, the release pipeline is the
		// only place where VSAs are considered, so if we think the Snapshot won't
		// be released, then let's not bother creating a VSA.
		//
		// No TaskRun was created, but we don't consider it an error. Return a nil
		// TaskRun and expect the caller to notice.
		s.logger.Info("Unable to find RPA in cluster. Skipping VSA creation.", gozap.Error(err))
		return nil, nil
	} else {
		s.logger.Info("Found RPA in cluster. Using correct ECP.")
	}

	s.logger.Info("Using VSA signing key from mounted secret.")

	// Validate VSA upload URL is configured
	if config.VsaUploadUrl == "" {
		return nil, fmt.Errorf("VSA upload URL is not set")
	}

	params := []tektonv1.Param{
		{Name: "IMAGES", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: string(specJSON)}},
		{Name: "POLICY_CONFIGURATION", Value: createParamValue(ecp)},
		{Name: "PUBLIC_KEY", Value: createParamValue(config.PublicKey)},
		{Name: "VSA_SIGNING_KEY_SECRET_NAME", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: fmt.Sprintf("%s/%s", taskNamespace, config.VsaSigningKeySecretName)}},
		{Name: "VSA_UPLOAD_URL", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: config.VsaUploadUrl}},
		{Name: "IGNORE_REKOR", Value: createParamValue(config.IgnoreRekor)},
		{Name: "STRICT", Value: createParamValue(config.Strict)},
		{Name: "WORKERS", Value: createNumericParamValue(config.Workers, "1")},
		{Name: "DEBUG", Value: createParamValue(config.Debug)},
	}

	// Debug logging for all parameters
	for _, param := range params {
		s.logger.Info("TaskRun param", gozap.String("name", param.Name), gozap.String("type", string(param.Value.Type)), gozap.String("value", param.Value.StringVal))
	}

	return &tektonv1.TaskRun{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("verify-conforma-%s-%d", snapshot.Name, time.Now().Unix()),
			Namespace: taskNamespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "verify-and-create-vsa",
				"app.kubernetes.io/instance":   snapshot.Name,
				"app.kubernetes.io/component":  "conforma",
				"app.kubernetes.io/part-of":    "konflux",
				"app.kubernetes.io/managed-by": "conforma-knative-service",
			},
		},
		Spec: tektonv1.TaskRunSpec{
			TaskRef: &tektonv1.TaskRef{
				ResolverRef: tektonv1.ResolverRef{
					Resolver: "cluster",
					Params: tektonv1.Params{
						{Name: "kind", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: "task"}},
						{Name: "name", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: config.TaskName}},
						{Name: "namespace", Value: tektonv1.ParamValue{Type: tektonv1.ParamTypeString, StringVal: taskNamespace}},
					},
				},
			},
			Params:             params,
			ServiceAccountName: "conforma-vsa-generator",
		},
	}, nil
}

// --- HTTP server ---
type Server struct {
	service  *Service
	port     string
	ceClient CloudEventsClient
}

func NewServer(service *Service, port string, ceClient CloudEventsClient) *Server {
	return &Server{service: service, port: port, ceClient: ceClient}
}

func (s *Server) Start() error {
	s.service.logger.Info("Starting server", gozap.String("port", s.port))
	return s.ceClient.StartReceiver(context.Background(), s.service.handleCloudEvent)
}

func main() {
	service, err := NewService(ServiceConfig{})
	if err != nil {
		log.Fatalf("Failed to create service: %v", err)
	}
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	protocol, err := cehttp.New(
		cehttp.WithPath("/"),
		cehttp.WithMiddleware(func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				// Health check endpoint for observability
				if r.URL.Path == "/health" && r.Method == "GET" {
					w.WriteHeader(http.StatusOK)
					if _, writeErr := w.Write([]byte("OK")); writeErr != nil {
						// Log but don't fail - health check should be resilient
						log.Printf("Health check response write failed: %v", writeErr)
					}
					return
				}

				if r.Header.Get("Ce-Type") != "dev.knative.apiserver.resource.add" {
					w.WriteHeader(http.StatusAccepted)
					return
				}
				next.ServeHTTP(w, r)
			})
		}),
	)
	if err != nil {
		log.Fatalf("Failed to create protocol: %v", err)
	}
	ceClient, err := ceclient.New(protocol)
	if err != nil {
		log.Fatalf("Failed to create CloudEvents client: %v", err)
	}
	server := NewServer(service, port, &realCloudEventsClient{client: ceClient})
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
