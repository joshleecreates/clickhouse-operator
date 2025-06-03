// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"context"
	"time"

	kubeinformers "k8s.io/client-go/informers"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	chopinformers "github.com/altinity/clickhouse-operator/pkg/client/informers/externalversions"
	"github.com/altinity/clickhouse-operator/pkg/controller"
	"github.com/altinity/clickhouse-operator/pkg/controller/chi"
)

// Prometheus exporter defaults
const (
	defaultInformerFactoryResyncPeriod      = 60 * time.Second
	defaultInformerFactoryResyncDebugPeriod = 60 * time.Second
)

// CLI parameter variables
var (
	// Setting to 0 disables resync
	// Informer fires Update() func to periodically verify current state
	kubeInformerFactoryResyncPeriod = defaultInformerFactoryResyncPeriod
	chopInformerFactoryResyncPeriod = defaultInformerFactoryResyncPeriod
)

func init() {
}

var chiController *chi.Controller

// initClickHouse is an entry point of the application
func initClickHouse(ctx context.Context) {
	log.S().P()
	defer log.E().P()

	if debugRequest {
		kubeInformerFactoryResyncPeriod = defaultInformerFactoryResyncDebugPeriod
		chopInformerFactoryResyncPeriod = defaultInformerFactoryResyncDebugPeriod
	}

	// Initialize k8s API clients
	kubeClient, extClient, chopClient := chop.GetClientset(kubeConfigFile, masterURL)

	// Create operator instance
	chop.New(kubeClient, chopClient, chopConfigFile)
	log.V(1).F().Info("Config parsed:")
	log.Info("\n" + chop.Config().String(true))

	// Create Informers
	kubeInformerFactory := kubeinformers.NewSharedInformerFactoryWithOptions(
		kubeClient,
		kubeInformerFactoryResyncPeriod,
		kubeinformers.WithNamespace(chop.Config().GetInformerNamespace()),
	)
	chopInformerFactory := chopinformers.NewSharedInformerFactoryWithOptions(
		chopClient,
		chopInformerFactoryResyncPeriod,
		chopinformers.WithNamespace(chop.Config().GetInformerNamespace()),
	)

	// Create Controller
	chiController = chi.NewController(
		chopClient,
		extClient,
		kubeClient,
		chopInformerFactory,
		kubeInformerFactory,
	)

	// Register callback for configuration changes to reconcile all CHIs
	chop.Get().ConfigManager.RegisterConfigChangeCallback(func() {
		log.V(1).F().Info("Configuration changed, triggering reconciliation for all CHIs")
		reconcileAllCHIsOnConfigChange()
	})

	// Start Informers
	kubeInformerFactory.Start(ctx.Done())
	chopInformerFactory.Start(ctx.Done())
}

// runClickHouse is an entry point of the application
func runClickHouse(ctx context.Context) {
	log.S().P()
	defer log.E().P()

	// Start main CHI controller
	log.V(1).F().Info("Starting CHI controller")
	chiController.Run(ctx)
}

// reconcileAllCHIsOnConfigChange triggers reconciliation for all ClickHouseInstallations
// in watched namespaces when configuration changes occur
func reconcileAllCHIsOnConfigChange() {
	log.V(1).F().Info("Starting reconciliation of all CHIs due to configuration change")

	// Get the ConfigManager instance
	configManager := chop.Get().ConfigManager
	if configManager == nil {
		log.V(1).F().Error("ConfigManager is nil, cannot reconcile CHIs")
		return
	}

	// Get the chopClient from ConfigManager
	chopClient := configManager.ChopClient()
	if chopClient == nil {
		log.V(1).F().Error("chopClient is nil, cannot reconcile CHIs")
		return
	}

	// Get watched namespaces from configuration
	config := configManager.Config()
	if config == nil {
		log.V(1).F().Error("Config is nil, cannot determine watched namespaces")
		return
	}

	watchedNamespaces := config.Watch.Namespaces

	// If no specific namespaces are configured, we need to handle the "watch all namespaces" case
	if len(watchedNamespaces) == 0 {
		log.V(1).F().Info("No specific namespaces configured, will attempt to reconcile CHIs in operator namespace")
		// Default to operator's own namespace if available
		if operatorNamespace, ok := configManager.GetRuntimeParam("OPERATOR_POD_NAMESPACE"); ok && operatorNamespace != "" {
			watchedNamespaces = []string{operatorNamespace}
		} else {
			log.V(1).F().Warning("Cannot determine operator namespace, skipping CHI reconciliation")
			return
		}
	}

	log.V(1).F().Info("Reconciling CHIs in %d watched namespaces: %v", len(watchedNamespaces), watchedNamespaces)

	// Process each watched namespace
	for _, namespace := range watchedNamespaces {
		if namespace == "" {
			continue
		}

		log.V(1).F().Info("Processing namespace: %s", namespace)

		// List all ClickHouseInstallations in this namespace
		chiList, err := chopClient.ClickhouseV1().ClickHouseInstallations(namespace).List(context.TODO(), controller.NewListOptions())
		if err != nil {
			log.V(1).F().Error("Failed to list ClickHouseInstallations in namespace %s: %v", namespace, err)
			continue
		}

		log.V(1).F().Info("Found %d ClickHouseInstallations in namespace %s", len(chiList.Items), namespace)

		// Process each CHI in the namespace
		for i := range chiList.Items {
			chi := &chiList.Items[i]
			originalChi := chi.DeepCopy()

			if chi.Annotations == nil {
				chi.Annotations = make(map[string]string)
			}

			// Add unique annotation to trigger reconciliation
			reconcileAnnotation := "internal.altinity.com/reconcile-on-config-change"
			chi.Annotations[reconcileAnnotation] = time.Now().Format(time.RFC3339Nano)

			log.V(1).F().Info("Updating CHI %s/%s to trigger reconciliation", namespace, chi.Name)

			// Update the CHI in the Kubernetes API
			_, err := chopClient.ClickhouseV1().ClickHouseInstallations(namespace).Update(context.TODO(), chi, controller.NewUpdateOptions())
			if err != nil {
				log.V(1).F().Error("Failed to update CHI %s/%s: %v", namespace, chi.Name, err)
			} else {
				log.V(1).F().Info("Successfully triggered reconciliation for CHI %s/%s", namespace, chi.Name)
			}
			chi.Annotations = originalChi.Annotations // Restore original annotations to undo changes in the informer cache
		}
	}

	log.V(1).F().Info("Completed reconciliation trigger for all CHIs")
}
