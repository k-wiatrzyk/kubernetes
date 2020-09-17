/*
Copyright 2020 The Kubernetes Authors.

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


package topologymanager

import (
	"k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
)


type containerScope struct {
	scope
}

// Ensure containerScope implements Scope interface
var _ Scope = &containerScope{}

func NewContainerScope(policy Policy) Scope {
	pm := make(map[string]string)
	return &containerScope{
		scope{
			name: containerTopologyScope,
			podTopologyHints: PodTopologyHints{},
			policy: policy,
			podMap: pm,
		},
	}
}

func (s *containerScope) calculateAffinity(pod *v1.Pod, container *v1.Container) (TopologyHint, bool) {
	providersHints := s.accumulateProvidersHints(pod, container)
	bestHint, admit := s.policy.Merge(providersHints)
	klog.Infof("[topologymanager] ContainerTopologyHint: %v", bestHint)
	return bestHint, admit
}


func (s *containerScope) accumulateProvidersHints(pod *v1.Pod, container *v1.Container) (providersHints []map[string][]TopologyHint) {
	// Loop through all hint providers and save an accumulated list of the
	// hints returned by each hint provider.
	for _, provider := range s.hintProviders {
		// Get the TopologyHints from a provider.
		hints := provider.GetTopologyHints(pod, container)
		providersHints = append(providersHints, hints)
		klog.Infof("[topologymanager] TopologyHints for pod '%v', container '%v': %v", format.Pod(pod), container.Name, hints)
	}
	return providersHints
}

func (s *containerScope) Admit(pod *v1.Pod) lifecycle.PodAdmitResult{
	
	// Exception - Policy : none
	if s.policy.Name() == PolicyNone {
		return s.admitPolicyNone(pod)
	}
	
	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		bestHint, admit := s.calculateAffinity(pod, &container)

		if !admit {
			return topologyAffinityError()
		}

		if (s.podTopologyHints)[string(pod.UID)] == nil {
			(s.podTopologyHints)[string(pod.UID)] = make(map[string]TopologyHint)
		}

		klog.Infof("[topologymanager] Topology Affinity for (pod: %v container: %v): %v", format.Pod(pod), container.Name, bestHint)
		(s.podTopologyHints)[string(pod.UID)][container.Name]=bestHint
		err := s.allocateAlignedResources(pod, &container)
		if err != nil {
			return unexpectedAdmissionError(err)
		}
	}
	return admitPod()
}

