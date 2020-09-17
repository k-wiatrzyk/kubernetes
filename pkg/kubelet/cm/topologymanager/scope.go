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
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"sync"
	"k8s.io/klog/v2"
)


const (
	// containerTopologyScope specifies the TopologyManagerScope per container.
	containerTopologyScope = "container"
	// podTopologyScope specifies the TopologyManagerScope per pod.
	podTopologyScope = "pod"
)

type PodTopologyHints map[string]map[string]TopologyHint

type Scope interface {
	Name() string
	Admit(pod *v1.Pod) lifecycle.PodAdmitResult
	AddHintProvider(h HintProvider)
	GetAffinity(podUID string, containerName string) TopologyHint
	RemoveContainer(containerID string) error
	AddContainer(pod *v1.Pod, containerID string) error
}

type scope struct {
	mutex sync.Mutex
	name string
	//Mapping of a Pods mapping of Containers and their TopologyHints
	//Indexed by PodUID to ContainerName
	podTopologyHints PodTopologyHints
	hintProviders []HintProvider
	policy Policy
	podMap map[string]string
}

func (s *scope) Name() string {
	return s.name
}

func (s *scope) allocateAlignedResources(pod *v1.Pod, container *v1.Container) error {
	for _, provider := range s.hintProviders {
		err := provider.Allocate(pod, container)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *scope) admitPolicyNone(pod *v1.Pod) lifecycle.PodAdmitResult{

	for _, container := range append(pod.Spec.InitContainers, pod.Spec.Containers...) {
		err := s.allocateAlignedResources(pod, &container)
		if err != nil {
			return unexpectedAdmissionError(err)
		}
	}
	return admitPod()
}

func (s *scope) AddHintProvider(h HintProvider) {
	s.hintProviders = append(s.hintProviders, h)
}

func (s *scope) GetAffinity(podUID string, containerName string) TopologyHint {
	return s.podTopologyHints[podUID][containerName]
}

func (s *scope) AddContainer(pod *v1.Pod, containerID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.podMap[containerID] = string(pod.UID)
	return nil
}

func (s *scope) RemoveContainer(containerID string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	klog.Infof("[topologymanager] RemoveContainer - Container ID: %v", containerID)
	podUIDString := s.podMap[containerID]
	delete(s.podMap, containerID)
	if _, exists := s.podTopologyHints[podUIDString]; exists {
		delete(s.podTopologyHints[podUIDString], containerID)
		if len(s.podTopologyHints[podUIDString]) == 0 {
			delete(s.podTopologyHints, podUIDString)
		}
	}

	return nil
}