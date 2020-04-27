package memorymanager

import (
	"fmt"
	"reflect"
	"testing"

	cadvisorapi "github.com/google/cadvisor/info/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1alpha2"
	"k8s.io/kubernetes/pkg/kubelet/cm/containermap"
	"k8s.io/kubernetes/pkg/kubelet/cm/memorymanager/state"
	"k8s.io/kubernetes/pkg/kubelet/cm/topologymanager"
)

const (
	mb           = 1024 * 1024
	gb           = mb * 1024
	pageSize1Gb  = 1048576
	hugepages1Gi = v1.ResourceName(v1.ResourceHugePagesPrefix + "1Gi")
)

type mockState struct {
	assignments  state.ContainerMemoryAssignments
	machineState state.MemoryMap
}

func (s *mockState) ClearState() {
	s.machineState = state.MemoryMap{}
	s.assignments = make(state.ContainerMemoryAssignments)
}

func (s *mockState) SetMachineState(memoryMap state.MemoryMap) {
	s.machineState = memoryMap
}

func (s *mockState) SetMemoryBlocks(podUID string, containerName string, blocks []state.Block) {
	if _, ok := s.assignments[podUID]; !ok {
		s.assignments[podUID] = map[string][]state.Block{}
	}

	s.assignments[podUID][containerName] = blocks
}

func (s *mockState) SetMemoryAssignments(assignments state.ContainerMemoryAssignments) {
	s.assignments = assignments
}

func (s *mockState) Delete(podUID string, containerName string) {
	if _, ok := s.assignments[podUID]; !ok {
		return
	}

	if _, ok := s.assignments[podUID][containerName]; !ok {
		return
	}

	delete(s.assignments[podUID], containerName)
	if len(s.assignments[podUID]) == 0 {
		delete(s.assignments, podUID)
	}
}

func (s *mockState) GetMachineState() state.MemoryMap {
	return s.machineState.Clone()
}

func (s *mockState) GetMemoryBlocks(podUID string, containerName string) []state.Block {
	if res, ok := s.assignments[podUID][containerName]; ok {
		return append([]state.Block{}, res...)
	}
	return nil
}

func (s *mockState) GetMemoryAssignments() state.ContainerMemoryAssignments {
	return s.assignments.Clone()
}

type mockPolicy struct {
	err error
}

func (p *mockPolicy) Name() string {
	return "mock"
}

func (p *mockPolicy) Start(s state.State) error {
	return p.err
}

func (p *mockPolicy) Allocate(s state.State, pod *v1.Pod, container *v1.Container) error {
	return p.err
}

func (p *mockPolicy) RemoveContainer(s state.State, podUID string, containerName string) error {
	return p.err
}

func (p *mockPolicy) GetTopologyHints(s state.State, pod *v1.Pod, container *v1.Container) map[string][]topologymanager.TopologyHint {
	return nil
}

type mockRuntimeService struct {
	err error
}

func (rt mockRuntimeService) UpdateContainerResources(id string, resources *runtimeapi.LinuxContainerResources) error {
	return rt.err
}

type mockPodStatusProvider struct {
	podStatus v1.PodStatus
	found     bool
}

func (psp mockPodStatusProvider) GetPodStatus(uid types.UID) (v1.PodStatus, bool) {
	return psp.podStatus, psp.found
}

func makePod(podUID, containerName, memoryRequest, memoryLimit string) *v1.Pod {
	pod := &v1.Pod{
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1000m"),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse(memoryRequest),
						},
						Limits: v1.ResourceList{
							v1.ResourceName(v1.ResourceCPU):    resource.MustParse("1000m"),
							v1.ResourceName(v1.ResourceMemory): resource.MustParse(memoryLimit),
						},
					},
				},
			},
		},
	}

	pod.UID = types.UID(podUID)
	pod.Spec.Containers[0].Name = containerName

	return pod
}

type testMemoryManager struct {
	description string
}

func TestMemoryManagerStart(t *testing.T) {}
func TestRemoveContainer(t *testing.T)    {}

func TestAddContainer(t *testing.T) {
	machineInfo := cadvisorapi.MachineInfo{
		Topology: []cadvisorapi.Node{
			{
				Id:     0,
				Memory: 128 * gb,
				HugePages: []cadvisorapi.HugePagesInfo{
					{
						// size in KB
						PageSize: pageSize1Gb,
						NumPages: 10,
					},
				},
			},
			{
				Id:     1,
				Memory: 128 * gb,
				HugePages: []cadvisorapi.HugePagesInfo{
					{
						// size in KB
						PageSize: pageSize1Gb,
						NumPages: 10,
					},
				},
			},
		},
	}
	machineState := state.MemoryMap{
		0: map[v1.ResourceName]*state.MemoryTable{
			v1.ResourceMemory: {
				Allocatable:    128 * gb,
				Free:           128 * gb,
				Reserved:       1 * gb,
				SystemReserved: 0 * gb,
				TotalMemSize:   128 * gb,
			},
			hugepages1Gi: {
				Allocatable:    10 * gb,
				Free:           10 * gb,
				Reserved:       0,
				SystemReserved: 0,
				TotalMemSize:   10 * gb,
			},
		},
	}
	reserved := reservedMemory{
		0: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
		1: map[v1.ResourceName]uint64{
			v1.ResourceMemory: 1 * gb,
		},
	}
	testPolicySingleNUMA := NewPolicySingleNUMA(&machineInfo, reserved, topologymanager.NewFakeManager())

	testCases := []struct {
		description        string
		updateErr          error
		policy             Policy
		expAllocateErr     error
		expAddContainerErr error
	}{
		{
			description:        "Correct allocation and container add",
			updateErr:          nil,
			policy:             testPolicySingleNUMA,
			expAllocateErr:     nil,
			expAddContainerErr: nil,
		},
		{
			description: "Policy returns an error",
			updateErr:   nil,
			policy: &mockPolicy{
				err: fmt.Errorf("Fake reg error"),
			},
			expAllocateErr:     fmt.Errorf("Fake reg error"),
			expAddContainerErr: nil,
		},
		{
			description:        "ContainerRuntime returns an error",
			updateErr:          fmt.Errorf("Fake reg error"),
			policy:             testPolicySingleNUMA,
			expAllocateErr:     nil,
			expAddContainerErr: nil,
		},
	}
	//1.Allocating and adding Containers
	//2.Setting reserved MemoryMap

	for i, testCase := range testCases {
		fmt.Printf("Test case: %v\n", i)
		mgr := &manager{
			policy: testCase.policy,
			state: &mockState{
				assignments: state.ContainerMemoryAssignments{},
				//machineState: state.MemoryMap{},
				machineState: machineState,
			},
			containerMap: containermap.NewContainerMap(),
			containerRuntime: mockRuntimeService{
				err: testCase.updateErr,
			},
			activePods:        func() []*v1.Pod { return nil },
			podStatusProvider: mockPodStatusProvider{},
		}
		mgr.sourcesReady = &sourcesReadyStub{}

		pod := makePod("fakePod", "fakeContainer", "1G", "1G")
		container := &pod.Spec.Containers[0]
		err := mgr.Allocate(pod, container)
		if !reflect.DeepEqual(err, testCase.expAllocateErr) {
			t.Errorf("Memory Manager Allocate() error (%v), expected error: %v but got: %v",
				testCase.description, testCase.expAllocateErr, err)
		}
		err = mgr.AddContainer(pod, container, "fakeID")
		if !reflect.DeepEqual(err, testCase.expAddContainerErr) {
			t.Errorf("Memory Manager AddContainer() error (%v), expected error: %v but got: %v",
				testCase.description, testCase.expAddContainerErr, err)
		}
	}
}
