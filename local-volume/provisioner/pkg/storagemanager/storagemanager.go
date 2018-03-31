/*
Copyright 2018 The Kubernetes Authors.

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

package storagemanager

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/local-volume/provisioner/pkg/common"
	"github.com/kubernetes-incubator/external-storage/local-volume/provisioner/pkg/storagemanager/backend"

	"k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

const defaultRetryCount = 3
const defaultRetryWaitDuration = 2 * time.Second

type StorageManager interface {
	// CreateLocalVolume create volume basing on given claim,
	// along with a PV object that is pre-bound to the claim
	CreateLocalVolume(claim *v1.PersistentVolumeClaim) error
	// DeleteLocalVolume clean up the backend volume of the pv
	DeleteLocalVolume(pv *v1.PersistentVolume) error
}

type ManagerImpl struct {
	*common.RuntimeConfig
	ProcTable       common.ProcTable
	Backends        map[string]backend.DynamicProvisioningBackend
	Labels          map[string]string
	nodeAffinityAnn string
	nodeAffinity    *v1.VolumeNodeAffinity
}

func NewStorageManager(config *common.RuntimeConfig) (*ManagerImpl, error) {
	// Initialize the backends
	backends := make(map[string]backend.DynamicProvisioningBackend)
	for class, source := range config.ProvisionSourceMap {
		if source.Lvm != nil {
			// Since it's not allowed to specify multi backend in a same provisioner,
			// Initialize lvm backend and return once its source found in the map
			backends[class] = backend.NewLvmBackend(source.MountConfig.HostDir, source.Lvm.VolumeGroup)
		}
		// TODO: initialize backends from other sources
	}

	// Generate labels that will be used on the provisioned PVs
	labelMap := make(map[string]string)
	for _, labelName := range config.NodeLabelsForPV {
		labelVal, ok := config.Node.Labels[labelName]
		if ok {
			labelMap[labelName] = labelVal
		}
	}

	manager := &ManagerImpl{
		RuntimeConfig: config,
		ProcTable:     common.NewProcTable(),
		Backends:      backends,
		Labels:        labelMap,
	}

	// Generate node affinity information,
	if config.UseAlphaAPI {
		nodeAffinity, err := common.GenerateNodeAffinity(config.Node)
		if err != nil {
			return nil, fmt.Errorf("Failed to generate node affinity: %v", err)
		}
		tmpAnnotations := map[string]string{}
		err = helper.StorageNodeAffinityToAlphaAnnotation(tmpAnnotations, nodeAffinity)
		if err != nil {
			return nil, fmt.Errorf("Failed to convert node affinity to alpha annotation: %v", err)
		}
		manager.nodeAffinityAnn = tmpAnnotations[v1.AlphaStorageNodeAffinityAnnotation]
	} else {
		volumeNodeAffinity, err := common.GenerateVolumeNodeAffinity(config.Node)
		if err != nil {
			return nil, fmt.Errorf("Failed to generate volume node affinity: %v", err)
		}
		manager.nodeAffinity = volumeNodeAffinity
	}

	return manager, nil
}

func (m *ManagerImpl) Start() {
	go wait.Until(m.volumeProvisionWorker, time.Second, wait.NeverStop)
	go wait.Until(m.reconcileCapacity, 30*time.Second, wait.NeverStop)
}

func (m *ManagerImpl) volumeProvisionWorker() {
	workFunc := func() bool {
		keyObj, quit := m.ProvisionQueue.Get()
		if quit {
			return true
		}
		defer m.ProvisionQueue.Done(keyObj)

		claim, ok := keyObj.(*v1.PersistentVolumeClaim)
		if !ok {
			glog.Errorf("Object is not a *v1.PersistentVolumeClaim")
			return false
		}

		pvName := getProvisionedVolumeNameForClaim(claim)
		pv, exists := m.Cache.GetPV(pvName)
		if exists {
			if pv.Spec.ClaimRef == nil ||
				pv.Spec.ClaimRef.Name != claim.Name ||
				pv.Spec.ClaimRef.Namespace != claim.Namespace {
				glog.Errorf("PV %q already exist, but not bound to claim %q", pvName, getClaimName(claim))
			}
			return false
		}

		if m.ProcTable.IsRunning(getClaimName(claim)) {
			// Run in progress, nothing to do,
			return false
		}

		err := m.ProcTable.MarkRunning(getClaimName(claim))
		if err != nil {
			glog.Errorf("Error marking claim %q provisoning process as running in ProcTable: %v", getClaimName(claim), err)
			return false
		}

		if err := m.CreateLocalVolume(claim); err != nil {
			glog.Errorf("Error creating volumes for claim %q: %v", getClaimName(claim), err)
			// Signal back to the scheduler to retry dynamic provisioning
			// by removing the "annSelectedNode" annotation
			annotations := claim.Annotations
			delete(annotations, common.AnnSelectedNode)
			delete(annotations, common.AnnProvisionedTopology)
			claim.Annotations = annotations
			for i := 0; i <= defaultRetryCount; i++ {
				if _, err := m.APIUtil.UpdatePVC(claim); err == nil {
					break
				}
				glog.Errorf("Failed to update claim %q: %v", getClaimName(claim), err)
				time.Sleep(defaultRetryWaitDuration)
			}

			return false
		}
		return false
	}
	for {
		if quit := workFunc(); quit {
			glog.Infof("Provision worker queue shutting down")
			return
		}
	}
}

func (m *ManagerImpl) reconcileCapacity() {
	tickerPeriod := 2 * time.Second

	// In case the provisioners on different nodes start in rapid succession,
	// Let the worker wait for a random portion of tickerPeriod before reconciling.
	time.Sleep(time.Duration(rand.Float64() * float64(tickerPeriod)))
	for className, volumeBackend := range m.Backends {
		// Return true if need to retry
		workFunc := func() bool {
			// Size in GB
			actualSize, err := volumeBackend.GetCapacity()
			if err != nil {
				glog.Errorf("Failed to get actual capacity of class %q: %v", className, err)
				return true
			}
			actualCapacity := resource.MustParse(strconv.Itoa(int(actualSize)) + "G")
			class, err := m.APIUtil.GetStorageClass(className)
			if err != nil {
				glog.Errorf("Failed to get storage class %q: %v", className, err)
				return true
			}
			classStatus := class.Status
			if classStatus == nil || classStatus.Capacity == nil {
				classStatus = &storage.StorageClassStatus{
					Capacity: &storage.StorageClassCapacity{
						TopologyKey: common.NodeLabelKey,
						Capacities:  make(map[string]resource.Quantity),
					},
				}
				class.Status = classStatus
			}
			if classStatus.Capacity.TopologyKey != common.NodeLabelKey {
				glog.Errorf("Topology Key in storage class %q is not %q, but %q", className, common.NodeLabelKey, classStatus.Capacity.TopologyKey)
				// TODO: Should we modify the topology key if not match?
				return false
			}
			recordedCapacity, ok := classStatus.Capacity.Capacities[m.Node.Name]
			if ok {
				if recordedCapacity.Cmp(actualCapacity) == 0 {
					// Skip if capacity not changed
					return false
				}
			}

			classStatus.Capacity.Capacities[m.Node.Name] = actualCapacity
			if _, err := m.APIUtil.UpdateStorageClass(class); err != nil {
				glog.Errorf("Failed to update storage class %q: %v", className, err)
				return true
			}
			return false
		}
		for i := 0; i <= defaultRetryCount; i++ {
			if shouldRetry := workFunc(); !shouldRetry {
				glog.Infof("Capacity reconcile of storage class %q finished", className)
				return
			}
			time.Sleep(defaultRetryWaitDuration)
		}

	}
}

func (m *ManagerImpl) CreateLocalVolume(claim *v1.PersistentVolumeClaim) error {
	defer m.ProcTable.MarkDone(getClaimName(claim))

	// If annProvisionerTopology was not set, then it means
	// the scheduler made a decision not taking into account
	// capacity.
	// Do not try provisioning if the capacity in StorageClass
	// has not been initialized.
	// TODO: Double check the value of the annotation if needed
	if _, ok := claim.Annotations[common.AnnProvisionedTopology]; !ok {
		return fmt.Errorf("capacity not reported yet, skip creating volumes for claim %q", getClaimName(claim))
	}

	className := helper.GetPersistentVolumeClaimClass(claim)
	volumeBackend, ok := m.Backends[className]
	if !ok {
		// Backend does not exist, return error
		// This should not happen
		return fmt.Errorf("cannot handle volume creation of storage class: %s", className)
	}

	//class, err := m.APIUtil.GetStorageClass(className)
	//if err != nil {
	//	glog.Errorf("Failed to get storage class %q: %v", className, err)
	//	return err
	//}

	capacity := claim.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	sizeBytes := capacity.Value()
	// Convert to GiB with rounding up
	sizeGB := int64((sizeBytes + 1024*1024*1024 - 1) / (1024 * 1024 * 1024))

	// Create local volume
	volName := getProvisionedVolumeNameForClaim(claim)
	volReq := &backend.LocalVolumeReq{
		VolumeName: volName,
		SizeInGb:   sizeGB,
	}

	// Prepare a claimRef to the claim early (to fail before a volume is
	// provisioned)
	claimRef, err := ref.GetReference(scheme.Scheme, claim)
	if err != nil {
		glog.Errorf("Unexpected error getting claim reference to claim %q: %v", getClaimName(claim), err)
		return nil
	}

	volInfo, err := volumeBackend.CreateLocalVolume(volReq)
	if err != nil {
		return err
	}

	localPVConfig := &common.LocalPVConfig{
		Name:           volName,
		HostPath:       volInfo.VolumePath,
		Capacity:       sizeGB * (1024 * 1024 * 1024),
		StorageClass:   className,
		ProvisionerTag: m.Tag,
		VolumeMode:     *claim.Spec.VolumeMode,
		Labels:         m.Labels,
		AccessModes:    claim.Spec.AccessModes,
		AdditionalAnn:  map[string]string{},
		ClaimRef:       claimRef,
	}

	if m.UseAlphaAPI {
		localPVConfig.UseAlphaAPI = true
		localPVConfig.AffinityAnn = m.nodeAffinityAnn
	} else {
		localPVConfig.NodeAffinity = m.nodeAffinity
	}

	if annProvisionedTopology, ok := claim.Annotations[common.AnnProvisionedTopology]; ok {
		localPVConfig.AdditionalAnn[common.AnnProvisionedTopology] = annProvisionedTopology
	}

	pvSpec := common.CreateLocalPVSpec(localPVConfig)

	for trial := 0; trial <= defaultRetryCount; trial++ {
		if _, err := m.APIUtil.CreatePV(pvSpec); err == nil {
			break
		}
		// Recycle the volume created above if failed to create PV
		if trial >= defaultRetryCount {
			if delErr := volumeBackend.DeleteLocalVolume(volName); delErr != nil {
				glog.Errorf("Error clean up volume %q: %v", volName, delErr)
			}
			return err
		}
		// Create failed, try again after a while.
		glog.Errorf("Error creating PV %q for claim %q: %v", volName, getClaimName(claim), err)
		time.Sleep(defaultRetryWaitDuration)
	}

	glog.Infof("Created PV %q for claim %q", volName, getClaimName(claim))
	return nil

}

func (m *ManagerImpl) DeleteLocalVolume(pv *v1.PersistentVolume) error {
	pvClass := helper.GetPersistentVolumeClass(pv)
	volumeBackend, ok := m.Backends[pvClass]
	if !ok {
		// Backend does not exist, return error
		// This should not happen
		return fmt.Errorf("cannot handle volume deletion of storage class: %s", pvClass)
	}
	return volumeBackend.DeleteLocalVolume(pv.Name)
}

func getProvisionedVolumeNameForClaim(claim *v1.PersistentVolumeClaim) string {
	return "pvc-" + string(claim.UID)
}

func getClaimName(claim *v1.PersistentVolumeClaim) string {
	return claim.Namespace + "/" + claim.Name
}
