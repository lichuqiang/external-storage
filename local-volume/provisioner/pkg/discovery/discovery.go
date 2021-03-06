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

package discovery

import (
	"fmt"
	"hash/fnv"
	"path/filepath"

	"github.com/golang/glog"
	esUtil "github.com/kubernetes-incubator/external-storage/lib/util"
	"github.com/kubernetes-incubator/external-storage/local-volume/provisioner/pkg/common"
	"k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/util/mount"
)

// Discoverer finds available volumes and creates PVs for them
// It looks for volumes in the directories specified in the discoveryMap
type Discoverer struct {
	*common.RuntimeConfig
	Labels map[string]string
	// ProcTable is a reference to running processes so that we can prevent PV from being created while
	// it is being cleaned
	ProcTable       common.ProcTable
	nodeAffinityAnn string
	nodeAffinity    *v1.VolumeNodeAffinity
}

// NewDiscoverer creates a Discoverer object that will scan through
// the configured directories and create local PVs for any new directories found
func NewDiscoverer(config *common.RuntimeConfig, procTable common.ProcTable) (*Discoverer, error) {
	labelMap := make(map[string]string)
	for _, labelName := range config.NodeLabelsForPV {
		labelVal, ok := config.Node.Labels[labelName]
		if ok {
			labelMap[labelName] = labelVal
		}
	}

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
		return &Discoverer{
			RuntimeConfig:   config,
			Labels:          labelMap,
			ProcTable:       procTable,
			nodeAffinityAnn: tmpAnnotations[v1.AlphaStorageNodeAffinityAnnotation]}, nil
	}

	volumeNodeAffinity, err := common.GenerateVolumeNodeAffinity(config.Node)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate volume node affinity: %v", err)
	}

	return &Discoverer{
		RuntimeConfig: config,
		Labels:        labelMap,
		ProcTable:     procTable,
		nodeAffinity:  volumeNodeAffinity}, nil
}

// DiscoverLocalVolumes reads the configured discovery paths, and creates PVs for the new volumes
func (d *Discoverer) DiscoverLocalVolumes() {
	for class, config := range d.DiscoveryMap {
		d.discoverVolumesAtPath(class, config)
	}
}

func (d *Discoverer) discoverVolumesAtPath(class string, config common.DiscoveryConfig) {
	glog.V(7).Infof("Discovering volumes at hostpath %q, mount path %q for storage class %q", config.HostDir, config.MountDir, class)

	files, err := d.VolUtil.ReadDir(config.MountDir)
	if err != nil {
		glog.Errorf("Error reading directory: %v", err)
		return
	}

	// Retreive list of mount points to iterate through discovered paths (aka files) below
	mountPoints, mountPointsErr := d.RuntimeConfig.Mounter.List()
	if mountPointsErr != nil {
		glog.Errorf("Error retreiving mountpoints: %v", err)
		return
	}
	// Put mount moints into set for faster checks below
	type empty struct{}
	mountPointMap := make(map[string]empty)
	for _, mp := range mountPoints {
		mountPointMap[mp.Path] = empty{}
	}

	for _, file := range files {
		filePath := filepath.Join(config.MountDir, file)
		volMode, err := d.getVolumeMode(filePath)
		if err != nil {
			glog.Error(err)
			continue
		}
		// Check if PV already exists for it
		pvName := generatePVName(file, d.Node.Name, class)
		pv, exists := d.Cache.GetPV(pvName)
		if exists {
			if volMode == v1.PersistentVolumeBlock && (pv.Spec.VolumeMode == nil ||
				*pv.Spec.VolumeMode != v1.PersistentVolumeBlock) {
				glog.Errorf("Incorrect Volume Mode: PV %q (path %q) was not created in block mode. "+
					"Please check if BlockVolume features gate has been enabled for the cluster.", pvName, filePath)
			}
			continue
		}

		if d.ProcTable.IsRunning(pvName) {
			glog.Infof("PV %s is still being cleaned, not going to recreate it", pvName)
			continue
		}

		var capacityByte int64
		switch volMode {
		case v1.PersistentVolumeBlock:
			capacityByte, err = d.VolUtil.GetBlockCapacityByte(filePath)
			if err != nil {
				glog.Errorf("Path %q block stats error: %v", filePath, err)
				continue
			}
			// Check if device still in state of mounted
			refs, err := getMountRefsByDev(mountPoints, filePath)
			if err != nil {
				glog.Errorf("Error retreiving mounting information of %s: %v", filePath, err)
				continue
			}
			if len(refs) > 0 {
				glog.Errorf("Path %s still mounted, skipping...", filePath)
				continue
			}
		case v1.PersistentVolumeFilesystem:
			// Validate that this path is an actual mountpoint
			if _, isMntPnt := mountPointMap[filePath]; isMntPnt == false {
				glog.Errorf("Path %q is not an actual mountpoint", filePath)
				continue
			}
			capacityByte, err = d.VolUtil.GetFsCapacityByte(filePath)
			if err != nil {
				glog.Errorf("Path %q fs stats error: %v", filePath, err)
				continue
			}
		default:
			glog.Errorf("Path %q has unexpected volume type %q", filePath, volMode)
			continue
		}

		d.createPV(file, class, config, capacityByte)
	}
}

func (d *Discoverer) getVolumeMode(fullPath string) (v1.PersistentVolumeMode, error) {
	isdir, errdir := d.VolUtil.IsDir(fullPath)
	if isdir {
		return v1.PersistentVolumeFilesystem, nil
	}
	// check for Block before returning errdir
	isblk, errblk := d.VolUtil.IsBlock(fullPath)
	if isblk {
		return v1.PersistentVolumeBlock, nil
	}

	if errdir == nil && errblk == nil {
		return "", fmt.Errorf("Skipping file %q: not a directory nor block device", fullPath)
	}

	// report the first error found
	if errdir != nil {
		return "", fmt.Errorf("Directory check for %q failed: %s", fullPath, errdir)
	}
	return "", fmt.Errorf("Block device check for %q failed: %s", fullPath, errblk)
}

func generatePVName(file, node, class string) string {
	h := fnv.New32a()
	h.Write([]byte(file))
	h.Write([]byte(node))
	h.Write([]byte(class))
	// This is the FNV-1a 32-bit hash
	return fmt.Sprintf("local-pv-%x", h.Sum32())
}

func (d *Discoverer) createPV(file, class string, config common.DiscoveryConfig, capacityByte int64) {
	pvName := generatePVName(file, d.Node.Name, class)
	outsidePath := filepath.Join(config.HostDir, file)

	glog.Infof("Found new volume of volumeMode %q at host path %q with capacity %d, creating Local PV %q",
		config.VolumeMode, outsidePath, capacityByte, pvName)

	localPVConfig := &common.LocalPVConfig{
		Name:           pvName,
		HostPath:       outsidePath,
		Capacity:       roundDownCapacityPretty(capacityByte),
		StorageClass:   class,
		ProvisionerTag: d.Tag,
		VolumeMode:     config.VolumeMode,
		Labels:         d.Labels,
		AccessModes: []v1.PersistentVolumeAccessMode{
			v1.ReadWriteOnce,
		},
		AdditionalAnn: map[string]string{},
	}

	if d.UseAlphaAPI {
		localPVConfig.UseAlphaAPI = true
		localPVConfig.AffinityAnn = d.nodeAffinityAnn
	} else {
		localPVConfig.NodeAffinity = d.nodeAffinity
	}

	pvSpec := common.CreateLocalPVSpec(localPVConfig)

	_, err := d.APIUtil.CreatePV(pvSpec)
	if err != nil {
		glog.Errorf("Error creating PV %q for volume at %q: %v", pvName, outsidePath, err)
		return
	}
	glog.Infof("Created PV %q for volume at %q", pvName, outsidePath)
}

// Round down the capacity to an easy to read value.
func roundDownCapacityPretty(capacityBytes int64) int64 {

	easyToReadUnitsBytes := []int64{esUtil.GiB, esUtil.MiB}

	// Round down to the nearest easy to read unit
	// such that there are at least 10 units at that size.
	for _, easyToReadUnitBytes := range easyToReadUnitsBytes {
		// Round down the capacity to the nearest unit.
		size := capacityBytes / easyToReadUnitBytes
		if size >= 10 {
			return size * easyToReadUnitBytes
		}
	}
	return capacityBytes
}

func getMountRefsByDev(mps []mount.MountPoint, mountPath string) ([]string, error) {
	slTarget, err := filepath.EvalSymlinks(mountPath)
	if err != nil {
		slTarget = mountPath
	}

	// Finding the device mounted to mountPath
	diskDev := ""
	for i := range mps {
		if slTarget == mps[i].Path {
			diskDev = mps[i].Device
			break
		}
	}

	// Find all references to the device.
	var refs []string
	for i := range mps {
		if mps[i].Device == diskDev || mps[i].Device == slTarget {
			if mps[i].Path != slTarget {
				refs = append(refs, mps[i].Path)
			}
		}
	}
	return refs, nil
}
