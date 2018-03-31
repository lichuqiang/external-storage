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

package deleter

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/local-volume/provisioner/pkg/common"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
)

type provisionedVolumeDeleteFuncType func(*v1.PersistentVolume) error

// Deleter handles PV cleanup and object deletion
// For file-based volumes, it deletes the contents of the directory
type Deleter struct {
	*common.RuntimeConfig
	ProcTable                   common.ProcTable
	provisionedVolumeDeleteFunc provisionedVolumeDeleteFuncType
}

// NewDeleter creates a Deleter object to handle the cleanup and deletion of local PVs
// allocated by this provisioner
func NewDeleter(config *common.RuntimeConfig, procTable common.ProcTable, provisionedVolumeDeleteFunc provisionedVolumeDeleteFuncType) *Deleter {
	return &Deleter{
		RuntimeConfig:               config,
		ProcTable:                   procTable,
		provisionedVolumeDeleteFunc: provisionedVolumeDeleteFunc,
	}
}

// DeletePVs will scan through all the existing PVs that are released, and cleanup and
// delete them
func (d *Deleter) DeletePVs() {
	for _, pv := range d.Cache.ListPVs() {
		if pv.Status.Phase == v1.VolumeReleased {
			name := pv.Name
			// Cleanup volume
			err := d.deletePV(pv)
			if err != nil {
				cleaningLocalPVErr := fmt.Errorf("Error cleaning PV %q: %v", name, err.Error())
				d.RuntimeConfig.Recorder.Eventf(pv, v1.EventTypeWarning, common.EventVolumeFailedDelete, cleaningLocalPVErr.Error())
				glog.Error(err)
				continue
			}
		}
	}
}

func (d *Deleter) deletePV(pv *v1.PersistentVolume) error {
	if pv.Spec.Local == nil {
		return fmt.Errorf("Unsupported volume type")
	}
	var mountConfig common.MountConfig
	storageSourceConfig, isProvisioned := d.ProvisionSourceMap[pv.Spec.StorageClassName]
	if isProvisioned {
		mountConfig = *storageSourceConfig.MountConfig
	} else {
		discoveryConfig, isDiscovered := d.DiscoveryMap[pv.Spec.StorageClassName]
		if !isDiscovered {
			return fmt.Errorf("Unknown storage class name %s", pv.Spec.StorageClassName)
		}
		mountConfig = *discoveryConfig.MountConfig
	}

	mountPath, err := common.GetContainerPath(pv, mountConfig)
	if err != nil {
		return err
	}

	if d.ProcTable.IsRunning(pv.Name) {
		// Run in progress, nothing to do,
		return nil
	}

	err = d.ProcTable.MarkRunning(pv.Name)
	if err != nil {
		return err
	}

	go d.asyncDeletePV(pv, mountPath, mountConfig, isProvisioned)

	return nil
}

func (d *Deleter) asyncDeletePV(pv *v1.PersistentVolume, mountPath string, config common.MountConfig, shouldDeleteVol bool) {
	defer d.ProcTable.MarkDone(pv.Name)

	// Make absolutely sure here that we are not deleting anything outside of mounted dir
	if !strings.HasPrefix(mountPath, config.MountDir) {
		err := fmt.Errorf("Unexpected error pv %q mountPath %s but mount dir is %s", pv.Name, mountPath,
			config.MountDir)
		glog.Error(err)
		return
	}

	isDevice, err := d.VolUtil.IsBlock(mountPath)
	if err != nil {
		glog.Errorf("Error checking if path %s is of deivce: %v", mountPath, err)
		return
	}

	if isDevice {
		err = d.cleanupBlockPV(pv, mountPath, config.BlockCleanerCommand)
	} else {
		err = d.deleteFilePV(pv, mountPath, config)
	}

	if err != nil {
		glog.Error(err)
		return
	}

	// Recycle dynamically provisioned volume behind the PV
	if shouldDeleteVol {
		if err := d.provisionedVolumeDeleteFunc(pv); err != nil {
			glog.Error(err)
			return
		}
	}

	// Remove API object
	if err := d.APIUtil.DeletePV(pv.Name); err != nil {
		if !errors.IsNotFound(err) {
			deletingLocalPVErr := fmt.Errorf("Error deleting PV %q: %v", pv.Name, err.Error())
			d.RuntimeConfig.Recorder.Eventf(pv, v1.EventTypeWarning, common.EventVolumeFailedDelete,
				deletingLocalPVErr.Error())
			glog.Error(deletingLocalPVErr)
			return
		}
	}

	glog.Infof("Deleted PV %q", pv.Name)

}

func (d *Deleter) deleteFilePV(pv *v1.PersistentVolume, mountPath string, config common.MountConfig) error {
	glog.Infof("Deleting PV file volume %q contents at hostpath %q, mountpath %q", pv.Name, pv.Spec.Local.Path,
		mountPath)

	return d.VolUtil.DeleteContents(mountPath)
}

func (d *Deleter) cleanupBlockPV(pv *v1.PersistentVolume, blkdevPath string, blockCleanerCommand []string) error {

	if len(blockCleanerCommand) < 1 {
		err := fmt.Errorf("Blockcleaner command was empty for pv %q ountPath %s", pv.Name, blkdevPath)
		glog.Error(err)
		return err
	}

	cleaningInfo := fmt.Errorf("Starting cleanup of Block PV %q, this may take a while", pv.Name)
	d.RuntimeConfig.Recorder.Eventf(pv, v1.EventTypeNormal, common.VolumeDelete, cleaningInfo.Error())
	glog.Infof("Deleting PV block volume %q device hostpath %q, mountpath %q", pv.Name, pv.Spec.Local.Path,
		blkdevPath)

	err := d.execScript(pv.Name, blkdevPath, blockCleanerCommand[0], blockCleanerCommand[1:]...)
	if err != nil {
		glog.Error(err)
		return err
	}
	glog.Infof("Completed cleanup of pv %q", pv.Name)

	return nil
}

func (d *Deleter) execScript(pvName string, blkdevPath string, exe string, exeArgs ...string) error {
	cmd := exec.Command(exe, exeArgs...)
	cmd.Env = append(os.Environ(), fmt.Sprintf("%s=%s", common.LocalPVEnv, blkdevPath))
	var wg sync.WaitGroup
	// Wait for stderr & stdout  go routines
	wg.Add(2)

	outReader, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	go func() {
		defer wg.Done()
		outScanner := bufio.NewScanner(outReader)
		for outScanner.Scan() {
			outstr := outScanner.Text()
			glog.Infof("Cleanup pv %q: StdoutBuf - %q", pvName, outstr)
		}
	}()

	errReader, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	go func() {
		defer wg.Done()
		errScanner := bufio.NewScanner(errReader)
		for errScanner.Scan() {
			errstr := errScanner.Text()
			glog.Infof("Cleanup pv %q: StderrBuf - %q", pvName, errstr)
		}
	}()

	err = cmd.Start()
	if err != nil {
		return err
	}

	wg.Wait()
	err = cmd.Wait()
	if err != nil {
		return err
	}

	return nil
}
