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
	"path/filepath"
	"strings"
	"testing"

	"github.com/kubernetes-incubator/external-storage/local-volume/provisioner/pkg/cache"
	"github.com/kubernetes-incubator/external-storage/local-volume/provisioner/pkg/common"
	"github.com/kubernetes-incubator/external-storage/local-volume/provisioner/pkg/util"

	"time"

	"reflect"

	"k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
)

const (
	testHostDir      = "/mnt/disks"
	testMountDir     = "/discoveryPath"
	testStorageClass = "sc1"
)

type testConfig struct {
	// Values defined by the test
	apiShouldFail       bool
	volDeleteShouldFail bool
	// Precreated PVs
	vols map[string]*testVol
	// Expected names of deleted PV
	expectedDeletedPVs map[string]string

	// ==== The remaining fields are set during setup ====
	// Map of PVs generated by setup for the test
	generatedPVs map[string]*v1.PersistentVolume
	// Layout of directory in which local volumes are searched for (optional, can be nil)
	searchDir  map[string][]*util.FakeDirEntry
	volUtil    *util.FakeVolumeUtil
	apiUtil    *util.FakeAPIUtil
	cache      *cache.VolumeCache
	procTable  *FakeProcTableImpl
	jobControl *FakeJobController
}

type testVol struct {
	pvPhase    v1.PersistentVolumePhase
	VolumeMode string
}

func TestDeleteVolumes_Basic(t *testing.T) {
	vols := map[string]*testVol{
		"pv1": {
			pvPhase: v1.VolumePending,
		},
		"pv2": {
			pvPhase: v1.VolumeAvailable,
		},
		"pv3": {
			pvPhase: v1.VolumeBound,
		},
		"pv4": {
			pvPhase: v1.VolumeReleased,
		},
		"pv5": {
			pvPhase: v1.VolumeFailed,
		},
	}
	expectedDeletedPVs := map[string]string{"pv4": ""}
	test := &testConfig{
		vols:               vols,
		expectedDeletedPVs: expectedDeletedPVs,
	}
	d := testSetupForProcCleaning(t, test, nil)

	d.DeletePVs()
	waitForAsyncToComplete(t, d, "pv4")
	verifyDeletedPVs(t, test)
}

func TestDeleteVolumes_Twice(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase: v1.VolumeReleased,
		},
	}
	expectedDeletedPVs := map[string]string{"pv4": ""}
	test := &testConfig{
		vols:               vols,
		expectedDeletedPVs: expectedDeletedPVs,
	}
	d := testSetupForProcCleaning(t, test, nil)

	d.DeletePVs()
	waitForAsyncToComplete(t, d)
	verifyDeletedPVs(t, test)

	d.DeletePVs()
	waitForAsyncToComplete(t, d)
	test.expectedDeletedPVs = map[string]string{}
	verifyDeletedPVs(t, test)
}

func TestDeleteVolumes_Empty(t *testing.T) {
	vols := map[string]*testVol{}
	expectedDeletedPVs := map[string]string{}
	test := &testConfig{
		vols:               vols,
		expectedDeletedPVs: expectedDeletedPVs,
	}
	d := testSetupForProcCleaning(t, test, nil)

	d.DeletePVs()
	waitForAsyncToComplete(t, d)
	verifyDeletedPVs(t, test)
}

func TestDeleteVolumes_DeletePVFails(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase: v1.VolumeReleased,
		},
	}
	test := &testConfig{
		apiShouldFail:      true,
		vols:               vols,
		expectedDeletedPVs: map[string]string{},
	}
	d := testSetupForProcCleaning(t, test, nil)

	d.DeletePVs()
	waitForAsyncToComplete(t, d)
	verifyDeletedPVs(t, test)
	verifyPVExists(t, test)
}

func TestDeleteVolumes_DeletePVNotFound(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase: v1.VolumeReleased,
		},
	}
	test := &testConfig{
		apiShouldFail:      false,
		vols:               vols,
		expectedDeletedPVs: map[string]string{"pv4": ""},
	}
	d := testSetupForProcCleaning(t, test, nil)

	d.DeletePVs()
	waitForAsyncToComplete(t, d)
	verifyDeletedPVs(t, test)

	// delete not found pv
	err := d.deletePV(test.generatedPVs["pv4"])
	if err != nil {
		t.Error(err)
	}
	waitForAsyncToComplete(t, d)

	recorderChan := d.RuntimeConfig.Recorder.(*record.FakeRecorder).Events
	select {
	case err := <-recorderChan:
		t.Errorf("error deletePV %v", err)
	default:
	}
}

func TestDeleteVolumes_CleanupFails(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase: v1.VolumeReleased,
		},
	}
	test := &testConfig{
		volDeleteShouldFail: true,
		vols:                vols,
		expectedDeletedPVs:  map[string]string{},
	}
	d := testSetupForProcCleaning(t, test, nil)

	d.DeletePVs()
	waitForAsyncToComplete(t, d)
	// A few checks to see if its still running.
	if test.procTable.IsRunningCount == 0 {
		t.Errorf("Unexpected isRunning count %d", test.procTable.IsRunningCount)
	}
	// Must have marked itself as running twice, since when it detected failure, it would have restarted it.
	if test.procTable.MarkRunningCount != 2 {
		t.Errorf("Unexpected MarkRunning count %d", test.procTable.MarkRunningCount)
	}
	// Must have marked itself has done
	if test.procTable.MarkDoneCount != 1 {
		t.Errorf("Unexpected MarkDone count %d", test.procTable.MarkDoneCount)
	}
	verifyDeletedPVs(t, test)
	verifyPVExists(t, test)
}

func TestDeleteBlock_BasicProcessExec(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase:    v1.VolumeReleased,
			VolumeMode: util.FakeEntryBlock,
		},
	}
	// The volume should be deleted after a successful cleanup
	expectedDeletedPVs := map[string]string{"pv4": ""}
	test := &testConfig{vols: vols, expectedDeletedPVs: expectedDeletedPVs}
	d := testSetupForProcCleaning(t, test, []string{"sh", "-c", "echo \"hello\""})

	err := d.deletePV(test.generatedPVs["pv4"])
	if err != nil {
		t.Error(err)
	}

	waitForAsyncToComplete(t, d)

	// A few checks to see if its still running.
	if test.procTable.IsRunningCount == 0 {
		t.Errorf("Unexpected isRunning count %d", test.procTable.IsRunningCount)
	}
	// Must have marked itself as running
	if test.procTable.MarkRunningCount != 1 {
		t.Errorf("Unexpected MarkRunning count %d", test.procTable.MarkRunningCount)
	}
	// Must have marked itself has done
	if test.procTable.MarkDoneCount != 1 {
		t.Errorf("Unexpected MarkDone count %d", test.procTable.MarkDoneCount)
	}

	verifyDeletedPVs(t, test)
}

func TestDeleteBlock_FailedProcess(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase:    v1.VolumeReleased,
			VolumeMode: util.FakeEntryBlock,
		},
	}
	// Nothing should be deleted as it was a failed cleanup process
	expectedDeletedPVs := map[string]string{}

	test := &testConfig{vols: vols, expectedDeletedPVs: expectedDeletedPVs}
	d := testSetupForProcCleaning(t, test, []string{"sh", "-c", "exit 10"})
	err := d.deletePV(test.generatedPVs["pv4"])
	if err != nil {
		t.Error(err)
	}

	waitForAsyncToComplete(t, d)

	// A few checks to see if its still running.
	if test.procTable.IsRunningCount == 0 {
		t.Errorf("Unexpected isRunning count %d", test.procTable.IsRunningCount)
	}
	// Must have marked itself as running twice, because when the first attempt failed, it would try again.
	if test.procTable.MarkRunningCount != 2 {
		t.Errorf("Unexpected MarkRunning count %d", test.procTable.MarkRunningCount)
	}
	// Must have marked itself has done
	if test.procTable.MarkDoneCount != 1 {
		t.Errorf("Unexpected MarkDone count %d", test.procTable.MarkDoneCount)
	}

	verifyDeletedPVs(t, test)

}

func TestDeleteBlock_DuplicateAttempts(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase:    v1.VolumeReleased,
			VolumeMode: util.FakeEntryBlock,
		},
	}
	expectedDeletedPVs := map[string]string{"pv4": ""}
	test := &testConfig{vols: vols, expectedDeletedPVs: expectedDeletedPVs}

	d := testSetupForProcCleaning(t, test, []string{"sh", "-c", "echo \"hello\""})

	// Simulate a currently running process by marking it as running in process table
	test.procTable.MarkRunning("pv4")

	err := d.deletePV(test.generatedPVs["pv4"])
	if err != nil {
		t.Error(err)
	}

	// The second execution must have checked to see if something is already running.
	if test.procTable.IsRunningCount != 1 {
		t.Errorf("Unexpected isrrunning %d", test.procTable.IsRunningCount)
	}

	// The second process should not have attempted to mark itself running.
	if test.procTable.MarkRunningCount != 1 {
		t.Errorf("Unexpected MarkRunning %d", test.procTable.MarkRunningCount)
	}

	// Nothing should have marked itself as done, since the first run was a fake and second run did not happen
	if test.procTable.MarkDoneCount != 0 {
		t.Errorf("Unexpected MarkDone %d", test.procTable.MarkDoneCount)
	}
}

func TestDeleteBlock_Jobs(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase:    v1.VolumeReleased,
			VolumeMode: util.FakeEntryBlock,
		},
	}
	expectedDeletedPVs := map[string]string{"pv4": ""}
	test := &testConfig{vols: vols, expectedDeletedPVs: expectedDeletedPVs}

	cmd := []string{"sh", "-c", "echo \"hello\""}
	d := testSetupForJobCleaning(t, test, cmd)

	err := d.deletePV(test.generatedPVs["pv4"])
	if err != nil {
		t.Error(err)
	}

	// The second execution must have checked to see if something is already running.
	if test.jobControl.IsRunningCount != 1 {
		t.Errorf("Unexpected isrrunning %d", test.procTable.IsRunningCount)
	}

	if len(test.apiUtil.CreatedJobs) != 1 {
		t.Fatalf("Job creation was not invoked correctly %+v", test.apiUtil.CreatedJobs)
	}

	for k, job := range test.apiUtil.CreatedJobs {
		nsjob := strings.Split(k, "/")
		if nsjob[0] != "kubesystem" {
			t.Fatalf("Invalid namespace in key %s", k)
		}
		if nsjob[1] != JobNamePrefix+"pv4" {
			t.Fatalf("Invalid namespace in key %s", k)
		}

		if job.Namespace != nsjob[0] {
			t.Fatalf("Invalid namespace in job %s", job.Namespace)
		}

		if job.Name != nsjob[1] {
			t.Fatalf("Invalid name in job %s", job.Name)
		}

		if job.Spec.Template.Spec.Containers[0].Env[0].Name != common.LocalPVEnv {
			t.Fatalf("Invalid environment variable set in job container %s",
				job.Spec.Template.Spec.Containers[0].Env[0].Name)
		}
		if job.Spec.Template.Spec.Containers[0].Env[0].Value != "/discoveryPath/test1/entry-pv4" {
			t.Fatalf("Invalid blkdevpath environment value set in job container %s",
				job.Spec.Template.Spec.Containers[0].Env[0].Value)
		}
		if !reflect.DeepEqual(job.Spec.Template.Spec.Containers[0].Command, cmd) {
			t.Fatalf("Invalid command set in job container - %+v",
				job.Spec.Template.Spec.Containers[0].Command)
		}
		if job.Annotations[DeviceAnnotation] != "/discoveryPath/test1/entry-pv4" {
			t.Fatalf("Invalid device annotation on job %s", job.Annotations[DeviceAnnotation])
		}
		if job.Labels[PVLabel] != "pv4" {
			t.Fatalf("Invalid PV label on job %s", job.Labels[PVLabel])
		}
		if job.Labels[PVUuidLabel] != "" {
			t.Fatalf("Invalid PV uuid label on job %s", job.Labels[PVUuidLabel])
		}
	}
}

func TestDeleteBlock_DuplicateAttempts_Jobs(t *testing.T) {
	vols := map[string]*testVol{
		"pv4": {
			pvPhase:    v1.VolumeReleased,
			VolumeMode: util.FakeEntryBlock,
		},
	}
	expectedDeletedPVs := map[string]string{"pv4": ""}
	test := &testConfig{vols: vols, expectedDeletedPVs: expectedDeletedPVs}

	d := testSetupForJobCleaning(t, test, []string{"sh", "-c", "echo \"hello\""})

	// Simulate a currently running process by marking it as running in process table
	test.jobControl.MarkRunning("pv4")

	err := d.deletePV(test.generatedPVs["pv4"])
	if err != nil {
		t.Error(err)
	}

	// The second execution must have checked to see if something is already running.
	if test.jobControl.IsRunningCount != 1 {
		t.Errorf("Unexpected isrrunning %d", test.procTable.IsRunningCount)
	}

	if len(test.apiUtil.CreatedJobs) != 0 {
		t.Errorf("Unexpected job was created. %+v", test.apiUtil.CreatedJobs)
		t.Fatalf("No job should have been created when one is already simulated as running.")
	}
}

func testSetupForProcCleaning(t *testing.T, config *testConfig, cleanupCmd []string) *Deleter {
	return testSetup(t, config, cleanupCmd, false)
}

func testSetupForJobCleaning(t *testing.T, config *testConfig, cleanupCmd []string) *Deleter {
	return testSetup(t, config, cleanupCmd, true)
}

func testSetup(t *testing.T, config *testConfig, cleanupCmd []string, useJobForCleaning bool) *Deleter {
	config.cache = cache.NewVolumeCache()
	config.apiUtil = util.NewFakeAPIUtil(false, config.cache)
	config.procTable = NewFakeProcTable()
	config.jobControl = NewFakeJobController()
	config.volUtil = util.NewFakeVolumeUtil(config.volDeleteShouldFail, map[string][]*util.FakeDirEntry{})

	containerImage := ""
	ns := ""
	if useJobForCleaning {
		containerImage = "busybox/busybox"
		ns = "kubesystem"
	}
	// Precreate PVs
	config.generatedPVs = map[string]*v1.PersistentVolume{}
	newVols := map[string][]*util.FakeDirEntry{}
	newVols["test1"] = []*util.FakeDirEntry{}
	for pvName, vol := range config.vols {
		fakePath := filepath.Join(testHostDir, "test1", "entry-"+pvName)
		lpvConfig := common.LocalPVConfig{
			Name:         pvName,
			HostPath:     fakePath,
			StorageClass: testStorageClass,
		}
		// If volume mode has been explicitly specified in the volume config, then explicitly set it in the PV.
		switch vol.VolumeMode {
		case util.FakeEntryBlock:
			lpvConfig.VolumeMode = v1.PersistentVolumeBlock
		case util.FakeEntryFile:
			lpvConfig.VolumeMode = v1.PersistentVolumeFilesystem
		}
		pv := common.CreateLocalPVSpec(&lpvConfig)
		pv.Status.Phase = vol.pvPhase

		_, err := config.apiUtil.CreatePV(pv)
		if err != nil {
			t.Fatalf("Error creating fake PV: %v", err)
		}
		// Add PV to cache
		config.cache.AddPV(pv)
		// Track it in the list of generated PVs
		config.generatedPVs[pvName] = pv
		// Make sure the fake Volumeutil knows about it
		newVols["test1"] = append(newVols["test1"], &util.FakeDirEntry{Name: "entry-" + pvName, Hash: 0xf34b8003,
			VolumeType: vol.VolumeMode})
	}
	// Update volume util
	config.volUtil.AddNewDirEntries(testMountDir, newVols)

	config.apiUtil = util.NewFakeAPIUtil(config.apiShouldFail, config.cache)

	userConfig := &common.UserConfig{
		DiscoveryMap: map[string]common.DiscoveryConfig{
			testStorageClass: {
				MountConfig: &common.MountConfig{
					HostDir:             testHostDir,
					MountDir:            testMountDir,
					BlockCleanerCommand: cleanupCmd,
				},
			},
		},
		Node:              &v1.Node{ObjectMeta: meta_v1.ObjectMeta{Name: "somehost.acme.com"}},
		UseJobForCleaning: useJobForCleaning,
		JobContainerImage: containerImage,
		Namespace:         ns,
	}

	// set buffer size big enough, not all cases care about recorder.
	fakeRecorder := record.NewFakeRecorder(100)
	runtimeConfig := &common.RuntimeConfig{
		UserConfig: userConfig,
		Cache:      config.cache,
		VolUtil:    config.volUtil,
		APIUtil:    config.apiUtil,
		Recorder:   fakeRecorder,
	}

	cleanupTracker := &CleanupStatusTracker{ProcTable: config.procTable, JobController: config.jobControl}
	return NewDeleter(runtimeConfig, cleanupTracker)
}

func verifyDeletedPVs(t *testing.T, config *testConfig) {
	deletedPVs := config.apiUtil.GetAndResetDeletedPVs()
	expectedLen := len(config.expectedDeletedPVs)
	actualLen := len(deletedPVs)
	if expectedLen != actualLen {
		t.Errorf("Expected %d deleted PVs, got %d", expectedLen, actualLen)
	}

	for pvName := range deletedPVs {
		_, found := config.expectedDeletedPVs[pvName]
		if !found {
			t.Errorf("Did not expect deleted PVs %q", pvName)
			continue
		}
		_, found = config.cache.GetPV(pvName)
		if found {
			t.Errorf("PV %q still exists in cache", pvName)
		}
	}
}

func verifyPVExists(t *testing.T, config *testConfig) {
	for pvName := range config.vols {
		_, found := config.cache.GetPV(pvName)
		if !found {
			t.Errorf("PV %q doesn't exist in cache", pvName)
		}
	}
}

// waitForAsyncToComplete Since commands are all async, this function helps wait for commands to complete.
func waitForAsyncToComplete(t *testing.T, d *Deleter, pvNames ...string) {
	for count := 0; count < 30 && !d.CleanupStatus.ProcTable.IsEmpty(); count++ {
		time.Sleep(200 * time.Millisecond)
	}

	// Run again to delete PVs that have been cleaned up.
	d.DeletePVs()
	for _, pvName := range pvNames {
		if d.CleanupStatus.ProcTable.IsRunning(pvName) {
			t.Errorf("Command failed to complete for pv " + pvName)
		}
	}
}
