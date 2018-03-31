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

package common

import (
	"fmt"
	"sync"
	"time"
)

// ProcTable Interface for tracking running processes
type ProcTable interface {
	IsRunning(objName string) bool
	IsEmpty() bool
	MarkRunning(objName string) error
	MarkDone(objName string)
}

var _ ProcTable = &ProcTableImpl{}

// ProcEntry represents an entry in the proc table
type ProcEntry struct {
	StartTime time.Time
}

// ProcTableImpl Implementation of BLockCleaner interface
type ProcTableImpl struct {
	mutex     sync.Mutex
	procTable map[string]ProcEntry
}

// NewProcTable returns a BlockCleaner
func NewProcTable() *ProcTableImpl {
	return &ProcTableImpl{procTable: make(map[string]ProcEntry)}
}

// IsRunning Check if cleanup process is still running
func (v *ProcTableImpl) IsRunning(objName string) bool {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	_, ok := v.procTable[objName]
	return ok
}

// IsEmpty Check if any cleanup process is running
func (v *ProcTableImpl) IsEmpty() bool {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	return len(v.procTable) == 0
}

// MarkRunning Indicate that process is running.
func (v *ProcTableImpl) MarkRunning(objName string) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	_, ok := v.procTable[objName]
	if ok {
		return fmt.Errorf("Failed to mark running of %q as it is already running, should never happen", objName)
	}
	v.procTable[objName] = ProcEntry{StartTime: time.Now()}
	return nil
}

// MarkDone Indicate the process is no longer running or being tracked.
func (v *ProcTableImpl) MarkDone(objName string) {
	v.mutex.Lock()
	defer v.mutex.Unlock()
	delete(v.procTable, objName)
}

// FakeProcTableImpl creates a mock proc table that enables testing.
type FakeProcTableImpl struct {
	realTable ProcTable
	// IsRunningCount keeps count of number of times IsRunning() was called
	IsRunningCount int
	// MarkRunningCount keeps count of number of times MarkRunning() was called
	MarkRunningCount int
	// MarkDoneCount keeps count of number of times MarkDone() was called
	MarkDoneCount int
}

var _ ProcTable = &FakeProcTableImpl{}

// NewFakeProcTable returns a BlockCleaner
func NewFakeProcTable() *FakeProcTableImpl {
	return &FakeProcTableImpl{realTable: NewProcTable()}
}

// IsRunning Check if cleanup process is still running
func (f *FakeProcTableImpl) IsRunning(objName string) bool {
	f.IsRunningCount++
	return f.realTable.IsRunning(objName)
}

// IsEmpty Check if any cleanup process is running
func (f *FakeProcTableImpl) IsEmpty() bool {
	return f.realTable.IsEmpty()
}

// MarkRunning Indicate that process is running.
func (f *FakeProcTableImpl) MarkRunning(objName string) error {
	f.MarkRunningCount++
	return f.realTable.MarkRunning(objName)
}

// MarkDone Indicate the process is no longer running or being tracked.
func (f *FakeProcTableImpl) MarkDone(objName string) {
	f.MarkDoneCount++
	f.realTable.MarkDone(objName)
}
