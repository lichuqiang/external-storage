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

package backend

// Created Local Volume's detail Information
type LocalVolumeInfo struct {
	// Volume name
	VolumeName string `json:"volumeName"`

	// Size in GB
	SizeInGb int64 `json:"sizeInGb"`

	// Volume Path
	VolumePath string `json:"volumePath"`
}

// Local Volume configuration Information
type LocalVolumeReq struct {
	// Volume name
	VolumeName string

	// Size in GB
	SizeInGb int64
}

// DynamicProvisioningBackend is used to de-couple local storage manager and backend storage.
type DynamicProvisioningBackend interface {
	CreateLocalVolume(volReq *LocalVolumeReq) (*LocalVolumeInfo, error)
	DeleteLocalVolume(volName string) error
	GetAllLocalVolumes() ([]*LocalVolumeInfo, error)
	GetLocalVolume(volName string) (*LocalVolumeInfo, error)
	GetCapacity() (int64, error)
}

type FakeBackend struct {
}

func NewFakeBackend() FakeBackend {
	return FakeBackend{}
}

func (w *FakeBackend) CreateLocalVolume(volReq *LocalVolumeReq) (*LocalVolumeInfo, error) {
	return nil, nil
}

func (w *FakeBackend) DeleteLocalVolume(volName string) error {
	return nil
}

func (w *FakeBackend) GetAllLocalVolumes() ([]*LocalVolumeInfo, error) {
	return []*LocalVolumeInfo{}, nil
}

func (w *FakeBackend) GetLocalVolume(volName string) (*LocalVolumeInfo, error) {
	return nil, nil
}

func (w *FakeBackend) GetCapacity() (int64, error) {
	return 0, nil
}
