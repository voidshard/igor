// Code generated by MockGen. DO NOT EDIT.
// Source: ./pkg/api/interface.go
//
// Generated by this command:
//
//	mockgen -source=./pkg/api/interface.go -destination=internal/mocks/./pkg/api_mock/autogenerated.go -package=api_mock
//
// Package api_mock is a generated GoMock package.
package api_mock

import (
	reflect "reflect"

	api "github.com/voidshard/igor/pkg/api"
	structs "github.com/voidshard/igor/pkg/structs"
	gomock "go.uber.org/mock/gomock"
)

// MockAPI is a mock of API interface.
type MockAPI struct {
	ctrl     *gomock.Controller
	recorder *MockAPIMockRecorder
}

// MockAPIMockRecorder is the mock recorder for MockAPI.
type MockAPIMockRecorder struct {
	mock *MockAPI
}

// NewMockAPI creates a new mock instance.
func NewMockAPI(ctrl *gomock.Controller) *MockAPI {
	mock := &MockAPI{ctrl: ctrl}
	mock.recorder = &MockAPIMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAPI) EXPECT() *MockAPIMockRecorder {
	return m.recorder
}

// CreateJob mocks base method.
func (m *MockAPI) CreateJob(cjr *structs.CreateJobRequest) (*structs.CreateJobResponse, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateJob", cjr)
	ret0, _ := ret[0].(*structs.CreateJobResponse)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateJob indicates an expected call of CreateJob.
func (mr *MockAPIMockRecorder) CreateJob(cjr any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateJob", reflect.TypeOf((*MockAPI)(nil).CreateJob), cjr)
}

// CreateTasks mocks base method.
func (m *MockAPI) CreateTasks(in []*structs.CreateTaskRequest) ([]*structs.Task, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateTasks", in)
	ret0, _ := ret[0].([]*structs.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateTasks indicates an expected call of CreateTasks.
func (mr *MockAPIMockRecorder) CreateTasks(in any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateTasks", reflect.TypeOf((*MockAPI)(nil).CreateTasks), in)
}

// Jobs mocks base method.
func (m *MockAPI) Jobs(q *structs.Query) ([]*structs.Job, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Jobs", q)
	ret0, _ := ret[0].([]*structs.Job)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Jobs indicates an expected call of Jobs.
func (mr *MockAPIMockRecorder) Jobs(q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Jobs", reflect.TypeOf((*MockAPI)(nil).Jobs), q)
}

// Kill mocks base method.
func (m *MockAPI) Kill(r []*structs.ObjectRef) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Kill", r)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Kill indicates an expected call of Kill.
func (mr *MockAPIMockRecorder) Kill(r any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Kill", reflect.TypeOf((*MockAPI)(nil).Kill), r)
}

// Layers mocks base method.
func (m *MockAPI) Layers(q *structs.Query) ([]*structs.Layer, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Layers", q)
	ret0, _ := ret[0].([]*structs.Layer)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Layers indicates an expected call of Layers.
func (mr *MockAPIMockRecorder) Layers(q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Layers", reflect.TypeOf((*MockAPI)(nil).Layers), q)
}

// Pause mocks base method.
func (m *MockAPI) Pause(r []*structs.ObjectRef) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Pause", r)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Pause indicates an expected call of Pause.
func (mr *MockAPIMockRecorder) Pause(r any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Pause", reflect.TypeOf((*MockAPI)(nil).Pause), r)
}

// Retry mocks base method.
func (m *MockAPI) Retry(r []*structs.ObjectRef) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Retry", r)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Retry indicates an expected call of Retry.
func (mr *MockAPIMockRecorder) Retry(r any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Retry", reflect.TypeOf((*MockAPI)(nil).Retry), r)
}

// Skip mocks base method.
func (m *MockAPI) Skip(r []*structs.ObjectRef) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Skip", r)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Skip indicates an expected call of Skip.
func (mr *MockAPIMockRecorder) Skip(r any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Skip", reflect.TypeOf((*MockAPI)(nil).Skip), r)
}

// Tasks mocks base method.
func (m *MockAPI) Tasks(q *structs.Query) ([]*structs.Task, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Tasks", q)
	ret0, _ := ret[0].([]*structs.Task)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Tasks indicates an expected call of Tasks.
func (mr *MockAPIMockRecorder) Tasks(q any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Tasks", reflect.TypeOf((*MockAPI)(nil).Tasks), q)
}

// Unpause mocks base method.
func (m *MockAPI) Unpause(r []*structs.ObjectRef) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Unpause", r)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Unpause indicates an expected call of Unpause.
func (mr *MockAPIMockRecorder) Unpause(r any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Unpause", reflect.TypeOf((*MockAPI)(nil).Unpause), r)
}

// MockServer is a mock of Server interface.
type MockServer struct {
	ctrl     *gomock.Controller
	recorder *MockServerMockRecorder
}

// MockServerMockRecorder is the mock recorder for MockServer.
type MockServerMockRecorder struct {
	mock *MockServer
}

// NewMockServer creates a new mock instance.
func NewMockServer(ctrl *gomock.Controller) *MockServer {
	mock := &MockServer{ctrl: ctrl}
	mock.recorder = &MockServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockServer) EXPECT() *MockServerMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockServer) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockServerMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockServer)(nil).Close))
}

// ServeForever mocks base method.
func (m *MockServer) ServeForever(api api.API) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ServeForever", api)
	ret0, _ := ret[0].(error)
	return ret0
}

// ServeForever indicates an expected call of ServeForever.
func (mr *MockServerMockRecorder) ServeForever(api any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ServeForever", reflect.TypeOf((*MockServer)(nil).ServeForever), api)
}
