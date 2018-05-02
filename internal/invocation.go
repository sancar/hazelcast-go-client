// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
)

const RetryWaitTime = 1 * time.Second

type invocation struct {
	boundConnection         *Connection
	sentConnection          *Connection
	address                 *protocol.Address
	request                 *protocol.ClientMessage
	partitionID             int32
	response                chan *protocol.ClientMessage
	closed                  chan bool
	err                     chan error
	done                    chan bool
	eventHandler            func(clientMessage *protocol.ClientMessage)
	registrationId          *string
	timeout                 <-chan time.Time
	isTimedout              atomic.Value
	timedoutTime            atomic.Value
	listenerResponseDecoder protocol.DecodeListenerResponse
}

type connectionAndError struct {
	connection *Connection
	error      error
}

type invocationResult interface {
	Result() (*protocol.ClientMessage, error)
	AndThen(func(*protocol.ClientMessage, error))
}

func newInvocation(request *protocol.ClientMessage, partitionID int32, address *protocol.Address, connection *Connection, client *HazelcastClient) *invocation {
	invocation := &invocation{
		request:         request,
		partitionID:     partitionID,
		address:         address,
		boundConnection: connection,
		response:        make(chan *protocol.ClientMessage, 10),
		err:             make(chan error, 1),
		closed:          make(chan bool, 1),
		done:            make(chan bool, 1),
		timeout:         time.After(client.ClientConfig.ClientNetworkConfig().InvocationTimeout()),
	}
	invocation.isTimedout.Store(false)
	go func() {
		select {
		case <-invocation.done:
			return
		case <-invocation.timeout:
			invocation.timedoutTime.Store(time.Now())
			invocation.isTimedout.Store(true)
		}
	}()
	return invocation
}

func (i *invocation) Result() (*protocol.ClientMessage, error) {
	select {
	case response := <-i.response:
		i.done <- true
		return response, nil
	case err := <-i.err:
		i.done <- true
		return nil, err
	}
}

func (i *invocation) isBoundToSingleConnection() bool {
	return i.boundConnection != nil
}

func (i *invocation) AndThen(callback func(*protocol.ClientMessage, error) ) {
	go func() {
		select {
		case response := <-i.response:
			i.done <- true
			callback(response, nil)
		case err := <-i.err:
			i.done <- true
			callback(nil, err)
		}
	}()
}

type invocationService interface {
	invokeOnPartitionOwner(message *protocol.ClientMessage, partitionID int32) invocationResult
	invokeOnRandomTarget(message *protocol.ClientMessage) invocationResult
	invokeOnKeyOwner(message *protocol.ClientMessage, data *serialization.Data) invocationResult
	invokeOnTarget(message *protocol.ClientMessage, address *protocol.Address) invocationResult
	invokeOnConnection(message *protocol.ClientMessage, connection *Connection) invocationResult
	cleanupConnection(connection *Connection, e error)
	removeEventHandler(correlationID int64)
	sendInvocation(invocation *invocation) invocationResult
	shutdown()
}

func (is *invocationServiceImpl) invokeOnPartitionOwner(request *protocol.ClientMessage, partitionID int32) invocationResult {
	invocation := newInvocation(request, partitionID, nil, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationServiceImpl) invokeOnRandomTarget(request *protocol.ClientMessage) invocationResult {
	invocation := newInvocation(request, -1, nil, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationServiceImpl) invokeOnKeyOwner(request *protocol.ClientMessage, keyData *serialization.Data) invocationResult {
	partitionID := is.client.PartitionService.GetPartitionID(keyData)
	return is.invokeOnPartitionOwner(request, partitionID)
}

func (is *invocationServiceImpl) invokeOnTarget(request *protocol.ClientMessage, target *protocol.Address) invocationResult {
	invocation := newInvocation(request, -1, target, nil, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationServiceImpl) invokeOnConnection(request *protocol.ClientMessage, connection *Connection) invocationResult {
	invocation := newInvocation(request, -1, nil, connection, is.client)
	return is.sendInvocation(invocation)
}

func (is *invocationServiceImpl) cleanupConnection(connection *Connection, cause error) {
	is.cleanupConnectionChannel <- &connectionAndError{connection: connection, error: cause}
}

func (is *invocationServiceImpl) removeEventHandler(correlationID int64) {
	is.removeEventHandlerChannel <- correlationID
}

func (is *invocationServiceImpl) sendInvocation(invocation *invocation) invocationResult {
	select {
	case <-is.isClosedChan:
		invocation.err <- core.NewHazelcastClientNotActiveError("Client is shut down", nil)
		return invocation
	case is.sending <- invocation:
	}

	return invocation
}

func (is *invocationServiceImpl) retryInvocation(invocation *invocation, cause error) {
	select {
	case <-is.isClosedChan:
		invocation.err <- core.NewHazelcastClientNotActiveError("Client is shut down", cause)
	case is.sending <- invocation:
	}

}

func (is *invocationServiceImpl) shutdown() {
	is.isShutdown.Store(true)
	close(is.quit)
}

func (is *invocationServiceImpl) onConnectionClosed(connection *Connection, cause error) {
	is.cleanupConnection(connection, cause)
}

func (is *invocationServiceImpl) onConnectionOpened(connection *Connection) {
}

//internal definitions and methods called inside service process

type invocationServiceImpl struct {
	client                    *HazelcastClient
	quit                      chan struct{}
	isClosedChan              chan struct{}
	nextCorrelation           int64
	responseWaitings          map[int64]*invocation
	eventHandlers             map[int64]*invocation
	sending                   chan *invocation
	responseChannel           chan *protocol.ClientMessage
	cleanupConnectionChannel  chan *connectionAndError
	removeEventHandlerChannel chan int64
	notSentMessages           chan int64
	invoke                    func(*invocation)
	isShutdown                atomic.Value
}

func newInvocationService(client *HazelcastClient) *invocationServiceImpl {
	service := &invocationServiceImpl{
		client:                    client,
		sending:                   make(chan *invocation, 10000),
		responseWaitings:          make(map[int64]*invocation),
		eventHandlers:             make(map[int64]*invocation),
		responseChannel:           make(chan *protocol.ClientMessage, 1),
		quit:                      make(chan struct{}),
		isClosedChan:              make(chan struct{}),
		cleanupConnectionChannel:  make(chan *connectionAndError, 1),
		removeEventHandlerChannel: make(chan int64, 1),
		notSentMessages:           make(chan int64, 10000),
	}

	service.isShutdown.Store(false)
	if client.ClientConfig.ClientNetworkConfig().IsSmartRouting() {
		service.invoke = service.invokeSmart
	} else {
		service.invoke = service.invokeNonSmart
	}
	service.start()
	service.client.ConnectionManager.addListener(service)
	return service
}

func (is *invocationServiceImpl) start() {
	go is.process()
}
func (is *invocationServiceImpl) process() {
	for {
		select {
		case invocation := <-is.sending:
			is.registerInvocation(invocation)
			is.invoke(invocation)
		case response := <-is.responseChannel:
			is.handleResponse(response)
		case correlationID := <-is.notSentMessages:
			is.handleNotSentInvocation(correlationID)
		case connectionAndErr := <-is.cleanupConnectionChannel:
			is.cleanupConnectionInternal(connectionAndErr.connection, connectionAndErr.error)
		case correlationID := <-is.removeEventHandlerChannel:
			is.removeEventHandlerInternal(correlationID)
		case <-is.quit:
			is.quitInternal()
			return
		}
	}
}

func (is *invocationServiceImpl) quitInternal() {
	//signal channel closed to outside
	close(is.isClosedChan)
	//consume all invocations in sending
	for invocation := range is.sending {
		invocation.err <- core.NewHazelcastClientNotActiveError("client has been shutdown", nil)
	}
	for _, invocation := range is.responseWaitings {
		invocation.err <- core.NewHazelcastClientNotActiveError("client has been shutdown", nil)
	}
}

func (is *invocationServiceImpl) nextCorrelationId() int64 {
	is.nextCorrelation = is.nextCorrelation + 1
	return is.nextCorrelation
}

func (is *invocationServiceImpl) sendToRandomAddress(invocation *invocation) {
	var target = is.client.LoadBalancer.nextAddress()
	if target == nil {
		is.handleException(invocation, core.NewHazelcastIOError("No address found to invoke", nil))
		return
	}
	is.sendToAddress(invocation, target)
}

func (is *invocationServiceImpl) invokeSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		is.sendToConnection(invocation, invocation.boundConnection)
	} else if invocation.partitionID != -1 {
		if target, ok := is.client.PartitionService.partitionOwner(invocation.partitionID); ok {
			is.sendToAddress(invocation, target)
		} else {
			is.handleException(invocation,
				core.NewHazelcastIOError(fmt.Sprintf("partition does not have an owner. partitionID: %d", invocation.partitionID), nil))

		}
	} else if invocation.address != nil {
		is.sendToAddress(invocation, invocation.address)
	} else {
		is.sendToRandomAddress(invocation)
	}
}

func (is *invocationServiceImpl) invokeNonSmart(invocation *invocation) {
	if invocation.boundConnection != nil {
		is.sendToConnection(invocation, invocation.boundConnection)
	} else {
		address := is.client.ClusterService.ownerConnectionAddress.Load().(*protocol.Address)
		if address == nil {
			is.handleException(invocation, core.NewHazelcastIOError("No address found to invoke", nil))
			return
		}
		is.sendToAddress(invocation, address)
	}
}

func (is *invocationServiceImpl) sendToConnection(invocation *invocation, connection *Connection) {
	sent := connection.send(invocation.request)
	if !sent {
		is.handleNotSentInvocation(invocation.request.CorrelationID())
	} else {
		invocation.sentConnection = connection
	}

}

func (is *invocationServiceImpl) sendToAddress(invocation *invocation, address *protocol.Address) {
	connection, err := is.client.ConnectionManager.getOrTriggerConnect(address)
	if err != nil {
		log.Println("the following error occured while trying to send the invocation ", err)
		is.handleException(invocation, err)
		return
	}
	is.sendToConnection(invocation, connection)
}

func (is *invocationServiceImpl) registerInvocation(invocation *invocation) {
	message := invocation.request
	correlationID := is.nextCorrelationId()
	message.SetCorrelationID(correlationID)
	message.SetPartitionID(invocation.partitionID)
	message.SetFlags(bufutil.BeginEndFlag)
	if invocation.eventHandler != nil {
		is.eventHandlers[correlationID] = invocation
	}
	is.responseWaitings[correlationID] = invocation
}

func (is *invocationServiceImpl) unRegisterInvocation(correlationID int64) (*invocation, bool) {
	if invocation, ok := is.responseWaitings[correlationID]; ok {
		delete(is.responseWaitings, correlationID)
		return invocation, ok
	}
	if invocation, ok := is.eventHandlers[correlationID]; ok {
		return invocation, ok
	}
	return nil, false
}

func (is *invocationServiceImpl) handleNotSentInvocation(correlationID int64) {
	if invocation, ok := is.unRegisterInvocation(correlationID); ok {
		is.handleException(invocation, core.NewHazelcastIOError("packet is not sent", nil))
	} else {
		log.Println("No invocation has been found with the correlation ID: ", correlationID)
	}
}

func (is *invocationServiceImpl) removeEventHandlerInternal(correlationID int64) {
	if _, ok := is.eventHandlers[correlationID]; ok {
		delete(is.eventHandlers, correlationID)
	}
}

func (is *invocationServiceImpl) handleResponse(response *protocol.ClientMessage) {
	correlationID := response.CorrelationID()
	if invocation, ok := is.unRegisterInvocation(correlationID); ok {
		if response.HasFlags(bufutil.ListenerFlag) > 0 {
			invocation, found := is.eventHandlers[correlationID]
			if !found {
				log.Println("Got an event message with unknown correlation id.")
			} else {
				invocation.eventHandler(response)
			}
			return
		}
		if response.MessageType() == bufutil.MessageTypeException {
			err := createHazelcastError(convertToError(response))
			is.handleException(invocation, err)
		} else {
			invocation.response <- response
		}
	} else {
		log.Println("No invocation has been found with the correlationID: ", correlationID)
	}
}

func convertToError(clientMessage *protocol.ClientMessage) *protocol.Error {
	return protocol.ErrorCodecDecode(clientMessage)
}

func (is *invocationServiceImpl) cleanupConnectionInternal(connection *Connection, cause error) {
	for _, invocation := range is.responseWaitings {
		if invocation.sentConnection == connection {
			is.handleException(invocation, cause)
		}
	}
}

func (is *invocationServiceImpl) handleException(invocation *invocation, err error) {
	is.unRegisterInvocation(invocation.request.CorrelationID())
	if !is.client.LifecycleService.isLive.Load().(bool) {
		invocation.err <- core.NewHazelcastClientNotActiveError(err.Error(), err)
		return
	}
	if is.isNotAllowedToRetryOnConnection(invocation, err) {
		invocation.err <- err
		return
	}

	if invocation.isTimedout.Load().(bool) {
		timeSinceDeadline := time.Since(invocation.timedoutTime.Load().(time.Time))
		log.Println("Invocation will not be retried because it timed out by ", timeSinceDeadline.String())
		invocation.err <- core.NewHazelcastTimeoutError("invocation timed out by"+timeSinceDeadline.String(), nil)
		return
	}
	if is.shouldRetryInvocation(invocation, err) {
		time.AfterFunc(RetryWaitTime, func() {
			is.retryInvocation(invocation, err)
		})
		return
	}
	invocation.err <- err
}

func (is *invocationServiceImpl) isRedoOperation() bool {
	return is.client.ClientConfig.ClientNetworkConfig().IsRedoOperation()
}

func (is *invocationServiceImpl) shouldRetryInvocation(clientInvocation *invocation, err error) bool {
	_, isTargetDisconnectedError := err.(*core.HazelcastTargetDisconnectedError)
	if (isTargetDisconnectedError && clientInvocation.request.IsRetryable) || is.isRedoOperation() || isRetrySafeError(err) {
		return true
	}
	return false
}

func isRetrySafeError(err error) bool {
	var isRetrySafe = false
	_, ok := err.(*core.HazelcastInstanceNotActiveError)
	isRetrySafe = isRetrySafe || ok
	_, ok = err.(*core.HazelcastTargetNotMemberError)
	isRetrySafe = isRetrySafe || ok
	_, ok = err.(*core.HazelcastIOError)
	isRetrySafe = isRetrySafe || ok
	return isRetrySafe
}

func (is *invocationServiceImpl) isNotAllowedToRetryOnConnection(invocation *invocation, err error) bool {
	_, isIOError := err.(*core.HazelcastIOError)
	if invocation.isBoundToSingleConnection() && isIOError {
		return true
	}
	_, isTargetNotMemberError := err.(*core.HazelcastTargetNotMemberError)
	if invocation.address != nil && isTargetNotMemberError && is.client.ClusterService.GetMember(invocation.address) == nil {
		return true
	}
	return false
}
