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
	"sync"
	"sync/atomic"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

const (
	authenticated = iota
	credentialsFailed
	serializationVersionMismatch
)

type connectionManager interface {
	//returns a snapshot of active connections
	getActiveConnections() map[string]*Connection

	//returns connection if available, null otherwise
	getActiveConnection(address core.Address) *Connection

	//returns number of active connections
	ConnectionCount() int32

	// returns associated connection if available, creates new connection otherwise
	getOrConnect(address core.Address, asOwner bool) (*Connection, error)

	// returns associated connection if available, returns error and triggers new connection creation otherwise
	getOrTriggerConnect(address core.Address) (*Connection, error)

	getOwnerConnection() *Connection

	addListener(listener connectionListener)
	onConnectionClose(connection *Connection, cause error)
	NextConnectionID() int64
	shutdown()
}

func (cm *connectionManagerImpl) addListener(listener connectionListener) {
	cm.listenerMutex.Lock()
	defer cm.listenerMutex.Unlock()
	if listener != nil {
		listeners := cm.connectionListeners.Load().([]connectionListener)
		size := len(listeners) + 1
		copyListeners := make([]connectionListener, size)
		copy(copyListeners, listeners)
		copyListeners[size-1] = listener
		cm.connectionListeners.Store(copyListeners)
	}
}

func (cm *connectionManagerImpl) getActiveConnection(address core.Address) *Connection {
	return cm.getConnection(address, false)
}

func (cm *connectionManagerImpl) ConnectionCount() int32 {
	cm.connectionsMutex.RLock()
	defer cm.connectionsMutex.RUnlock()
	return int32(len(cm.connections))
}

func (cm *connectionManagerImpl) getActiveConnections() map[string]*Connection {
	connections := make(map[string]*Connection)
	cm.connectionsMutex.RLock()
	defer cm.connectionsMutex.RUnlock()
	for k, v := range cm.connections {
		connections[k] = v
	}
	return connections
}

func (cm *connectionManagerImpl) onConnectionClose(connection *Connection, cause error) {
	//If Connection was authenticated fire event
	address, ok := connection.endpoint.Load().(core.Address)
	if ok {
		cm.connectionsMutex.Lock()
		delete(cm.connections, address.String())
		cm.connectionsMutex.Unlock()

		listeners := cm.connectionListeners.Load().([]connectionListener)
		for _, listener := range listeners {
			if _, ok := listener.(connectionListener); ok {
				listener.(connectionListener).onConnectionClosed(connection, cause)
			}
		}
	} else {
		//Clean up unauthenticated Connection
		cm.client.InvocationService.cleanupConnection(connection, cause)
	}
}

func (cm *connectionManagerImpl) getOrTriggerConnect(address core.Address) (*Connection, error) {
	connection := cm.getConnection(address, false)
	if connection != nil {
		return connection, nil
	}
	go cm.getOrCreateConnectionInternal(address, false)
	return nil, core.NewHazelcastIOError("No available connection to address "+address.String(), nil)
}

func (cm *connectionManagerImpl) getOrConnect(address core.Address, asOwner bool) (*Connection, error) {
	connection := cm.getConnection(address, asOwner)
	if connection != nil {
		return connection, nil
	}
	return cm.getOrCreateConnectionInternal(address, asOwner)
}

func (cm *connectionManagerImpl) getOwnerConnection() *Connection {
	ownerConnectionAddress := cm.client.ClusterService.getOwnerConnectionAddress()
	if ownerConnectionAddress == nil {
		return nil
	}
	return cm.getActiveConnection(ownerConnectionAddress)
}

func (cm *connectionManagerImpl) shutdown() {
	activeCons := cm.getActiveConnections()
	for _, con := range activeCons {
		con.close(core.NewHazelcastClientNotActiveError("client is shutting down", nil))
	}
}

//internal definitions and methods called inside connection manager process

type connectionManagerImpl struct {
	client              *HazelcastClient
	connectionsMutex    sync.RWMutex
	connections         map[string]*Connection
	nextConnectionID    int64
	listenerMutex       sync.Mutex
	connectionListeners atomic.Value
}

func newConnectionManager(client *HazelcastClient) connectionManager {
	cm := connectionManagerImpl{
		client:      client,
		connections: make(map[string]*Connection),
	}
	cm.connectionListeners.Store(make([]connectionListener, 0))
	return &cm
}

func (cm *connectionManagerImpl) getConnection(address core.Address, asOwner bool) *Connection {
	cm.connectionsMutex.RLock()
	conn, found := cm.connections[address.String()]
	cm.connectionsMutex.RUnlock()

	if !found {
		return nil
	}

	if !asOwner {
		return conn
	}

	if conn.isOwnerConnection {
		return conn
	}

	return nil
}

// following methods are called under same connectionsMutex writeLock
// only entry point is getOrCreateConnectionInternal
func (cm *connectionManagerImpl) getOrCreateConnectionInternal(address core.Address, asOwner bool) (*Connection, error) {
	cm.connectionsMutex.Lock()
	defer cm.connectionsMutex.Unlock()

	conn, found := cm.connections[address.String()]

	if !found {
		return cm.createConnection(address, asOwner)
	}

	if !asOwner {
		return conn, nil
	}

	if conn.isOwnerConnection {
		return conn, nil
	}
	err := cm.authenticate(conn, asOwner)

	if err == nil {
		return conn, nil
	}
	return nil, err
}

func (cm *connectionManagerImpl) NextConnectionID() int64 {
	cm.nextConnectionID = cm.nextConnectionID + 1
	return cm.nextConnectionID
}

func (cm *connectionManagerImpl) encodeRequest(asOwner bool) *protocol.ClientMessage {
	uuid := cm.client.ClusterService.uuid.Load().(*string)
	ownerUUID := cm.client.ClusterService.ownerUUID.Load().(*string)
	clientType := protocol.ClientType
	name := cm.client.ClientConfig.GroupConfig().Name()
	password := cm.client.ClientConfig.GroupConfig().Password()
	clientVersion := "ALPHA"
	//TODO This should be replace with a build time version variable, BuildInfo etc.
	request := protocol.ClientAuthenticationEncodeRequest(
		&name,
		&password,
		uuid,
		ownerUUID,
		asOwner,
		&clientType,
		1,
		&clientVersion,
	)
	return request
}

func (cm *connectionManagerImpl) authenticate(connection *Connection, asOwner bool) error {
	request := cm.encodeRequest(asOwner)
	invocationResult := cm.client.InvocationService.invokeOnConnection(request, connection)
	result, err := invocationResult.ResultWithTimeout(cm.client.HeartBeatService.heartBeatTimeout)
	if err != nil {
		return err
	}
	//status, address, uuid, ownerUUID, serializationVersion, serverHazelcastVersion , clientUnregisteredMembers
	status, address, uuid, ownerUUID, _, serverHazelcastVersion, _ := protocol.ClientAuthenticationDecodeResponse(result)()
	switch status {
	case authenticated:
		connection.serverHazelcastVersion = serverHazelcastVersion
		connection.endpoint.Store(address)
		connection.isOwnerConnection = asOwner
		cm.connections[address.String()] = connection
		cm.fireConnectionAddedEvent(connection)
		if asOwner {
			cm.client.ClusterService.ownerConnectionAddress.Store(address)
			cm.client.ClusterService.ownerUUID.Store(ownerUUID)
			cm.client.ClusterService.uuid.Store(uuid)
		}
	case credentialsFailed:
		return core.NewHazelcastAuthenticationError("invalid credentials!", nil)
	case serializationVersionMismatch:
		return core.NewHazelcastAuthenticationError("serialization version mismatches with the server!", nil)
	}

	return nil
}

func (cm *connectionManagerImpl) fireConnectionAddedEvent(connection *Connection) {
	listeners := cm.connectionListeners.Load().([]connectionListener)
	for _, listener := range listeners {
		if _, ok := listener.(connectionListener); ok {
			listener.(connectionListener).onConnectionOpened(connection)
		}
	}
}

func (cm *connectionManagerImpl) createConnection(address core.Address, asOwner bool) (*Connection, error) {
	if !asOwner && cm.client.ClusterService.getOwnerConnectionAddress() == nil {
		return nil, core.NewHazelcastIllegalStateError("ownerConnection is not active", nil)
	}
	invocationService := cm.client.InvocationService.(*invocationServiceImpl)
	connectionID := cm.NextConnectionID()
	con := newConnection(address, invocationService.handleResponse, connectionID, cm)
	if con == nil {
		return nil, core.NewHazelcastTargetDisconnectedError("target is disconnected", nil)
	}
	err := cm.authenticate(con, asOwner)

	if err != nil {
		return nil, err
	}
	return con, nil
}

type connectionListener interface {
	onConnectionClosed(connection *Connection, cause error)
	onConnectionOpened(connection *Connection)
}
