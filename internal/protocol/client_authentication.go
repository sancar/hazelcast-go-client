// Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
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

package protocol

import (
	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
)

func ClientAuthenticationCalculateSize(username *string, password *string, uuid *string, ownerUUID *string, isOwnerConnection bool, clientType *string, serializationVersion uint8, clientHazelcastVersion *string) int {
	// Calculates the request payload size
	dataSize := 0
	dataSize += stringCalculateSize(username)
	dataSize += stringCalculateSize(password)
	dataSize += bufutil.BoolSizeInBytes
	if uuid != nil {
		dataSize += stringCalculateSize(uuid)
	}
	dataSize += bufutil.BoolSizeInBytes
	if ownerUUID != nil {
		dataSize += stringCalculateSize(ownerUUID)
	}
	dataSize += bufutil.BoolSizeInBytes
	dataSize += stringCalculateSize(clientType)
	dataSize += bufutil.Uint8SizeInBytes
	dataSize += stringCalculateSize(clientHazelcastVersion)
	return dataSize
}

func ClientAuthenticationEncodeRequest(username *string, password *string, uuid *string, ownerUUID *string, isOwnerConnection bool, clientType *string, serializationVersion uint8, clientHazelcastVersion *string) *ClientMessage {
	// Encode request into clientMessage
	clientMessage := NewClientMessage(nil, ClientAuthenticationCalculateSize(username, password, uuid, ownerUUID, isOwnerConnection, clientType, serializationVersion, clientHazelcastVersion))
	clientMessage.SetMessageType(clientAuthentication)
	clientMessage.IsRetryable = true
	clientMessage.AppendString(username)
	clientMessage.AppendString(password)
	clientMessage.AppendBool(uuid == nil)
	if uuid != nil {
		clientMessage.AppendString(uuid)
	}
	clientMessage.AppendBool(ownerUUID == nil)
	if ownerUUID != nil {
		clientMessage.AppendString(ownerUUID)
	}
	clientMessage.AppendBool(isOwnerConnection)
	clientMessage.AppendString(clientType)
	clientMessage.AppendUint8(serializationVersion)
	clientMessage.AppendString(clientHazelcastVersion)
	clientMessage.UpdateFrameLength()
	return clientMessage
}

func ClientAuthenticationDecodeResponse(clientMessage *ClientMessage) func() (status uint8, address *Address, uuid *string, ownerUUID *string, serializationVersion uint8, serverHazelcastVersion *string, clientUnregisteredMembers []core.Member) {
	// Decode response from client message
	return func() (status uint8, address *Address, uuid *string, ownerUUID *string, serializationVersion uint8, serverHazelcastVersion *string, clientUnregisteredMembers []core.Member) {
		status = clientMessage.ReadUint8()

		if !clientMessage.ReadBool() {
			address = AddressCodecDecode(clientMessage)
		}

		if !clientMessage.ReadBool() {
			uuid = clientMessage.ReadString()
		}

		if !clientMessage.ReadBool() {
			ownerUUID = clientMessage.ReadString()
		}
		serializationVersion = clientMessage.ReadUint8()
		if clientMessage.IsComplete() {
			return
		}
		serverHazelcastVersion = clientMessage.ReadString()

		if !clientMessage.ReadBool() {
			clientUnregisteredMembersSize := clientMessage.ReadInt32()
			clientUnregisteredMembers = make([]core.Member, clientUnregisteredMembersSize)
			for clientUnregisteredMembersIndex := 0; clientUnregisteredMembersIndex < int(clientUnregisteredMembersSize); clientUnregisteredMembersIndex++ {
				clientUnregisteredMembersItem := MemberCodecDecode(clientMessage)
				clientUnregisteredMembers[clientUnregisteredMembersIndex] = clientUnregisteredMembersItem
			}
		}
		return
	}
}
