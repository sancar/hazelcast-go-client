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

package protocol

import (
	"bytes"
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol/bufutil"
	"github.com/hazelcast/hazelcast-go-client/internal/serialization"
	"github.com/hazelcast/hazelcast-go-client/internal/timeutil"
)

type Address struct {
	host string
	port int32
}

func NewAddressWithParameters(Host string, Port int32) *Address {
	return &Address{Host, Port}
}

func (a *Address) Host() string {
	return a.host
}

func (a *Address) Port() int {
	return int(a.port)
}

func (a *Address) String() string {
	return a.Host() + ":" + strconv.Itoa(a.Port())
}

type uuid struct {
	msb int64
	lsb int64
}

type Member struct {
	address      Address
	uuid         string
	isLiteMember bool
	attributes   map[string]string
}

func NewMember(address Address, uuid string, isLiteMember bool, attributes map[string]string) *Member {
	return &Member{address: address, uuid: uuid, isLiteMember: isLiteMember, attributes: attributes}
}

func (m *Member) Address() core.Address {
	return &m.address
}

func (m *Member) UUID() string {
	return m.uuid
}

func (m *Member) IsLiteMember() bool {
	return m.isLiteMember
}

func (m *Member) Attributes() map[string]string {
	return m.attributes
}

func (m *Member) String() string {
	memberInfo := fmt.Sprintf("Member %s - %s", m.address.String(), m.UUID())
	if m.IsLiteMember() {
		memberInfo += " lite"
	}
	return memberInfo
}

type Pair struct {
	key, value interface{}
}

func NewPair(key interface{}, value interface{}) *Pair {
	return &Pair{key, value}
}

func (p *Pair) Key() interface{} {
	return p.key
}

func (p *Pair) Value() interface{} {
	return p.value
}

func (m *Member) Equal(member2 Member) bool {
	if m.address != member2.address {
		return false
	}
	if m.uuid != member2.uuid {
		return false
	}
	if m.isLiteMember != member2.isLiteMember {
		return false
	}
	if !reflect.DeepEqual(m.attributes, member2.attributes) {
		return false
	}
	return true
}

type DistributedObjectInfo struct {
	name        string
	serviceName string
}

func (i *DistributedObjectInfo) Name() string {
	return i.name
}

func (i *DistributedObjectInfo) ServiceName() string {
	return i.serviceName
}

type DataEntryView struct {
	keyData                *serialization.Data
	valueData              *serialization.Data
	cost                   int64
	creationTime           int64
	expirationTime         int64
	hits                   int64
	lastAccessTime         int64
	lastStoredTime         int64
	lastUpdateTime         int64
	version                int64
	evictionCriteriaNumber int64
	ttl                    int64
}

func (ev *DataEntryView) KeyData() *serialization.Data {
	return ev.keyData
}

func (ev *DataEntryView) ValueData() *serialization.Data {
	return ev.valueData
}

func (ev *DataEntryView) Cost() int64 {
	return ev.cost
}

func (ev *DataEntryView) CreationTime() int64 {
	return ev.creationTime
}

func (ev *DataEntryView) ExpirationTime() int64 {
	return ev.expirationTime
}

func (ev *DataEntryView) Hits() int64 {
	return ev.hits
}

func (ev *DataEntryView) LastAccessTime() int64 {
	return ev.lastAccessTime
}

func (ev *DataEntryView) LastStoredTime() int64 {
	return ev.lastStoredTime
}

func (ev *DataEntryView) LastUpdateTime() int64 {
	return ev.lastUpdateTime
}

func (ev *DataEntryView) Version() int64 {
	return ev.version
}

func (ev *DataEntryView) EvictionCriteriaNumber() int64 {
	return ev.evictionCriteriaNumber
}

func (ev *DataEntryView) TTL() int64 {
	return ev.ttl
}

type EntryView struct {
	key                    interface{}
	value                  interface{}
	cost                   int64
	creationTime           time.Time
	expirationTime         time.Time
	hits                   int64
	lastAccessTime         time.Time
	lastStoredTime         time.Time
	lastUpdateTime         time.Time
	version                int64
	evictionCriteriaNumber int64
	ttl                    time.Duration
}

func NewEntryView(key interface{}, value interface{}, cost int64, creationTime int64, expirationTime int64, hits int64,
	lastAccessTime int64, lastStoredTime int64, lastUpdateTime int64, version int64, evictionCriteriaNumber int64, ttl int64) *EntryView {
	return &EntryView{
		key:                    key,
		value:                  value,
		cost:                   cost,
		creationTime:           timeutil.ConvertMillisToUnixTime(creationTime),
		expirationTime:         timeutil.ConvertMillisToUnixTime(expirationTime),
		hits:                   hits,
		lastAccessTime:         timeutil.ConvertMillisToUnixTime(lastAccessTime),
		lastStoredTime:         timeutil.ConvertMillisToUnixTime(lastStoredTime),
		lastUpdateTime:         timeutil.ConvertMillisToUnixTime(lastUpdateTime),
		version:                version,
		evictionCriteriaNumber: evictionCriteriaNumber,
		ttl: timeutil.ConvertMillisToDuration(ttl),
	}
}

func (ev *EntryView) Key() interface{} {
	return ev.key
}

func (ev *EntryView) Value() interface{} {
	return ev.value
}

func (ev *EntryView) Cost() int64 {
	return ev.cost
}

func (ev *EntryView) CreationTime() time.Time {
	return ev.creationTime
}

func (ev *EntryView) ExpirationTime() time.Time {
	return ev.expirationTime
}

func (ev *EntryView) Hits() int64 {
	return ev.hits
}

func (ev *EntryView) LastAccessTime() time.Time {
	return ev.lastAccessTime
}

func (ev *EntryView) LastStoredTime() time.Time {
	return ev.lastStoredTime
}

func (ev *EntryView) LastUpdateTime() time.Time {
	return ev.lastUpdateTime
}

func (ev *EntryView) Version() int64 {
	return ev.version
}

func (ev *EntryView) EvictionCriteriaNumber() int64 {
	return ev.evictionCriteriaNumber
}

func (ev *EntryView) TTL() time.Duration {
	return ev.ttl
}

func (ev DataEntryView) Equal(ev2 DataEntryView) bool {
	if !bytes.Equal(ev.keyData.Buffer(), ev2.keyData.Buffer()) || !bytes.Equal(ev.valueData.Buffer(), ev2.valueData.Buffer()) {
		return false
	}
	if ev.cost != ev2.cost || ev.creationTime != ev2.creationTime || ev.expirationTime != ev2.expirationTime || ev.hits != ev2.hits {
		return false
	}
	if ev.lastAccessTime != ev2.lastAccessTime || ev.lastStoredTime != ev2.lastStoredTime || ev.lastUpdateTime != ev2.lastUpdateTime {
		return false
	}
	if ev.version != ev2.version || ev.evictionCriteriaNumber != ev2.evictionCriteriaNumber || ev.ttl != ev2.ttl {
		return false
	}
	return true
}

type Error struct {
	errorCode      int32
	className      string
	message        string
	stackTrace     []*StackTraceElement
	causeErrorCode int32
	causeClassName string
}

func (e *Error) Error() string {
	return e.message
}

func (e *Error) ErrorCode() int32 {
	return e.errorCode
}

func (e *Error) ClassName() string {
	return e.className
}

func (e *Error) Message() string {
	return e.message
}

func (e *Error) StackTrace() []core.StackTraceElement {
	stackTrace := make([]core.StackTraceElement, len(e.stackTrace))
	for i, v := range e.stackTrace {
		stackTrace[i] = core.StackTraceElement(v)
	}
	return stackTrace
}

func (e *Error) CauseErrorCode() int32 {
	return e.causeErrorCode
}

func (e *Error) CauseClassName() string {
	return e.causeClassName
}

type StackTraceElement struct {
	declaringClass string
	methodName     string
	fileName       string
	lineNumber     int32
}

func (e *StackTraceElement) DeclaringClass() string {
	return e.declaringClass
}

func (e *StackTraceElement) MethodName() string {
	return e.methodName
}

func (e *StackTraceElement) FileName() string {
	return e.fileName
}

func (e *StackTraceElement) LineNumber() int32 {
	return e.lineNumber
}

type EntryEvent struct {
	key          interface{}
	value        interface{}
	oldValue     interface{}
	mergingValue interface{}
	eventType    int32
	uuid         *string
}

func (e *EntryEvent) Key() interface{} {
	return e.key
}

func (e *EntryEvent) Value() interface{} {
	return e.value
}

func (e *EntryEvent) OldValue() interface{} {
	return e.oldValue
}

func (e *EntryEvent) MergingValue() interface{} {
	return e.mergingValue
}

func (e *EntryEvent) UUID() *string {
	return e.uuid
}

func (e *EntryEvent) EventType() int32 {
	return e.eventType
}

func NewEntryEvent(key interface{}, value interface{}, oldValue interface{}, mergingValue interface{}, eventType int32, uuid *string) *EntryEvent {
	return &EntryEvent{key: key, value: value, oldValue: oldValue, mergingValue: mergingValue, eventType: eventType, uuid: uuid}
}

type MapEvent struct {
	eventType               int32
	uuid                    *string
	numberOfAffectedEntries int32
}

func NewItemEvent(name *string, item interface{}, eventType int32, member *Member) *ItemEvent {
	return &ItemEvent{
		name:      *name,
		item:      item,
		eventType: eventType,
		member:    member,
	}
}

type ItemEvent struct {
	name      string
	item      interface{}
	eventType int32
	member    *Member
}

func (e *ItemEvent) Name() string {
	return e.name
}

func (e *ItemEvent) Item() interface{} {
	return e.item
}

func (e *ItemEvent) EventType() int32 {
	return e.eventType
}

func (e *ItemEvent) Member() core.Member {
	return e.member
}

func (e *MapEvent) UUID() *string {
	return e.uuid
}

func (e *MapEvent) NumberOfAffectedEntries() int32 {
	return e.numberOfAffectedEntries
}

func (e *MapEvent) EventType() int32 {
	return e.eventType
}

func NewMapEvent(eventType int32, uuid *string, numberOfAffectedEntries int32) *MapEvent {
	return &MapEvent{eventType: eventType, uuid: uuid, numberOfAffectedEntries: numberOfAffectedEntries}
}

type EntryAddedListener interface {
	EntryAdded(core.EntryEvent)
}

type EntryRemovedListener interface {
	EntryRemoved(core.EntryEvent)
}

type EntryUpdatedListener interface {
	EntryUpdated(core.EntryEvent)
}

type EntryEvictedListener interface {
	EntryEvicted(core.EntryEvent)
}

type EntryEvictAllListener interface {
	EntryEvictAll(core.MapEvent)
}

type EntryClearAllListener interface {
	EntryClearAll(core.MapEvent)
}

type EntryMergedListener interface {
	EntryMerged(core.EntryEvent)
}

type EntryExpiredListener interface {
	EntryExpired(core.EntryEvent)
}

type DecodeListenerResponse func(message *ClientMessage) *string
type EncodeListenerRemoveRequest func(registrationID *string) *ClientMessage
type MemberAddedListener interface {
	MemberAdded(member core.Member)
}

type MemberRemovedListener interface {
	MemberRemoved(member core.Member)
}

// Helper function to get flags for listeners
func GetEntryListenerFlags(listener interface{}) int32 {
	flags := int32(0)
	if _, ok := listener.(EntryAddedListener); ok {
		flags |= bufutil.EntryEventAdded
	}
	if _, ok := listener.(EntryRemovedListener); ok {
		flags |= bufutil.EntryEventRemoved
	}
	if _, ok := listener.(EntryUpdatedListener); ok {
		flags |= bufutil.EntryEventUpdated
	}
	if _, ok := listener.(EntryEvictedListener); ok {
		flags |= bufutil.EntryEventEvicted
	}
	if _, ok := listener.(EntryEvictAllListener); ok {
		flags |= bufutil.EntryEventEvictAll
	}
	if _, ok := listener.(EntryClearAllListener); ok {
		flags |= bufutil.EntryEventClearAll
	}
	if _, ok := listener.(EntryExpiredListener); ok {
		flags |= bufutil.EntryEventExpired
	}
	if _, ok := listener.(EntryMergedListener); ok {
		flags |= bufutil.EntryEventMerged
	}
	return flags
}

type TopicMessage struct {
	messageObject    interface{}
	publishTime      time.Time
	publishingMember *Member
}

func NewTopicMessage(messageObject interface{}, publishTime int64, publishingMember *Member) *TopicMessage {
	return &TopicMessage{
		messageObject:    messageObject,
		publishTime:      timeutil.ConvertMillisToUnixTime(publishTime),
		publishingMember: publishingMember,
	}
}

func (m *TopicMessage) MessageObject() interface{} {
	return m.messageObject
}

func (m *TopicMessage) PublishTime() time.Time {
	return m.publishTime
}

func (m *TopicMessage) PublishingMember() core.Member {
	return m.publishingMember
}
