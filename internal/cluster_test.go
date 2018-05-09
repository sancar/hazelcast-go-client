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
	"testing"

	"fmt"
	"reflect"

	"github.com/hazelcast/hazelcast-go-client/core"
	"github.com/hazelcast/hazelcast-go-client/internal/IPutil"
	"github.com/hazelcast/hazelcast-go-client/internal/protocol"
)

func Test_getPossibleAddresses(t *testing.T) {
	configAddresses := []string{
		"132.63.211.12:5012",
		"132.63.211.12:5011",
		"132.63.211.12:5010",
		"132.63.211.12:5010",
		"12.63.31.12:501",
	}
	members := []core.Member{
		protocol.NewMember(*protocol.NewAddressWithParameters("132.63.211.12", 5012), "", false, nil),
		protocol.NewMember(*protocol.NewAddressWithParameters("55.63.211.112", 5011), "", false, nil),
	}
	addresses := getPossibleAddresses(configAddresses, members)
	if len(addresses) != 5 {
		t.Fatal("getPossibleAddresses failed")
	}
	addressesInMap := make(map[protocol.Address]struct{}, len(addresses))
	for _, address := range addresses {
		addressesInMap[*address.(*protocol.Address)] = struct{}{}
	}
	for _, address := range configAddresses {
		ip, port := IPutil.GetIPAndPort(address)
		if _, found := addressesInMap[*protocol.NewAddressWithParameters(ip, port)]; !found {
			t.Fatal("getPossibleAddresses failed")
		}
	}
	for _, member := range members {
		if _, found := addressesInMap[*protocol.NewAddressWithParameters(member.Address().Host(),
			int32(member.Address().Port()))]; !found {
			t.Fatal("getPossibleAddresses failed")
		}
	}
}

func Test_getPossibleAddressesWithEmptyParameters(t *testing.T) {
	addresses := getPossibleAddresses(nil, nil)
	if len(addresses) != 1 {
		t.Fatal("getPossibleAddresses failed")
	}
	defaultAddress := protocol.NewAddressWithParameters(defaultAddress, defaultPort)
	for _, address := range addresses {
		if !reflect.DeepEqual(address, defaultAddress) {
			t.Fatal("getPossibleAddresses failed")
		}
	}

}

type A struct {
	a int
}

func (a *A) String() string {
	return fmt.Sprintf("%d", a.a)
}
func TestSancar(t *testing.T) {
	stringers := make([]fmt.Stringer, 10)

	for i := 0; i < 10; i++ {
		stringers[i] = &A{i * 10}
	}

	cp := make([]fmt.Stringer, 10)

	copy(cp, stringers)

	stringers[0] = &A{200}
	stringers[1].(*A).a = 100
	for _, v := range cp {
		fmt.Println(v)
	}
	fmt.Println()
	for _, v := range stringers {
		fmt.Println(v)
	}
}
