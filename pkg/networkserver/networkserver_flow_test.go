// Copyright © 2019 The Things Network Foundation, The Things Industries B.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package networkserver_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	pbtypes "github.com/gogo/protobuf/types"
	"github.com/smartystreets/assertions"
	clusterauth "go.thethings.network/lorawan-stack/pkg/auth/cluster"
	"go.thethings.network/lorawan-stack/pkg/cluster"
	"go.thethings.network/lorawan-stack/pkg/component"
	"go.thethings.network/lorawan-stack/pkg/config"
	"go.thethings.network/lorawan-stack/pkg/crypto"
	"go.thethings.network/lorawan-stack/pkg/frequencyplans"
	. "go.thethings.network/lorawan-stack/pkg/networkserver"
	"go.thethings.network/lorawan-stack/pkg/networkserver/redis"
	"go.thethings.network/lorawan-stack/pkg/rpcmetadata"
	"go.thethings.network/lorawan-stack/pkg/rpcserver"
	"go.thethings.network/lorawan-stack/pkg/ttnpb"
	"go.thethings.network/lorawan-stack/pkg/types"
	"go.thethings.network/lorawan-stack/pkg/util/test"
	"go.thethings.network/lorawan-stack/pkg/util/test/assertions/should"
	"google.golang.org/grpc"
)

func handleOTAAClassA868FlowTest(t *testing.T, reg DeviceRegistry, tq DownlinkTaskQueue) {
	a := assertions.New(t)

	netID := test.Must(types.NewNetID(2, []byte{1, 2, 3})).(types.NetID)

	setDeviceKey := "set-device-key"
	linkApplicationKey := "link-application-key"

	appSKey := types.AES128Key{0x42, 0x42, 0x42, 0x42, 0x42, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	fNwkSIntKey := types.AES128Key{0x42, 0x42, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	nwkSEncKey := types.AES128Key{0x42, 0x42, 0x42, 0x42, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	sNwkSIntKey := types.AES128Key{0x42, 0x42, 0x42, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}

	isPeer := test.Must(test.NewGRPCServerPeer(test.Context(), test.MockApplicationAccessServer{
		ListRightsFunc: func(ctx context.Context, ids *ttnpb.ApplicationIdentifiers) (*ttnpb.Rights, error) {
			a.So(ids, should.Resemble, &ttnpb.ApplicationIdentifiers{
				ApplicationID: "test-app-id",
			})
			md := rpcmetadata.FromIncomingContext(ctx)
			a.So(md.AuthType, should.Equal, "Bearer")
			switch md.AuthValue {
			case setDeviceKey:
				return &ttnpb.Rights{
					Rights: []ttnpb.Right{
						ttnpb.RIGHT_APPLICATION_DEVICES_WRITE,
					},
				}, nil
			case linkApplicationKey:
				return &ttnpb.Rights{
					Rights: []ttnpb.Right{
						ttnpb.RIGHT_APPLICATION_LINK,
					},
				}, nil
			default:
				t.Errorf("Invalid AuthValue: %s", md.AuthValue)
				return &ttnpb.Rights{}, nil
			}
		},
	}, ttnpb.RegisterApplicationAccessServer)).(cluster.Peer)

	type scheduleDownlinkResp struct {
		Response *ttnpb.ScheduleDownlinkResponse
		Error    error
	}
	type scheduleDownlinkReq struct {
		Context  context.Context
		Request  *ttnpb.DownlinkMessage
		Response chan<- scheduleDownlinkResp
	}
	scheduleDownlinkCh := make(chan scheduleDownlinkReq)
	gsPeer := test.Must(test.NewGRPCServerPeer(test.Context(), &MockNsGsServer{
		ScheduleDownlinkFunc: func(ctx context.Context, msg *ttnpb.DownlinkMessage) (*ttnpb.ScheduleDownlinkResponse, error) {
			respCh := make(chan scheduleDownlinkResp)
			scheduleDownlinkCh <- scheduleDownlinkReq{
				Context:  ctx,
				Request:  msg,
				Response: respCh,
			}
			resp := <-respCh
			return resp.Response, resp.Error
		},
	}, ttnpb.RegisterNsGsServer)).(cluster.Peer)

	type handleJoinResp struct {
		Response *ttnpb.JoinResponse
		Error    error
	}
	type handleJoinReq struct {
		Context  context.Context
		Request  *ttnpb.JoinRequest
		Response chan<- handleJoinResp
	}
	handleJoinCh := make(chan handleJoinReq)
	jsPeer := test.Must(test.NewGRPCServerPeer(test.Context(), &MockNsJsServer{
		HandleJoinFunc: func(ctx context.Context, req *ttnpb.JoinRequest) (*ttnpb.JoinResponse, error) {
			respCh := make(chan handleJoinResp)
			handleJoinCh <- handleJoinReq{
				Context:  ctx,
				Request:  req,
				Response: respCh,
			}
			resp := <-respCh
			return resp.Response, resp.Error
		},
	}, ttnpb.RegisterNsJsServer)).(cluster.Peer)

	collectionDoneCh := make(chan windowEnd)
	deduplicationDoneCh := make(chan windowEnd)

	ns := test.Must(New(
		component.MustNew(
			test.GetLogger(t),
			&component.Config{
				ServiceBase: config.ServiceBase{
					GRPC: config.GRPC{
						AllowInsecureForCredentials: true,
					},
				},
			},
			component.WithClusterNew(func(context.Context, *config.ServiceBase, ...rpcserver.Registerer) (cluster.Cluster, error) {
				return &test.MockCluster{
					GetPeerFunc: func(ctx context.Context, role ttnpb.PeerInfo_Role, ids ttnpb.Identifiers) cluster.Peer {
						switch role {
						case ttnpb.PeerInfo_ACCESS:
							return isPeer

						case ttnpb.PeerInfo_GATEWAY_SERVER:
							return gsPeer

						case ttnpb.PeerInfo_JOIN_SERVER:
							return jsPeer

						default:
							t.Errorf("Invalid peer role requested: %s", role)
							return nil
						}
					},
					WithVerifiedSourceFunc: func(ctx context.Context) context.Context {
						return clusterauth.NewContext(ctx, nil)
					},
				}, nil
			}),
		),
		&Config{
			NetID:         netID,
			Devices:       reg,
			DownlinkTasks: tq,
			DownlinkPriorities: DownlinkPriorityConfig{
				JoinAccept:             "highest",
				MACCommands:            "highest",
				MaxApplicationDownlink: "high",
			},
			DefaultMACSettings: MACSettingConfig{
				DesiredRx1Delay: func(v ttnpb.RxDelay) *ttnpb.RxDelay { return &v }(ttnpb.RX_DELAY_6),
			},
		},
		WithDeduplicationDoneFunc(func(ctx context.Context, msg *ttnpb.UplinkMessage) <-chan time.Time {
			ch := make(chan time.Time)
			deduplicationDoneCh <- windowEnd{ctx, msg, ch}
			return ch
		}),
		WithCollectionDoneFunc(func(ctx context.Context, msg *ttnpb.UplinkMessage) <-chan time.Time {
			ch := make(chan time.Time)
			collectionDoneCh <- windowEnd{ctx, msg, ch}
			return ch
		}),
	)).(*NetworkServer)
	ns.FrequencyPlans = frequencyplans.NewStore(test.FrequencyPlansFetcher)
	test.Must(nil, ns.Start())
	defer ns.Close()

	conn := ns.LoopbackConn()
	nsReg := ttnpb.NewNsEndDeviceRegistryClient(conn)
	asns := ttnpb.NewAsNsClient(conn)
	gsns := ttnpb.NewGsNsClient(conn)

	start := time.Now()
	ctx := test.Context()

	link, err := asns.LinkApplication((rpcmetadata.MD{
		ID: "test-app-id",
	}).ToOutgoingContext(ctx),
		grpc.PerRPCCredentials(rpcmetadata.MD{
			AuthType:      "Bearer",
			AuthValue:     linkApplicationKey,
			AllowInsecure: true,
		}),
	)
	if !a.So(err, should.BeNil) || !a.So(link, should.NotBeNil) {
		t.Fatal("Failed to link application")
	}

	dev, err := nsReg.Set(
		ctx,
		&ttnpb.SetEndDeviceRequest{
			EndDevice: ttnpb.EndDevice{
				EndDeviceIdentifiers: ttnpb.EndDeviceIdentifiers{
					DeviceID:               "test-dev-id",
					ApplicationIdentifiers: ttnpb.ApplicationIdentifiers{ApplicationID: "test-app-id"},
					JoinEUI:                &types.EUI64{0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
					DevEUI:                 &types.EUI64{0x42, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				},
				FrequencyPlanID:   test.EUFrequencyPlanID,
				LoRaWANPHYVersion: ttnpb.PHY_V1_0,
				LoRaWANVersion:    ttnpb.MAC_V1_0,
				SupportsJoin:      true,
			},
			FieldMask: pbtypes.FieldMask{
				Paths: []string{
					"frequency_plan_id",
					"lorawan_phy_version",
					"lorawan_version",
					"supports_join",
				},
			},
		},
		grpc.PerRPCCredentials(rpcmetadata.MD{
			AuthType:      "Bearer",
			AuthValue:     setDeviceKey,
			AllowInsecure: true,
		}),
	)
	if !a.So(err, should.BeNil) || !a.So(dev, should.NotBeNil) {
		t.Fatal("Failed to create device")
	}
	a.So(dev.CreatedAt, should.HappenAfter, start)
	a.So(dev.UpdatedAt, should.Equal, dev.CreatedAt)
	a.So([]time.Time{start, dev.CreatedAt, time.Now()}, should.BeChronological)
	a.So(dev, should.Resemble, &ttnpb.EndDevice{
		EndDeviceIdentifiers: ttnpb.EndDeviceIdentifiers{
			DeviceID:               "test-dev-id",
			ApplicationIdentifiers: ttnpb.ApplicationIdentifiers{ApplicationID: "test-app-id"},
			JoinEUI:                &types.EUI64{0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			DevEUI:                 &types.EUI64{0x42, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		},
		FrequencyPlanID:   test.EUFrequencyPlanID,
		LoRaWANPHYVersion: ttnpb.PHY_V1_0,
		LoRaWANVersion:    ttnpb.MAC_V1_0,
		SupportsJoin:      true,
		CreatedAt:         dev.CreatedAt,
		UpdatedAt:         dev.UpdatedAt,
	})

	var devAddr types.DevAddr
	t.Run("join-request", func(t *testing.T) {
		a := assertions.New(t)

		uplink := &ttnpb.UplinkMessage{
			RawPayload: []byte{
				/* MHDR */
				0x00,
				/* Join-request */
				/** JoinEUI **/
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42,
				/** DevEUI **/
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x42, 0x42,
				/** DevNonce **/
				0x01, 0x00,
				/* MIC */
				0x03, 0x02, 0x01, 0x00,
			},
			Settings: ttnpb.TxSettings{
				DataRate: ttnpb.DataRate{
					Modulation: &ttnpb.DataRate_LoRa{LoRa: &ttnpb.LoRaDataRate{
						Bandwidth:       125000,
						SpreadingFactor: 12,
					}},
				},
				Frequency: 868100000,
				EnableCRC: true,
				Timestamp: 42,
			},
			RxMetadata: []*ttnpb.RxMetadata{
				{
					GatewayIdentifiers: ttnpb.GatewayIdentifiers{
						GatewayID: "test-gtw",
					},
					UplinkToken: []byte("join-request-token"),
				},
			},
			ReceivedAt:          time.Now(),
			CorrelationIDs:      []string{"GsNs-1", "GsNs-2"},
			GatewayChannelIndex: 2,
		}
		handleUplinkErrCh := make(chan error)
		go func() {
			_, err := gsns.HandleUplink(ctx, uplink)
			handleUplinkErrCh <- err
			close(handleUplinkErrCh)
		}()

		select {
		case req := <-handleJoinCh:
			if !a.So(req.Request, should.NotBeNil) {
				t.Fatal("Nil join-request sent to JS")
			}

			a.So(req.Request.CorrelationIDs, should.Contain, "GsNs-1")
			a.So(req.Request.CorrelationIDs, should.Contain, "GsNs-2")
			a.So(req.Request.CorrelationIDs, should.HaveLength, 4)
			a.So(req.Request.DevAddr, should.NotBeEmpty)
			a.So(req.Request.DevAddr.NwkID(), should.Resemble, netID.ID())
			a.So(req.Request.DevAddr.NetIDType(), should.Equal, netID.Type())
			a.So(req.Request, should.Resemble, &ttnpb.JoinRequest{
				RawPayload:         uplink.RawPayload,
				DevAddr:            req.Request.DevAddr,
				SelectedMACVersion: ttnpb.MAC_V1_0,
				NetID:              netID,
				RxDelay:            ttnpb.RX_DELAY_6,
				CFList: &ttnpb.CFList{
					Type: ttnpb.CFListType_FREQUENCIES,
					Freq: []uint32{8671000, 8673000, 8675000, 8677000, 8679000},
				},
				CorrelationIDs: req.Request.CorrelationIDs,
			})
			req.Response <- handleJoinResp{
				Response: &ttnpb.JoinResponse{
					RawPayload: bytes.Repeat([]byte{0x42}, 33),
					SessionKeys: ttnpb.SessionKeys{
						SessionKeyID: []byte("session-key-id"),
						AppSKey: &ttnpb.KeyEnvelope{
							Key: &appSKey,
						},
						FNwkSIntKey: &ttnpb.KeyEnvelope{
							Key: &fNwkSIntKey,
						},
						NwkSEncKey: &ttnpb.KeyEnvelope{
							Key: &nwkSEncKey,
						},
						SNwkSIntKey: &ttnpb.KeyEnvelope{
							Key: &sNwkSIntKey,
						},
					},
					CorrelationIDs: []string{"NsJs-1", "NsJs-2"},
				},
			}
			devAddr = req.Request.DevAddr

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for join-request to be sent to JS")
		}

		select {
		case we := <-deduplicationDoneCh:
			we.ch <- time.Now()
			close(we.ch)

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for deduplication window to close")
		}

		var asUp *ttnpb.ApplicationUp
		if !a.So(test.WaitTimeout(Timeout, func() {
			asUp, err = link.Recv()
		}), should.BeTrue) {
			t.Fatal("Timed out while waiting for join-accept to be sent to AS")
		}
		if !a.So(err, should.BeNil) {
			t.Fatalf("Failed to receive AS uplink: %s", err)
		}
		a.So(asUp.CorrelationIDs, should.Contain, "GsNs-1")
		a.So(asUp.CorrelationIDs, should.Contain, "GsNs-2")
		a.So(asUp.CorrelationIDs, should.Contain, "NsJs-1")
		a.So(asUp.CorrelationIDs, should.Contain, "NsJs-2")
		a.So(asUp.CorrelationIDs, should.HaveLength, 6)
		if !a.So(asUp.ReceivedAt, should.NotBeNil) {
			a.So([]time.Time{start, *asUp.ReceivedAt, time.Now()}, should.BeChronological)
		}
		a.So(asUp, should.Resemble, &ttnpb.ApplicationUp{
			EndDeviceIdentifiers: ttnpb.EndDeviceIdentifiers{
				DeviceID:               "test-dev-id",
				ApplicationIdentifiers: ttnpb.ApplicationIdentifiers{ApplicationID: "test-app-id"},
				JoinEUI:                &types.EUI64{0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				DevEUI:                 &types.EUI64{0x42, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				DevAddr:                &devAddr,
			},
			CorrelationIDs: asUp.CorrelationIDs,
			ReceivedAt:     asUp.ReceivedAt,
			Up: &ttnpb.ApplicationUp_JoinAccept{JoinAccept: &ttnpb.ApplicationJoinAccept{
				AppSKey: &ttnpb.KeyEnvelope{
					Key: &appSKey,
				},
				SessionKeyID: []byte("session-key-id"),
			}},
		})

		if !a.So(test.WaitTimeout(Timeout, func() {
			err = link.Send(ttnpb.Empty)
		}), should.BeTrue) {
			t.Fatal("Timed out while waiting for NS to process AS response")
		}
		if !a.So(err, should.BeNil) {
			t.Fatalf("Failed to send AS uplink response: %s", err)
		}

		select {
		case we := <-collectionDoneCh:
			we.ch <- time.Now()
			close(we.ch)

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for collection window to close")
		}

		select {
		case err := <-handleUplinkErrCh:
			if !a.So(err, should.BeNil) {
				t.Fatalf("Failed to handle uplink: %s", err)
			}

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for HandleUplink to return")
		}

		select {
		case req := <-scheduleDownlinkCh:
			a.So(req.Request.CorrelationIDs, should.Contain, "GsNs-1")
			a.So(req.Request.CorrelationIDs, should.Contain, "GsNs-2")
			a.So(req.Request.CorrelationIDs, should.HaveLength, 5)
			a.So(req.Request, should.Resemble, &ttnpb.DownlinkMessage{
				RawPayload: bytes.Repeat([]byte{0x42}, 33),
				Settings: &ttnpb.DownlinkMessage_Request{
					Request: &ttnpb.TxRequest{
						Class: ttnpb.CLASS_A,
						DownlinkPaths: []*ttnpb.DownlinkPath{
							{
								Path: &ttnpb.DownlinkPath_UplinkToken{
									UplinkToken: []byte("join-request-token"),
								},
							},
						},
						Rx1Delay:         ttnpb.RX_DELAY_5,
						Rx1DataRateIndex: ttnpb.DATA_RATE_0,
						Rx1Frequency:     868100000,
						Rx2DataRateIndex: ttnpb.DATA_RATE_0,
						Rx2Frequency:     869525000,
						Priority:         ttnpb.TxSchedulePriority_HIGHEST,
					},
				},
				CorrelationIDs: req.Request.CorrelationIDs,
			})
			req.Response <- scheduleDownlinkResp{
				Response: &ttnpb.ScheduleDownlinkResponse{},
			}

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for join-accept to be scheduled")
		}
	})

	t.Logf("Device successfully joined. DevAddr: %s", devAddr)

	t.Run("uplink", func(t *testing.T) {
		a := assertions.New(t)

		uplinkFRMPayload := test.Must(crypto.EncryptUplink(fNwkSIntKey, devAddr, 0, []byte("test"))).([]byte)
		uplink := &ttnpb.UplinkMessage{
			RawPayload: func() []byte {
				b := append([]byte{
					/* MHDR */
					0x40,
					/* MACPayload */
					/** FHDR **/
					/*** DevAddr ***/
					devAddr[3], devAddr[2], devAddr[1], devAddr[0],
					/*** FCtrl ***/
					0x80,
					/*** FCnt ***/
					0x00, 0x00,
					/** FPort **/
					0x42,
				},
					uplinkFRMPayload...,
				)
				mic := test.Must(crypto.ComputeLegacyUplinkMIC(fNwkSIntKey, devAddr, 0, b)).([4]byte)
				return append(b, mic[:]...)
			}(),
			Settings: ttnpb.TxSettings{
				DataRate: ttnpb.DataRate{
					Modulation: &ttnpb.DataRate_LoRa{LoRa: &ttnpb.LoRaDataRate{
						Bandwidth:       125000,
						SpreadingFactor: 11,
					}},
				},
				EnableCRC: true,
				Frequency: 867100000,
				Timestamp: 42,
			},
			RxMetadata: []*ttnpb.RxMetadata{
				{
					GatewayIdentifiers: ttnpb.GatewayIdentifiers{
						GatewayID: "test-gtw",
					},
					UplinkToken: []byte("test-uplink-token"),
				},
			},
			ReceivedAt:          time.Now(),
			CorrelationIDs:      []string{"GsNs-1", "GsNs-2"},
			GatewayChannelIndex: 2,
		}

		handleUplinkErrCh := make(chan error)
		go func() {
			_, err := gsns.HandleUplink(ctx, uplink)
			handleUplinkErrCh <- err
			close(handleUplinkErrCh)
		}()

		select {
		case we := <-deduplicationDoneCh:
			we.ch <- time.Now()
			close(we.ch)

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for deduplication window to close")
		}

		var asUp *ttnpb.ApplicationUp
		if !a.So(test.WaitTimeout(Timeout, func() {
			asUp, err = link.Recv()
		}), should.BeTrue) {
			t.Fatal("Timed out while waiting for uplink to be sent to AS")
		}
		if !a.So(err, should.BeNil) {
			t.Fatalf("Failed to receive AS uplink: %s", err)
		}
		a.So(asUp.CorrelationIDs, should.Contain, "GsNs-1")
		a.So(asUp.CorrelationIDs, should.Contain, "GsNs-2")
		a.So(asUp.CorrelationIDs, should.HaveLength, 4)
		if !a.So(asUp.ReceivedAt, should.NotBeNil) {
			a.So([]time.Time{start, *asUp.ReceivedAt, time.Now()}, should.BeChronological)
		}
		a.So(asUp, should.Resemble, &ttnpb.ApplicationUp{
			EndDeviceIdentifiers: ttnpb.EndDeviceIdentifiers{
				DeviceID:               "test-dev-id",
				ApplicationIdentifiers: ttnpb.ApplicationIdentifiers{ApplicationID: "test-app-id"},
				JoinEUI:                &types.EUI64{0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				DevEUI:                 &types.EUI64{0x42, 0x42, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
				DevAddr:                &devAddr,
			},
			CorrelationIDs: asUp.CorrelationIDs,
			ReceivedAt:     asUp.ReceivedAt,
			Up: &ttnpb.ApplicationUp_UplinkMessage{UplinkMessage: &ttnpb.ApplicationUplink{
				SessionKeyID: []byte("session-key-id"),
				FPort:        0x42,
				FCnt:         0,
				FRMPayload:   uplinkFRMPayload,
				RxMetadata:   uplink.RxMetadata,
				Settings: ttnpb.TxSettings{
					DataRate: ttnpb.DataRate{
						Modulation: &ttnpb.DataRate_LoRa{LoRa: &ttnpb.LoRaDataRate{
							Bandwidth:       125000,
							SpreadingFactor: 11,
						}},
					},
					DataRateIndex: ttnpb.DATA_RATE_1,
					EnableCRC:     true,
					Frequency:     867100000,
					Timestamp:     42,
				},
			}},
		})

		if !a.So(test.WaitTimeout(Timeout, func() {
			err = link.Send(ttnpb.Empty)
		}), should.BeTrue) {
			t.Fatal("Timed out while waiting for NS to process AS response")
		}
		if !a.So(err, should.BeNil) {
			t.Fatalf("Failed to send AS uplink response: %s", err)
		}

		select {
		case we := <-collectionDoneCh:
			we.ch <- time.Now()
			close(we.ch)

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for collection window to close")
		}

		select {
		case err := <-handleUplinkErrCh:
			if !a.So(err, should.BeNil) {
				t.Fatalf("Failed to handle uplink: %s", err)
			}

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for HandleUplink to return")
		}

		select {
		case req := <-scheduleDownlinkCh:
			a.So(req.Request.CorrelationIDs, should.Contain, "GsNs-1")
			a.So(req.Request.CorrelationIDs, should.Contain, "GsNs-2")
			a.So(req.Request.CorrelationIDs, should.HaveLength, 5)
			a.So(req.Request, should.Resemble, &ttnpb.DownlinkMessage{
				RawPayload: func() []byte {
					b := append([]byte{
						/* MHDR */
						0x60,
						/* MACPayload */
						/** FHDR **/
						/*** DevAddr ***/
						devAddr[3], devAddr[2], devAddr[1], devAddr[0],
						/*** FCtrl ***/
						0x80,
						/*** FCnt ***/
						0x01, 0x00,
						/** FPort **/
						0x0,
					},
						test.Must(crypto.EncryptDownlink(fNwkSIntKey, devAddr, 1, []byte{
							/* DevStatusReq */
							0x06,
						})).([]byte)...,
					)
					mic := test.Must(crypto.ComputeLegacyDownlinkMIC(fNwkSIntKey, devAddr, 1, b)).([4]byte)
					return append(b, mic[:]...)
				}(),
				Settings: &ttnpb.DownlinkMessage_Request{
					Request: &ttnpb.TxRequest{
						Class: ttnpb.CLASS_A,
						DownlinkPaths: []*ttnpb.DownlinkPath{
							{
								Path: &ttnpb.DownlinkPath_UplinkToken{
									UplinkToken: []byte("test-uplink-token"),
								},
							},
						},
						Rx1Delay:         ttnpb.RX_DELAY_6,
						Rx1DataRateIndex: ttnpb.DATA_RATE_1,
						Rx1Frequency:     867100000,
						Rx2DataRateIndex: ttnpb.DATA_RATE_0,
						Rx2Frequency:     869525000,
						Priority:         ttnpb.TxSchedulePriority_HIGHEST,
					},
				},
				CorrelationIDs: req.Request.CorrelationIDs,
			})
			req.Response <- scheduleDownlinkResp{
				Response: &ttnpb.ScheduleDownlinkResponse{},
			}

		case <-time.After(Timeout):
			t.Fatal("Timed out while waiting for data uplink to be scheduled")
		}
	})
}

func TestFlow(t *testing.T) {
	t.Parallel()

	namespace := [...]string{
		"networkserver_test",
	}

	for _, tc := range []struct {
		Name                 string
		NewRegistry          func(t testing.TB) (reg DeviceRegistry, closeFn func() error)
		NewDownlinkTaskQueue func(t testing.TB) (tq DownlinkTaskQueue, closeFn func() error)
	}{
		{
			Name: "Redis registry/Redis downlink task queue",
			NewRegistry: func(t testing.TB) (DeviceRegistry, func() error) {
				cl, flush := test.NewRedis(t, append(namespace[:], "devices")...)
				reg := &redis.DeviceRegistry{Redis: cl}
				return reg, func() error {
					flush()
					return cl.Close()
				}
			},
			NewDownlinkTaskQueue: func(t testing.TB) (DownlinkTaskQueue, func() error) {
				cl, flush := test.NewRedis(t, append(namespace[:], "tasks")...)
				tq := redis.NewDownlinkTaskQueue(cl, 100000, "ns", "test")
				ctx, cancel := context.WithCancel(test.Context())
				errch := make(chan error)
				go func() {
					errch <- tq.Run(ctx)
				}()
				return tq, func() error {
					cancel()
					if err := tq.Add(ctx, ttnpb.EndDeviceIdentifiers{
						DeviceID:               "test",
						ApplicationIdentifiers: ttnpb.ApplicationIdentifiers{ApplicationID: "test"},
					}, time.Now(), false); err != nil {
						t.Fatalf("Failed to add mock device to task queue: %s", err)
					}
					runErr := <-errch
					flush()
					closeErr := cl.Close()
					if runErr != nil && runErr != context.Canceled {
						return runErr
					}
					return closeErr
				}
			},
		},
	} {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			for flow, handleFlowTest := range map[string]func(*testing.T, DeviceRegistry, DownlinkTaskQueue){
				"Class A/OTAA/EU868": handleOTAAClassA868FlowTest,
			} {
				t.Run(flow, func(t *testing.T) {
					t.Parallel()

					reg, regClose := tc.NewRegistry(t)
					if regClose != nil {
						defer func() {
							if err := regClose(); err != nil {
								t.Errorf("Failed to close device registry: %s", err)
							}
						}()
					}

					tq, tqClose := tc.NewDownlinkTaskQueue(t)
					if tqClose != nil {
						defer func() {
							if err := tqClose(); err != nil {
								t.Errorf("Failed to close downlink task queue: %s", err)
							}
						}()
					}
					handleFlowTest(t, reg, tq)
				})
			}
		})
	}
}
