// Copyright © 2018 The Things Network Foundation, The Things Industries B.V.
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

package mqtt

import (
	"fmt"
	"math"
	"strconv"
	"time"

	legacyttnpb "go.thethings.network/lorawan-stack-legacy/pkg/ttnpb"
	"go.thethings.network/lorawan-stack/pkg/band"
	"go.thethings.network/lorawan-stack/pkg/errors"
	"go.thethings.network/lorawan-stack/pkg/gatewayserver/io/mqtt/topics"
	"go.thethings.network/lorawan-stack/pkg/ttnpb"
	"go.thethings.network/lorawan-stack/pkg/types"
)

const eirpDelta = 2 // Integer-rounded delta between EIRP and ERP

type V2 struct{}

var (
	modulationToV3 = map[legacyttnpb.Modulation]ttnpb.Modulation{
		legacyttnpb.Modulation_LORA: ttnpb.Modulation_LORA,
		legacyttnpb.Modulation_FSK:  ttnpb.Modulation_FSK,
	}
	modulationToV2 = map[ttnpb.Modulation]legacyttnpb.Modulation{
		ttnpb.Modulation_LORA: legacyttnpb.Modulation_LORA,
		ttnpb.Modulation_FSK:  legacyttnpb.Modulation_FSK,
	}
	sourceToV3 = map[legacyttnpb.LocationMetadata_LocationSource]ttnpb.LocationSource{
		legacyttnpb.LocationMetadata_GPS:            ttnpb.SOURCE_GPS,
		legacyttnpb.LocationMetadata_CONFIG:         ttnpb.SOURCE_REGISTRY,
		legacyttnpb.LocationMetadata_REGISTRY:       ttnpb.SOURCE_REGISTRY,
		legacyttnpb.LocationMetadata_IP_GEOLOCATION: ttnpb.SOURCE_IP_GEOLOCATION,
	}

	frequencyPlanToBand = map[uint32]string{
		0:  "EU_863_870",
		1:  "US_902_928",
		2:  "CN_779_787",
		3:  "EU_433",
		4:  "AU_915_928",
		5:  "CN_470_510",
		6:  "AS_923",
		7:  "KR_920_923",
		8:  "IN_865_867",
		9:  "RU_864_870",
		61: "AS_923",
		62: "AS_923",
	}

	errLoRaWANPayload  = errors.DefineInvalidArgument("lorawan_payload", "invalid LoRaWAN payload")
	errLoRaWANMetadata = errors.DefineInvalidArgument("lorawan_metadata", "missing LoRaWAN metadata")
	errDataRate        = errors.DefineInvalidArgument("data_rate", "unknown data rate `{data_rate}`")
	errModulation      = errors.DefineInvalidArgument("modulation", "unknown modulation `{modulation}`")
	errFrequencyPlan   = errors.DefineNotFound("frequency_plan", "unknown frequency plan `{frequency_plan}`")
)

func (V2) Version() topics.Version {
	return topics.V2
}

func (V2) MarshalDownlink(down *ttnpb.DownlinkMessage) ([]byte, error) {
	var fcnt uint32
	if pld, ok := down.Payload.Payload.(*ttnpb.Message_MACPayload); ok {
		fcnt = pld.MACPayload.FHDR.FCnt
	}
	modulation, ok := modulationToV2[down.Settings.Modulation]
	if !ok {
		return nil, errModulation.WithAttributes("modulation", modulation.String())
	}
	v2downlink := &legacyttnpb.DownlinkMessage{
		Payload: down.RawPayload,
		GatewayConfiguration: legacyttnpb.GatewayTxConfiguration{
			Frequency:             down.Settings.Frequency,
			Power:                 down.Settings.TxPower - eirpDelta,
			PolarizationInversion: true,
			RfChain:               0,
			Timestamp:             uint32(down.TxMetadata.Timestamp % math.MaxUint32),
		},
		ProtocolConfiguration: legacyttnpb.ProtocolTxConfiguration{
			LoRaWAN: &legacyttnpb.LoRaWANTxConfiguration{
				BitRate:    down.Settings.BitRate,
				CodingRate: down.Settings.CodingRate,
				DataRate:   fmt.Sprintf("SF%dBW%d", down.Settings.SpreadingFactor, down.Settings.Bandwidth/1000),
				FCnt:       fcnt,
				Modulation: modulation,
			},
		},
	}
	return v2downlink.Marshal()
}

func (V2) UnmarshalUplink(message []byte) (*ttnpb.UplinkMessage, error) {
	v2uplink := &legacyttnpb.UplinkMessage{}
	err := v2uplink.Unmarshal(message)
	if err != nil {
		return nil, err
	}

	if v2uplink.ProtocolMetadata.LoRaWAN == nil {
		return nil, errLoRaWANMetadata
	}
	lorawanMetadata := v2uplink.ProtocolMetadata.LoRaWAN
	gwMetadata := v2uplink.GatewayMetadata
	uplink := &ttnpb.UplinkMessage{
		RawPayload: v2uplink.Payload,
	}
	modulation, ok := modulationToV3[lorawanMetadata.Modulation]
	if !ok {
		return nil, errModulation.WithAttributes("modulation", modulation.String())
	}
	settings := ttnpb.TxSettings{
		CodingRate: lorawanMetadata.CodingRate,
		Frequency:  gwMetadata.Frequency,
		Modulation: modulation,
	}

	switch lorawanMetadata.Modulation {
	case legacyttnpb.Modulation_LORA:
		bandID, ok := frequencyPlanToBand[lorawanMetadata.FrequencyPlan]
		if !ok {
			return nil, errFrequencyPlan.WithAttributes("frequency_plan", lorawanMetadata.FrequencyPlan)
		}
		band, err := band.GetByID(bandID)
		if err != nil {
			return nil, err
		}

		var drIndex ttnpb.DataRateIndex
		var found bool
		dr := types.DataRate{
			LoRa: lorawanMetadata.DataRate,
		}
		for bandDRIndex, bandDR := range band.DataRates {
			if bandDR.Rate == dr {
				found = true
				drIndex = ttnpb.DataRateIndex(bandDRIndex)
				break
			}
		}
		if !found {
			return nil, errDataRate.WithAttributes("data_rate", lorawanMetadata.DataRate)
		}
		settings.DataRateIndex = drIndex
		settings.Bandwidth, err = dr.Bandwidth()
		if err != nil {
			return nil, err
		}
		sf, err := dr.SpreadingFactor()
		if err != nil {
			return nil, err
		}
		settings.SpreadingFactor = uint32(sf)
	case legacyttnpb.Modulation_FSK:
		settings.BitRate = lorawanMetadata.BitRate
	default:
		return nil, errModulation.WithAttributes("modulation", lorawanMetadata.Modulation)
	}

	ids := ttnpb.GatewayIdentifiers{
		GatewayID: gwMetadata.GatewayID,
	}
	uplink.RxMetadata = make([]*ttnpb.RxMetadata, 0, 1)
	mdTime := time.Unix(0, gwMetadata.Time)
	if antennas := gwMetadata.Antennas; len(antennas) > 0 {
		for _, antenna := range antennas {
			uplink.RxMetadata = append(uplink.RxMetadata, &ttnpb.RxMetadata{
				AntennaIndex:          antenna.Antenna,
				ChannelRSSI:           antenna.ChannelRSSI,
				FrequencyOffset:       antenna.FrequencyOffset,
				GatewayIdentifiers:    ids,
				RSSI:                  antenna.RSSI,
				RSSIStandardDeviation: antenna.RSSIStandardDeviation,
				SNR:                   antenna.SNR,
				Time:                  &mdTime,
				Timestamp:             uint64(gwMetadata.Timestamp),
			})
		}
	} else {
		uplink.RxMetadata = append(uplink.RxMetadata, &ttnpb.RxMetadata{
			AntennaIndex:       0,
			GatewayIdentifiers: ids,
			RSSI:               gwMetadata.RSSI,
			SNR:                gwMetadata.SNR,
			Time:               &mdTime,
			Timestamp:          uint64(gwMetadata.Timestamp),
		})
	}
	uplink.Settings = settings

	return uplink, nil
}

func (V2) UnmarshalStatus(message []byte) (*ttnpb.GatewayStatus, error) {
	v2status := &legacyttnpb.StatusMessage{}
	err := v2status.Unmarshal(message)
	if err != nil {
		return nil, err
	}
	metrics := map[string]float32{
		"lmnw": float32(v2status.LmNw),
		"lmst": float32(v2status.LmSt),
		"lmok": float32(v2status.LmOk),
		"lpps": float32(v2status.LPPS),
		"rxin": float32(v2status.RxIn),
		"rxok": float32(v2status.RxOk),
		"txin": float32(v2status.TxIn),
		"txok": float32(v2status.TxOk),
	}
	if os := v2status.OS; os != nil {
		metrics["cpu_percentage"] = os.CPUPercentage
		metrics["load_1"] = os.Load_1
		metrics["load_5"] = os.Load_5
		metrics["load_15"] = os.Load_15
		metrics["memory_percentage"] = os.MemoryPercentage
		metrics["temp"] = os.Temperature
	}
	if v2status.RTT != 0 {
		metrics["rtt_ms"] = float32(v2status.RTT)
	}
	versions := make(map[string]string)
	if v2status.DSP > 0 {
		versions["dsp"] = strconv.Itoa(int(v2status.DSP))
	}
	if v2status.FPGA > 0 {
		versions["fpga"] = strconv.Itoa(int(v2status.FPGA))
	}
	if v2status.HAL != "" {
		versions["hal"] = v2status.HAL
	}
	var antennasLocation []*ttnpb.Location
	if loc := v2status.Location; loc.Validate() {
		antennasLocation = []*ttnpb.Location{
			{
				Accuracy:  loc.Accuracy,
				Altitude:  loc.Altitude,
				Latitude:  float64(loc.Latitude),
				Longitude: float64(loc.Longitude),
				Source:    sourceToV3[loc.Source],
			},
		}
	}
	return &ttnpb.GatewayStatus{
		AntennaLocations: antennasLocation,
		BootTime:         time.Unix(0, v2status.BootTime),
		IP:               v2status.IP,
		Metrics:          metrics,
		Time:             time.Unix(0, v2status.Time),
		Versions:         versions,
	}, nil
}