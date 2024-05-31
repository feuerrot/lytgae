package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.thethings.network/lorawan-stack/v3/pkg/errors"
	"go.thethings.network/lorawan-stack/v3/pkg/events"
	"go.thethings.network/lorawan-stack/v3/pkg/ttnpb"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

var (
	gwTime = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_time",
	}, []string{"gateway", "type"})
	gwCount = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_count",
	}, []string{"gateway", "type"})
)

const timeFmt = "2006-01-02 15:04:05"

type Gateway struct {
	id            string
	connectTime   time.Time
	uplinkTime    time.Time
	uplinkCount   uint64
	downlinkTime  time.Time
	downlinkCount uint64
	txAckTime     time.Time
	txAckCount    uint64
}

func (g Gateway) String() string {
	parts := []string{g.id}

	if g.connectTime.Unix() != 0 {
		parts = append(parts, fmt.Sprintf("connected: %s", g.connectTime.Format(timeFmt)))
		gwTime.WithLabelValues(g.id, "connect").Set(float64(g.connectTime.Unix()))
	}

	if g.uplinkCount != 0 {
		parts = append(parts, fmt.Sprintf("uplinks: %d (last %s)", g.uplinkCount, g.uplinkTime.Format(timeFmt)))
		gwTime.WithLabelValues(g.id, "uplink").Set(float64(g.uplinkTime.Unix()))
		gwCount.WithLabelValues(g.id, "uplink").Set(float64(g.uplinkCount))
	}

	if g.downlinkCount != 0 {
		parts = append(parts, fmt.Sprintf("downlinks: %d (last %s)", g.downlinkCount, g.downlinkTime.Format(timeFmt)))
		gwTime.WithLabelValues(g.id, "downlink").Set(float64(g.downlinkTime.Unix()))
		gwCount.WithLabelValues(g.id, "downlink").Set(float64(g.downlinkCount))
	}

	if g.txAckCount != 0 {
		parts = append(parts, fmt.Sprintf("txAck: %d (last %s)", g.txAckCount, g.txAckTime.Format(timeFmt)))
		gwTime.WithLabelValues(g.id, "txack").Set(float64(g.txAckTime.Unix()))
		gwCount.WithLabelValues(g.id, "txack").Set(float64(g.txAckCount))
	}

	return strings.Join(parts, " ")
}

type Client struct {
	server string
	apikey string

	gateways []*ttnpb.EntityIdentifiers
	esc      *ttnpb.Events_StreamClient
	ctx      context.Context
	conn     *grpc.ClientConn
}

func NewClient(server string, apikey string, gateways []string) (*Client, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    10 * time.Second,
			Timeout: time.Second,
		}),
	}

	md := metadata.Pairs("authorization", "Bearer "+apikey)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	conn, err := grpc.NewClient(server, opts...)
	if err != nil {
		return nil, fmt.Errorf("NewClient: %v", err)
	}

	client := &Client{
		server: server,
		apikey: apikey,
		ctx:    ctx,
		conn:   conn,
	}

	if len(gateways) == 0 {
		gateways, err := client.getGateways()
		if err != nil {
			return nil, fmt.Errorf("getGateways: %v", err)
		}
		client.gateways = gateways
	} else {
		for _, gw := range gateways {
			client.gateways = append(client.gateways, (&ttnpb.GatewayIdentifiers{GatewayId: gw}).GetEntityIdentifiers())
		}
	}

	return client, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) getGateways() ([]*ttnpb.EntityIdentifiers, error) {
	rtn := []*ttnpb.EntityIdentifiers{}
	log.Printf("Get gateways")

	req := &ttnpb.ListGatewaysRequest{}
	gws, err := ttnpb.NewGatewayRegistryClient(c.conn).List(c.ctx, req)
	if err != nil {
		return rtn, fmt.Errorf("list gateways: %v", err)
	}

	for _, gw := range gws.GetGateways() {
		log.Printf("Found gateway %s", gw.IDString())
		rtn = append(rtn, gw.Ids.GetEntityIdentifiers())
	}

	return rtn, nil
}

func (c *Client) connectEventstream() error {
	client := ttnpb.NewEventsClient(c.conn)
	req := &ttnpb.StreamEventsRequest{
		Identifiers: c.gateways,
	}
	esc, err := client.Stream(c.ctx, req)
	if err != nil {
		return err
	}

	c.esc = &esc

	return nil
}

func (c *Client) getEvents(ec chan<- events.Event) error {
	err := c.connectEventstream()
	if err != nil {
		return fmt.Errorf("connectEventstream: %v", err)
	}

	for {
		pEvent, err := (*c.esc).Recv()
		if err != nil {
			if errors.IsCanceled(err) {
				continue
			}
			if errors.IsUnavailable(err) {
				log.Printf("Lost connection, trying to reconnect")
				time.Sleep(5 * time.Second)
				err := c.connectEventstream()
				if err != nil {
					return fmt.Errorf("during reconnect: %v", err)
				}
			}
			return fmt.Errorf("recv: %v", err)
		}

		eEvent, err := events.FromProto(pEvent)
		if err != nil {
			return fmt.Errorf("FromProto: %v", err)
		}

		ec <- eEvent
	}
}

func main() {
	apikey, ok := os.LookupEnv("LYTGAE_APIKEY")
	if !ok {
		log.Fatalf("LYTGAE_APIKEY is not set")
	}

	server, ok := os.LookupEnv("LYTGAE_SERVER")
	if !ok {
		log.Printf("LYTGAE_SERVER is not set, fallback to eu1.cloud.thethings.network:8884")
		server = "eu1.cloud.thethings.network:8884"
	}

	var gws []string
	if egws, ok := os.LookupEnv("LYTGAE_GW"); ok {
		gws = strings.Split(egws, ",")
	}

	c, err := NewClient(server, apikey, gws)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	gateways := make(map[string]*Gateway)
	ch := make(chan events.Event)
	go c.getEvents(ch)

	go func() {
		for ev := range ch {
			if ev.Name() != "gs.gateway.connection.stats" {
				continue
			}

			data, ok := ev.Data().(*ttnpb.GatewayConnectionStats)
			if !ok {
				log.Printf("event data seems to be of type %T", ev.Data())
				continue
			}

			for _, id := range ev.Identifiers() {
				gwid := id.GetGatewayIds().GetGatewayId()

				gw := &Gateway{
					id:            gwid,
					connectTime:   data.GetConnectedAt().AsTime(),
					uplinkCount:   data.GetUplinkCount(),
					downlinkCount: data.GetDownlinkCount(),
					txAckCount:    data.GetTxAcknowledgmentCount(),
					uplinkTime:    data.GetLastUplinkReceivedAt().AsTime(),
					downlinkTime:  data.GetLastDownlinkReceivedAt().AsTime(),
					txAckTime:     data.GetLastTxAcknowledgmentReceivedAt().AsTime(),
				}

				gateways[gwid] = gw
			}

			k := maps.Keys[map[string]*Gateway](gateways)
			slices.Sort[[]string](k)
			for _, g := range k {
				log.Printf("Gateway %s", gateways[g])
			}
		}
	}()

	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(":2113", nil)
}
