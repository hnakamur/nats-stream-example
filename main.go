package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"sort"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/jsm.go/api"
	"github.com/nats-io/nats.go"
	"github.com/urfave/cli/v2"
)

func main() {
	cli.VersionFlag = &cli.BoolFlag{
		Name:    "version",
		Aliases: []string{"V"},
		Usage:   "print only the version",
	}

	serverFlag := &cli.StringFlag{
		Name:    "servers",
		Aliases: []string{"s"},
		Value:   "",
		Usage:   "comma separated values of server URLs",
		EnvVars: []string{"SERVERS"},
	}
	tlscaFlag := &cli.StringFlag{
		Name:    "tlsca",
		Aliases: []string{"a"},
		Usage:   "TLS CA certificate path",
		EnvVars: []string{"TLSCA"},
	}

	app := &cli.App{
		Name:    "nats-stream-example",
		Version: "0.0.1",
		Usage:   "A stream example for NATS",
		Commands: []*cli.Command{
			{
				Name:  "stream-add",
				Usage: "Add a stream",
				Action: func(cCtx *cli.Context) error {
					return streamAdd(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("stream"), cCtx.String("subject"), cCtx.Bool("json"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.BoolFlag{
						Name:  "json",
						Usage: "show stream info as JSON",
					},
					&cli.StringFlag{
						Name:     "stream",
						Required: true,
						Usage:    "stream name to create",
					},
					&cli.StringFlag{
						Name:     "subject",
						Required: true,
						Usage:    "subject to consume",
					},
				},
			},
			{
				Name:    "publish",
				Aliases: []string{"p"},
				Usage:   "Publish items",
				Action: func(cCtx *cli.Context) error {
					return publish(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("subject"), cCtx.Int("count"), cCtx.Bool("json"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "subject",
						Required: true,
						Usage:    "subject to publish to",
					},
					&cli.IntFlag{
						Name:    "count",
						Aliases: []string{"c"},
						Value:   1,
						Usage:   "count of messages to publish",
					},
					&cli.BoolFlag{
						Name:  "json",
						Usage: "show stream info as JSON",
					},
				},
			},
			{
				Name:  "consumer-add",
				Usage: "Add a consumer",
				Action: func(cCtx *cli.Context) error {
					return consumerAdd(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("consumer"), cCtx.String("stream"), cCtx.Bool("json"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "stream",
						Required: true,
						Usage:    "stream name",
					},
					&cli.StringFlag{
						Name:     "consumer",
						Required: true,
						Usage:    "consumer name",
					},
					&cli.BoolFlag{
						Name:  "json",
						Usage: "show stream info as JSON",
					},
				},
			},
			{
				Name:    "consumer-next",
				Aliases: []string{"c"},
				Usage:   "subscribe from consumer",
				Action: func(cCtx *cli.Context) error {
					return consumerNext(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("stream"), cCtx.String("consumer"), cCtx.Int("count"), cCtx.Bool("json"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "stream",
						Required: true,
						Usage:    "stream name",
					},
					&cli.StringFlag{
						Name:     "consumer",
						Required: true,
						Usage:    "consumer name",
					},
					&cli.IntFlag{
						Name:    "count",
						Aliases: []string{"c"},
						Value:   1,
						Usage:   "count of messages to consume",
					},
					&cli.BoolFlag{
						Name:  "json",
						Usage: "show stream info as JSON",
					},
				},
			},
			{
				Name:  "request",
				Usage: "send requests",
				Action: func(cCtx *cli.Context) error {
					return request(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("subject"), cCtx.Int("count"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "subject",
						Required: true,
						Usage:    "subject of queue",
					},
					&cli.IntFlag{
						Name:    "count",
						Aliases: []string{"c"},
						Value:   1,
						Usage:   "count of messages to send",
					},
				},
			},
			{
				Name:  "reply",
				Usage: "send replies",
				Action: func(cCtx *cli.Context) error {
					return reply(cCtx.String("servers"), cCtx.String("tlsca"), cCtx.String("subject"), cCtx.String("queue"))
				},
				Flags: []cli.Flag{
					serverFlag,
					tlscaFlag,
					&cli.StringFlag{
						Name:     "subject",
						Required: true,
						Usage:    "subject of queue",
					},
					&cli.StringFlag{
						Name:    "queue",
						Aliases: []string{"q"},
						Usage:   "queue group name",
					},
				},
			},
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func streamAdd(servers, tlsca, streamName, subject string, showJson bool) error {
	fmt.Println("streamAdd subcommand called: severs=", servers, ", tlscert=", tlsca)
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()

	fmt.Println("connected")

	mgr, err := jsm.New(nc)
	if err != nil {
		return err
	}

	streamCfg := api.StreamConfig{
		Name:       streamName,
		Subjects:   []string{subject},
		Storage:    api.FileStorage,
		Retention:  api.LimitsPolicy,
		Discard:    api.DiscardOld,
		Duplicates: 2 * time.Minute,
		Replicas:   1,
	}
	stream, err := mgr.NewStreamFromDefault(streamName, streamCfg)
	if err != nil {
		return err
	}
	if err := showStream(stream, showJson); err != nil {
		return err
	}
	return nil
}

func publish(servers, tlsca, subject string, count int, showJson bool) error {
	fmt.Println("publish subcommand called: severs=", servers, ", tlscert=", tlsca)
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()
	fmt.Println("connected")

	for i := 0; i < count; i++ {
		if i > 0 {
			time.Sleep(time.Second)
		}

		msg := nats.NewMsg(subject)
		now := time.Now()
		msg.Data = []byte(fmt.Sprintf("hello %d at %s", i, now.Format(time.RFC3339Nano)))
		if err := nc.PublishMsg(msg); err != nil {
			return err
		}
		if err := nc.Flush(); err != nil {
			return err
		}
		if err := nc.LastError(); err != nil {
			return err
		}
		log.Printf("i=%d, published msg.Data=%s", i, string(msg.Data))
	}

	return nil
}

func showStream(stream *jsm.Stream, json bool) error {
	info, err := stream.LatestInformation()
	if err != nil {
		return err
	}

	showStreamInfo(info, json)

	return nil
}

func printJSON(d interface{}) error {
	j, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return err
	}

	fmt.Println(string(j))

	return nil
}

func showStreamInfo(info *api.StreamInfo, json bool) {
	if json {
		if err := printJSON(info); err != nil {
			log.Fatalf("could not display info: %s", err)
		}
		return
	}

	fmt.Printf("Information for Stream %s created %s\n", info.Config.Name, info.Created.Local().Format("2006-01-02 15:04:05"))
	fmt.Println()
	showStreamConfig(info.Config)
	fmt.Println()

	if info.Cluster != nil && info.Cluster.Name != "" {
		fmt.Println("Cluster Information:")
		fmt.Println()
		fmt.Printf("                 Name: %s\n", info.Cluster.Name)
		fmt.Printf("               Leader: %s\n", info.Cluster.Leader)
		for _, r := range info.Cluster.Replicas {
			state := []string{r.Name}

			if r.Current {
				state = append(state, "current")
			} else {
				state = append(state, "outdated")
			}

			if r.Offline {
				state = append(state, "OFFLINE")
			}

			if r.Active > 0 && r.Active < math.MaxInt64 {
				state = append(state, fmt.Sprintf("seen %s ago", humanizeDuration(r.Active)))
			} else {
				state = append(state, "not seen")
			}

			switch {
			case r.Lag > 1:
				state = append(state, fmt.Sprintf("%s operations behind", humanize.Comma(int64(r.Lag))))
			case r.Lag == 1:
				state = append(state, fmt.Sprintf("%d operation behind", r.Lag))
			}

			fmt.Printf("              Replica: %s\n", strings.Join(state, ", "))

		}
		fmt.Println()
	}

	showSource := func(s *api.StreamSourceInfo) {
		fmt.Printf("          Stream Name: %s\n", s.Name)
		fmt.Printf("                  Lag: %s\n", humanize.Comma(int64(s.Lag)))
		if s.Active > 0 && s.Active < math.MaxInt64 {
			fmt.Printf("            Last Seen: %v\n", humanizeDuration(s.Active))
		} else {
			fmt.Printf("            Last Seen: never\n")
		}
		if s.External != nil {
			fmt.Printf("      Ext. API Prefix: %s\n", s.External.ApiPrefix)
			if s.External.DeliverPrefix != "" {
				fmt.Printf(" Ext. Delivery Prefix: %s\n", s.External.DeliverPrefix)
			}
		}
		if s.Error != nil {
			fmt.Printf("                Error: %s\n", s.Error.Description)
		}
	}
	if info.Mirror != nil {
		fmt.Println("Mirror Information:")
		fmt.Println()
		showSource(info.Mirror)
		fmt.Println()
	}

	if len(info.Sources) > 0 {
		fmt.Println("Source Information:")
		fmt.Println()
		for _, s := range info.Sources {
			showSource(s)
			fmt.Println()
		}
	}

	fmt.Println("State:")
	fmt.Println()
	fmt.Printf("             Messages: %s\n", humanize.Comma(int64(info.State.Msgs)))
	fmt.Printf("                Bytes: %s\n", humanize.IBytes(info.State.Bytes))
	if info.State.Lost != nil && len(info.State.Lost.Msgs) > 0 {
		fmt.Printf("        Lost Messages: %s (%s)\n", humanize.Comma(int64(len(info.State.Lost.Msgs))), humanize.IBytes(info.State.Lost.Bytes))
	}

	if info.State.FirstTime.Equal(time.Unix(0, 0)) || info.State.LastTime.IsZero() {
		fmt.Printf("             FirstSeq: %s\n", humanize.Comma(int64(info.State.FirstSeq)))
	} else {
		fmt.Printf("             FirstSeq: %s @ %s UTC\n", humanize.Comma(int64(info.State.FirstSeq)), info.State.FirstTime.Format("2006-01-02T15:04:05"))
	}

	if info.State.LastTime.Equal(time.Unix(0, 0)) || info.State.LastTime.IsZero() {
		fmt.Printf("              LastSeq: %s\n", humanize.Comma(int64(info.State.LastSeq)))
	} else {
		fmt.Printf("              LastSeq: %s @ %s UTC\n", humanize.Comma(int64(info.State.LastSeq)), info.State.LastTime.Format("2006-01-02T15:04:05"))
	}

	if len(info.State.Deleted) > 0 { // backwards compat with older servers
		fmt.Printf("     Deleted Messages: %d\n", len(info.State.Deleted))
	} else if info.State.NumDeleted > 0 {
		fmt.Printf("     Deleted Messages: %d\n", info.State.NumDeleted)
	}

	fmt.Printf("     Active Consumers: %d\n", info.State.Consumers)

	if info.State.NumSubjects > 0 { // available from 2.8
		fmt.Printf("   Number of Subjects: %d\n", info.State.NumSubjects)
	}

	if len(info.Alternates) > 0 {
		fmt.Printf("           Alternates: ")
		lName := 0
		lCluster := 0
		for _, s := range info.Alternates {
			if len(s.Name) > lName {
				lName = len(s.Name)
			}
			if len(s.Cluster) > lCluster {
				lCluster = len(s.Cluster)
			}
		}

		for i, s := range info.Alternates {
			msg := fmt.Sprintf("%s%s: Cluster: %s%s", strings.Repeat(" ", lName-len(s.Name)), s.Name, strings.Repeat(" ", lCluster-len(s.Cluster)), s.Cluster)
			if s.Domain != "" {
				msg = fmt.Sprintf("%s Domain: %s", msg, s.Domain)
			}

			if i == 0 {
				fmt.Println(msg)
			} else {
				fmt.Printf("                       %s\n", msg)
			}
		}
	}
}

func showStreamConfig(cfg api.StreamConfig) {
	fmt.Println("Configuration:")
	fmt.Println()
	if cfg.Description != "" {
		fmt.Printf("          Description: %s\n", cfg.Description)
	}
	if len(cfg.Subjects) > 0 {
		fmt.Printf("             Subjects: %s\n", strings.Join(cfg.Subjects, ", "))
	}
	if cfg.Sealed {
		fmt.Printf("               Sealed: true\n")
	}
	fmt.Printf("     Acknowledgements: %v\n", !cfg.NoAck)
	fmt.Printf("            Retention: %s - %s\n", cfg.Storage.String(), cfg.Retention.String())
	fmt.Printf("             Replicas: %d\n", cfg.Replicas)
	fmt.Printf("       Discard Policy: %s\n", cfg.Discard.String())
	fmt.Printf("     Duplicate Window: %v\n", cfg.Duplicates)
	fmt.Printf("    Allows Msg Delete: %v\n", !cfg.DenyDelete)
	fmt.Printf("         Allows Purge: %v\n", !cfg.DenyPurge)
	fmt.Printf("       Allows Rollups: %v\n", cfg.RollupAllowed)
	if cfg.AllowDirect {
		fmt.Printf("    Allows Direct Get: %v\n", cfg.AllowDirect)
	}

	if cfg.MaxMsgs == -1 {
		fmt.Println("     Maximum Messages: unlimited")
	} else {
		fmt.Printf("     Maximum Messages: %s\n", humanize.Comma(cfg.MaxMsgs))
	}
	if cfg.MaxMsgsPer > 0 {
		fmt.Printf("  Maximum Per Subject: %s\n", humanize.Comma(cfg.MaxMsgsPer))
	}
	if cfg.MaxBytes == -1 {
		fmt.Println("        Maximum Bytes: unlimited")
	} else {
		fmt.Printf("        Maximum Bytes: %s\n", humanize.IBytes(uint64(cfg.MaxBytes)))
	}
	if cfg.MaxAge <= 0 {
		fmt.Println("          Maximum Age: unlimited")
	} else {
		fmt.Printf("          Maximum Age: %s\n", humanizeDuration(cfg.MaxAge))
	}
	if cfg.MaxMsgSize == -1 {
		fmt.Println(" Maximum Message Size: unlimited")
	} else {
		fmt.Printf(" Maximum Message Size: %s\n", humanize.IBytes(uint64(cfg.MaxMsgSize)))
	}
	if cfg.MaxConsumers == -1 {
		fmt.Println("    Maximum Consumers: unlimited")
	} else {
		fmt.Printf("    Maximum Consumers: %d\n", cfg.MaxConsumers)
	}
	if cfg.Template != "" {
		fmt.Printf("  Managed by Template: %s\n", cfg.Template)
	}
	if cfg.Placement != nil {
		if cfg.Placement.Cluster != "" {
			fmt.Printf("    Placement Cluster: %s\n", cfg.Placement.Cluster)
		}
		if len(cfg.Placement.Tags) > 0 {
			fmt.Printf("       Placement Tags: %s\n", strings.Join(cfg.Placement.Tags, ", "))
		}
	}
	if cfg.RePublish != nil {
		if cfg.RePublish.HeadersOnly {
			fmt.Printf(" Republishing Headers: %s to %s", cfg.RePublish.Source, cfg.RePublish.Destination)
		} else {
			fmt.Printf("         Republishing: %s to %s", cfg.RePublish.Source, cfg.RePublish.Destination)
		}
	}
	if cfg.Mirror != nil {
		fmt.Printf("               Mirror: %s\n", renderSource(cfg.Mirror))
	}
	if len(cfg.Sources) > 0 {
		fmt.Printf("              Sources: ")
		sort.Slice(cfg.Sources, func(i, j int) bool {
			return cfg.Sources[i].Name < cfg.Sources[j].Name
		})

		for i, source := range cfg.Sources {
			if i == 0 {
				fmt.Println(renderSource(source))
			} else {
				fmt.Printf("                       %s\n", renderSource(source))
			}
		}
	}

	fmt.Println()
}

func renderSource(s *api.StreamSource) string {
	parts := []string{s.Name}
	if s.OptStartSeq > 0 {
		parts = append(parts, fmt.Sprintf("Start Seq: %s", humanize.Comma(int64(s.OptStartSeq))))
	}

	if s.OptStartTime != nil {
		parts = append(parts, fmt.Sprintf("Start Time: %v", s.OptStartTime))
	}
	if s.FilterSubject != "" {
		parts = append(parts, fmt.Sprintf("Subject: %s", s.FilterSubject))
	}
	if s.External != nil {
		parts = append(parts, fmt.Sprintf("API Prefix: %s", s.External.ApiPrefix))
		parts = append(parts, fmt.Sprintf("Delivery Prefix: %s", s.External.DeliverPrefix))
	}

	return strings.Join(parts, ", ")
}

func humanizeDuration(d time.Duration) string {
	if d == math.MaxInt64 {
		return "never"
	}

	tsecs := d / time.Second
	tmins := tsecs / 60
	thrs := tmins / 60
	tdays := thrs / 24
	tyrs := tdays / 365

	if tyrs > 0 {
		return fmt.Sprintf("%dy%dd%dh%dm%ds", tyrs, tdays%365, thrs%24, tmins%60, tsecs%60)
	}

	if tdays > 0 {
		return fmt.Sprintf("%dd%dh%dm%ds", tdays, thrs%24, tmins%60, tsecs%60)
	}

	if thrs > 0 {
		return fmt.Sprintf("%dh%dm%ds", thrs, tmins%60, tsecs%60)
	}

	if tmins > 0 {
		return fmt.Sprintf("%dm%ds", tmins, tsecs%60)
	}

	return fmt.Sprintf("%.2fs", d.Seconds())
}

func consumerAdd(servers, tlsca, consumerName, streamName string, showJson bool) error {
	fmt.Println("consumerAdd subcommand called: severs=", servers, ", tlscert=", tlsca)
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()
	fmt.Println("connected")

	mgr, err := jsm.New(nc)
	if err != nil {
		return err
	}

	consumerCfg := api.ConsumerConfig{
		Durable:       consumerName,
		DeliverPolicy: api.DeliverAll,
		ReplayPolicy:  api.ReplayInstant,
		AckPolicy:     api.AckExplicit,
	}
	consumer, err := mgr.NewConsumerFromDefault(streamName, consumerCfg)
	if err != nil {
		return err
	}
	if err := showConsumerInfo(consumer, showJson); err != nil {
		return err
	}
	return nil
}

func consumerNext(servers, tlsca, streamName, consumerName string, count int, showJson bool) error {
	fmt.Println("consumerNext subcommand called: severs=", servers, ", tlscert=", tlsca)

	nc, mgr, err := connectAndSetup(servers, tlsca, nats.UseOldRequestStyle())
	// nc, err := connect(servers, tlsca, nats.UseOldRequestStyle())
	if err != nil {
		return err
	}
	defer nc.Close()
	fmt.Println("connected")

	// mgr, err := jsm.New(nc)
	// if err != nil {
	// 	return err
	// }

	for i := 0; i < count; i++ {
		log.Printf("i=%d", i)
		if err := getNextMsgDirect(nc, mgr, streamName, consumerName); err != nil {
			return err
		}
	}

	return nil
}

func getNextMsgDirect(nc *nats.Conn, mgr *jsm.Manager, stream, consumer string) error {
	sub, err := nc.SubscribeSync(nc.NewRespInbox())
	if err != nil {
		return err
	}
	sub.AutoUnsubscribe(1)

	timeout := 5 * time.Second
	req := &api.JSApiConsumerGetNextRequest{Batch: 1, Expires: timeout}
	if err := mgr.NextMsgRequest(stream, consumer, sub.Subject, req); err != nil {
		return err
	}

	fatalIfNotPull := func() {
		cons, err := mgr.LoadConsumer(stream, consumer)
		if err != nil {
			log.Fatalf("could not load consumer %q, err=%v", consumer, err)
		}

		if !cons.IsPullMode() {
			log.Fatalf("consumer %q is not a Pull consumer", consumer)
		}
	}

	msg, err := sub.NextMsg(timeout)
	if err != nil {
		return err
	}

	if msg.Header != nil && msg.Header.Get("Status") == "503" {
		fatalIfNotPull()
	}

	metadata, err := jsm.ParseJSMsgMetadata(msg)
	if err != nil {
		if msg.Reply == "" {
			fmt.Printf("--- subject: %s\n", msg.Subject)
		} else {
			fmt.Printf("--- subject: %s reply: %s\n", msg.Subject, msg.Reply)
		}
		return err
	}
	fmt.Printf("[%s] subj: %s / tries: %d / cons seq: %d / str seq: %d / pending: %d\n", time.Now().Format("15:04:05"), msg.Subject, metadata.Delivered(), metadata.ConsumerSequence(), metadata.StreamSequence(), metadata.Pending())
	if len(msg.Header) > 0 {
		fmt.Println()
		fmt.Println("Headers:")
		fmt.Println()
		for h, vals := range msg.Header {
			for _, val := range vals {
				fmt.Printf("  %s: %s\n", h, val)
			}
		}

		fmt.Println()
		fmt.Println("Data:")
		fmt.Println()
	}

	fmt.Println()
	fmt.Println(string(msg.Data))

	if err := msg.Respond(nil); err != nil {
		return err
	}
	if err := nc.Flush(); err != nil {
		return err
	}
	return nil
}

func showConsumerInfo(consumer *jsm.Consumer, json bool) error {
	config := consumer.Configuration()
	state, err := consumer.LatestState()
	if err != nil {
		return err
	}

	if json {
		printJSON(state)
		return nil
	}

	fmt.Printf("Information for Consumer %s > %s created %s\n", state.Stream, state.Name, state.Created.Local().Format(time.RFC3339))
	fmt.Println()
	fmt.Println("Configuration:")
	fmt.Println()
	if config.Durable != "" {
		fmt.Printf("        Durable Name: %s\n", config.Durable)
	}
	if config.Description != "" {
		fmt.Printf("         Description: %s\n", config.Description)
	}
	if config.DeliverSubject != "" {
		fmt.Printf("    Delivery Subject: %s\n", config.DeliverSubject)
	} else {
		fmt.Printf("           Pull Mode: true\n")
	}
	if config.FilterSubject != "" {
		fmt.Printf("      Filter Subject: %s\n", config.FilterSubject)
	}
	switch config.DeliverPolicy {
	case api.DeliverAll:
		fmt.Printf("      Deliver Policy: All\n")
	case api.DeliverLast:
		fmt.Printf("      Deliver Policy: Last\n")
	case api.DeliverNew:
		fmt.Printf("      Deliver Policy: New\n")
	case api.DeliverLastPerSubject:
		fmt.Printf("      Deliver Policy: Last Per Subject\n")
	case api.DeliverByStartTime:
		fmt.Printf("      Deliver Policy: Since %v\n", config.OptStartTime)
	case api.DeliverByStartSequence:
		fmt.Printf("      Deliver Policy: From Sequence %d\n", config.OptStartSeq)
	}
	if config.DeliverGroup != "" && config.DeliverSubject != "" {
		fmt.Printf(" Deliver Queue Group: %s\n", config.DeliverGroup)
	}
	fmt.Printf("          Ack Policy: %s\n", config.AckPolicy.String())
	if config.AckPolicy != api.AckNone {
		fmt.Printf("            Ack Wait: %v\n", config.AckWait)
	}
	fmt.Printf("       Replay Policy: %s\n", config.ReplayPolicy.String())
	if config.MaxDeliver != -1 {
		fmt.Printf("  Maximum Deliveries: %d\n", config.MaxDeliver)
	}
	if config.SampleFrequency != "" {
		fmt.Printf("       Sampling Rate: %s\n", config.SampleFrequency)
	}
	if config.RateLimit > 0 {
		fmt.Printf("          Rate Limit: %s / second\n", humanize.IBytes(config.RateLimit/8))
	}
	if config.MaxAckPending > 0 {
		fmt.Printf("     Max Ack Pending: %s\n", humanize.Comma(int64(config.MaxAckPending)))
	}
	if config.MaxWaiting > 0 {
		fmt.Printf("   Max Waiting Pulls: %s\n", humanize.Comma(int64(config.MaxWaiting)))
	}
	if config.Heartbeat > 0 {
		fmt.Printf("      Idle Heartbeat: %s\n", humanizeDuration(config.Heartbeat))
	}
	if config.DeliverSubject != "" {
		fmt.Printf("        Flow Control: %v\n", config.FlowControl)
	}
	if config.HeadersOnly {
		fmt.Printf("        Headers Only: true\n")
	}
	if config.InactiveThreshold > 0 && config.DeliverSubject == "" {
		fmt.Printf("  Inactive Threshold: %s\n", humanizeDuration(config.InactiveThreshold))
	}
	if config.MaxRequestExpires > 0 {
		fmt.Printf("     Max Pull Expire: %s\n", humanizeDuration(config.MaxRequestExpires))
	}
	if config.MaxRequestBatch > 0 {
		fmt.Printf("      Max Pull Batch: %s\n", humanize.Comma(int64(config.MaxRequestBatch)))
	}
	if config.MaxRequestMaxBytes > 0 {
		fmt.Printf("   Max Pull MaxBytes: %s\n", humanize.Comma(int64(config.MaxRequestMaxBytes)))
	}
	if len(config.BackOff) > 0 {
		fmt.Printf("             Backoff: %s\n", renderBackoff(config.BackOff))
	}
	if config.Replicas > 0 {
		fmt.Printf("            Replicas: %d\n", config.Replicas)
	}
	if config.MemoryStorage {
		fmt.Printf("      Memory Storage: yes\n")
	}
	fmt.Println()

	if state.Cluster != nil && state.Cluster.Name != "" {
		fmt.Println("Cluster Information:")
		fmt.Println()
		fmt.Printf("                Name: %s\n", state.Cluster.Name)
		fmt.Printf("              Leader: %s\n", state.Cluster.Leader)
		for _, r := range state.Cluster.Replicas {
			since := fmt.Sprintf("seen %s ago", humanizeDuration(r.Active))
			if r.Active == 0 || r.Active == math.MaxInt64 {
				since = "not seen"
			}

			if r.Current {
				fmt.Printf("             Replica: %s, current, %s\n", r.Name, since)
			} else {
				fmt.Printf("             Replica: %s, outdated, %s\n", r.Name, since)
			}
		}
		fmt.Println()
	}

	fmt.Println("State:")
	fmt.Println()
	if state.Delivered.Last == nil {
		fmt.Printf("   Last Delivered Message: Consumer sequence: %s Stream sequence: %s\n", humanize.Comma(int64(state.Delivered.Consumer)), humanize.Comma(int64(state.Delivered.Stream)))
	} else {
		fmt.Printf("   Last Delivered Message: Consumer sequence: %s Stream sequence: %s Last delivery: %s ago\n", humanize.Comma(int64(state.Delivered.Consumer)), humanize.Comma(int64(state.Delivered.Stream)), humanizeDuration(time.Since(*state.Delivered.Last)))
	}

	if config.AckPolicy != api.AckNone {
		if state.AckFloor.Last == nil {
			fmt.Printf("     Acknowledgment floor: Consumer sequence: %s Stream sequence: %s\n", humanize.Comma(int64(state.AckFloor.Consumer)), humanize.Comma(int64(state.AckFloor.Stream)))
		} else {
			fmt.Printf("     Acknowledgment floor: Consumer sequence: %s Stream sequence: %s Last Ack: %s ago\n", humanize.Comma(int64(state.AckFloor.Consumer)), humanize.Comma(int64(state.AckFloor.Stream)), humanizeDuration(time.Since(*state.AckFloor.Last)))
		}
		if config.MaxAckPending > 0 {
			fmt.Printf("         Outstanding Acks: %s out of maximum %s\n", humanize.Comma(int64(state.NumAckPending)), humanize.Comma(int64(config.MaxAckPending)))
		} else {
			fmt.Printf("         Outstanding Acks: %s\n", humanize.Comma(int64(state.NumAckPending)))
		}
		fmt.Printf("     Redelivered Messages: %s\n", humanize.Comma(int64(state.NumRedelivered)))
	}

	fmt.Printf("     Unprocessed Messages: %s\n", humanize.Comma(int64(state.NumPending)))
	if config.DeliverSubject == "" {
		if config.MaxWaiting > 0 {
			fmt.Printf("            Waiting Pulls: %s of maximum %s\n", humanize.Comma(int64(state.NumWaiting)), humanize.Comma(int64(config.MaxWaiting)))
		} else {
			fmt.Printf("            Waiting Pulls: %s of unlimited\n", humanize.Comma(int64(state.NumWaiting)))
		}
	} else {
		if state.PushBound {
			if config.DeliverGroup != "" {
				fmt.Printf("          Active Interest: Active using Queue Group %s", config.DeliverGroup)
			} else {
				fmt.Printf("          Active Interest: Active")
			}
		} else {
			fmt.Printf("          Active Interest: No interest")
		}
	}

	fmt.Println()
	return nil
}

func renderBackoff(bo []time.Duration) string {
	if len(bo) == 0 {
		return "unset"
	}

	var times []string

	if len(bo) > 15 {
		for _, d := range bo[:5] {
			times = append(times, d.String())
		}
		times = append(times, "...")
		for _, d := range bo[len(bo)-5:] {
			times = append(times, d.String())
		}

		return fmt.Sprintf("%s (%d total)", strings.Join(times, ", "), len(bo))
	} else {
		for _, p := range bo {
			times = append(times, p.String())
		}

		return strings.Join(times, ", ")
	}
}

func request(servers, tlsca, subject string, count int) error {
	fmt.Println("request subcommand called: severs=", servers, ", tlscert=", tlsca)
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()
	fmt.Println("connected")

	for i := 0; i < count; i++ {
		now := time.Now()
		nowStr := now.Format(time.RFC3339Nano)
		payload := "help me at " + nowStr
		msg, err := nc.Request(subject, []byte(payload), 2*time.Second)
		if err != nil {
			if nc.LastError() != nil {
				log.Fatalf("%v for request", nc.LastError())
			}
			log.Fatalf("%v for request", err)
		}
		log.Printf("Published [%s] : '%s'", subject, payload)
		log.Printf("Received  [%v] : '%s'", msg.Subject, string(msg.Data))
	}

	return nil
}

func reply(servers, tlsca, subject, queueName string) error {
	fmt.Println("reply subcommand called: severs=", servers, ", tlscert=", tlsca)
	nc, err := connect(servers, tlsca)
	if err != nil {
		return err
	}
	defer nc.Close()
	fmt.Println("connected")

	i := 0
	_, err = nc.QueueSubscribe(subject, queueName, func(msg *nats.Msg) {
		i++
		msgData := string(msg.Data)
		log.Printf("[#%d] Received on [%s]: '%s'\n", i, msg.Subject, msgData)
		now := time.Now()
		nowStr := now.Format(time.RFC3339Nano)
		reply := "reply at " + nowStr + " for " + msgData
		msg.Respond([]byte(reply))
	})
	if err != nil {
		return fmt.Errorf("subscribe queue: %s", err)
	}
	if err = nc.Flush(); err != nil {
		return fmt.Errorf("flush: %s", err)
	}

	log.Printf("Listening on [%s]", subject)

	// Setup the interrupt handler to drain so we don't miss
	// requests when scaling down.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	log.Printf("Draining...")
	if err = nc.Drain(); err != nil {
		log.Printf("drain: %s", err)
	}
	log.Printf("Exiting")
	return nil
}

func connect(servers, tlsca string, opts ...nats.Option) (*nats.Conn, error) {
	if tlsca != "" {
		log.Printf("tlsca options is not implemented yet")
	}
	return nats.Connect(servers, opts...)
}

func connectAndSetup(servers, tlsca string, opts ...nats.Option) (*nats.Conn, *jsm.Manager, error) {
	if tlsca != "" {
		log.Printf("tlsca options is not implemented yet")
	}

	nc, err := nats.Connect(servers, opts...)
	if err != nil {
		return nil, nil, err
	}

	// TODO: setup jsm.Manager
	jsApiPrefix := ""
	jsEventPrefix := ""
	jsDomain := ""
	timeout := 5 * time.Second
	jsopts := []jsm.Option{
		jsm.WithAPIPrefix(jsApiPrefix),
		jsm.WithEventPrefix(jsEventPrefix),
		jsm.WithDomain(jsDomain),
		jsm.WithTimeout(timeout),
	}
	mgr, err := jsm.New(nc, jsopts...)
	if err != nil {
		return nil, nil, err
	}

	return nc, mgr, nil
}
