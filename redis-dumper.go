package main

import (
	"fmt"
	"os"
	"strings"
	"regexp"
	"github.com/codegangsta/cli"
	"github.com/go-redis/redis"
)

func main() {
	run(os.Args)
}

func run(args []string) {
	app := cli.NewApp()
	app.Name = "redis-dumper"
	app.Usage = "Dump Redis database"

	app.Flags = []cli.Flag{
    	cli.StringFlag {
      		Name:  "address, a",
      		Usage: "Redis addresshostname",
			Value: "127.0.0.1:6379",
    	},
    	cli.StringFlag {
      		Name:  "password",
      		Usage: "Redis password",
			Value: "",
    	},
    	cli.IntFlag {
      		Name:  "db, d",
      		Usage: "Redis Database number",
			Value: 0,
    	},
    	cli.StringFlag {
      		Name:  "filter, f",
      		Usage: "Redis key filter",
			Value: "*",
    	},
  	}

  	app.Action = doDump
	app.Run(args)
}

func doDump(c *cli.Context) {
	redis, err := connectRedis(c.String("address"), c.String("password"), c.Int("db"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Failed to connect Redis server:", err)
		os.Exit(2)
	}
	defer redis.Close()

	err = scanKeys(redis, c.String("filter"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}

func scanKeys(redis *redis.Client, filter string) error {
	cursor := uint64(0)
	var keys []string
	var err error
	fmt.Fprintln(os.Stderr, "Dump Redis database with filter", filter)

	for {
		keys, cursor, err = redis.Scan(cursor, filter, 1000).Result()
		if err != nil { return err }

		for _, key := range(keys) {
			ttl, err := redis.TTL(key).Result()
			if err != nil { return err }

			t, err := redis.Type(key).Result()
			if err != nil { return err }
			switch t {
				case "string":
					v, err := redis.Get(key).Result()
					if err != nil { return err }
					if (ttl >= 0) {
						fmt.Printf("PSETEX %s %v %s\n", escape(key), ttl.Nanoseconds()/100000, escape(v))
					} else {
						fmt.Printf("SET %s %s\n", escape(key), escape(v))
					}

				case "list":
					v, err := redis.LRange(key, 0, -1).Result()
					if err != nil { return err }
					fmt.Printf("DEL %s\n", escape(key))
					if len(v) > 0 {
						fmt.Printf("RPUSH %s %s\n", escape(key), strings.Join(escapeSlice(v), " "))
						fmt.Printf("EXPIRE %s %v\n", escape(key), ttl.Seconds())
					}

				case "set":
					v, err := redis.SMembers(key).Result()
					if err != nil { return err }
					fmt.Printf("DEL %s\n", escape(key))
					if len(v) > 0 {
						fmt.Printf("SADD %s %s\n", escape(key), strings.Join(escapeSlice(v), " "))
						fmt.Printf("EXPIRE %s %v\n", escape(key), ttl.Seconds())
					}

				case "zset":
					v, err := redis.ZRange(key, 0, -1).Result()
					if err != nil { return err }
					fmt.Printf("DEL %s\n", escape(key))
					if len(v) > 0 {
						panic("not supported yet")
						// fmt.Printf("SADD %s %s", escape(key), strings.Join(escapeSlice(v), " "))
					}

				case "hash":
					v, err := redis.HGetAll(key).Result()
					if err != nil { return err }
					if len(v) > 0 {
						panic("not supported yet")
					}
			}
		}

		if cursor == 0 { break }
	}
	return nil
}

func connectRedis(address string, password string, db int) (*redis.Client, error) {
	redis := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       db,
	})
	// _, err := redis.Ping().Result()
	return redis, nil
}

var noescape = regexp.MustCompile(`^([a-zA-Z0-9_\:\-]+)$`)
func escape(raw string) string {
	if noescape.MatchString(raw) {
		return raw
	}

	s := strings.Join(strings.Split(strings.Join(strings.Split(raw, "\\"), "\\\\"), "'"), "\\'")
	return fmt.Sprintf("'%s'", s)
}

func escapeSlice(raw []string) (out []string) {
	for _, s := range(raw) {
		out = append(out, escape(s))
	}
	return
}
