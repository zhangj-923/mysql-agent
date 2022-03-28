package common

import (
	"github.com/influxdata/influxdb1-client/v2"
	"strconv"
)

type InfluxDBClient struct {
	Cli      client.Client
	Addr     string
	Username string
	Password string
	Port     int
	DB       string
}

func (i *InfluxDBClient) Conn() {
	port := strconv.Itoa(i.Port)
	cli, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     "http://" + i.Addr + ":" + port,
		Username: i.Username,
		Password: i.Password,
	})
	if err != nil {
		Error.Println(err)
	}

	_, _, err = cli.Ping(5)
	if err != nil {
		Error.Println(err)
	}
	i.Cli = cli
}

func (i *InfluxDBClient) CloseConn() {
	_ = i.Cli.Close()
}

//query
func (i *InfluxDBClient) QueryDB(cmd string) (res []client.Result, err error) {
	q := client.Query{
		Command:  cmd,
		Database: i.DB,
	}
	if response, err := i.Cli.Query(q); err == nil {
		if response.Error() != nil {
			return res, response.Error()
		}
		res = response.Results
	} else {
		return res, err
	}
	return res, nil
}

//insert
func (i *InfluxDBClient) WritesPoints(points []*client.Point) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  i.DB,
		Precision: "s",
	})
	if err != nil {
		Error.Println(err)
	}

	if len(points) != 0 {
		bp.AddPoints(points)
	}

	if err := i.Cli.Write(bp); err != nil {
		Error.Println(err)
	}

	Info.Println("insert influxdb success!")
}
