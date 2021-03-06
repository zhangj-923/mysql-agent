package internal

import (
	"fmt"
	client "github.com/influxdata/influxdb1-client/v2"
	"mysql-agent/common"
	"strconv"
)

type collect struct {
	mysqlClient *common.MysqlClient
	influClient *common.InfluxDBClient
	entityList  *[]common.Endpoint
	entity      *common.Endpoint
	conf        *common.AutoGenerated
	uptime      int
}

func (c *collect) init() {
	client := common.Conf{}
	conf := client.GetModelClass("config.json")
	c.conf = conf
	c.mysqlClient = &common.MysqlClient{
		Host:     conf.Mysql.Host,
		Port:     conf.Mysql.Port,
		Dbname:   conf.Mysql.Dbname,
		Username: conf.Mysql.Username,
		Password: conf.Mysql.Password,
	}
	c.influClient = &common.InfluxDBClient{
		Cli:      nil,
		Addr:     conf.Influxdb.Host,
		Username: conf.Influxdb.Username,
		Password: conf.Influxdb.Password,
		Port:     conf.Influxdb.Port,
		DB:       conf.Influxdb.Database,
	}
	entityList := make([]common.Endpoint, 0)
	if c.conf.IsMock {
		for i := range conf.Endpoint {
			entityMap := conf.Endpoint[i]
			entity := common.Endpoint{}
			entity.IpAddress = entityMap.IpAddress
			entity.Port = entityMap.Port
			entity.Username = entityMap.Username
			entity.Password = entityMap.Password
			entity.Dbname = entityMap.Dbname
			entity.EntityId = entityMap.EntityId
			entityList = append(entityList, entity)
		}
		c.entityList = &entityList
	} else {
		c.entityList = &entityList
	}

}

func Run() {
	c := collect{}
	c.init()
	if len(*c.entityList) > 0 {
		entitiyList := *c.entityList
		// 对实例进行循环
		for i := range entitiyList {
			// 声明需要监控的mysql对象
			entityMap := entitiyList[i]
			mysqlClient := &common.MysqlClient{
				Host:     entityMap.IpAddress,
				Port:     entityMap.Port,
				Username: entityMap.Username,
				Password: entityMap.Password,
				Dbname:   entityMap.Dbname,
			}

			// 建立连接 结束关闭连接
			mysqlClient.GetConn()
			defer mysqlClient.CloseConn()

			sql := "show global status;"
			list := mysqlClient.Query(sql)
			for i := range list {
				resultMap := list[i]
				if resultMap["Variable_name"].(string) == "Uptime" {
					c.uptime, _ = strconv.Atoi(resultMap["Value"].(string))
					fmt.Printf("采集uptime: %d \n", c.uptime)
					break
				}
			}

			// 处理采集数据
			var points []*client.Point
			points = c.toCollector(mysqlClient, entityMap, list)

			//采集数据入库操作
			c.sendData(points)
		}
	} else {
		return
	}

}

func (c *collect) toCollector(mysqlClient *common.MysqlClient, entityMap common.Endpoint, list []map[string]interface{}) []*client.Point {
	pParameter, err := c.getPParameter(list, mysqlClient) // 性能参数
	comTotal, err := c.getComTotal(list)                  // 操作统计
	lockTotal, err := c.getLockTotal(list)                // 锁
	netTotal, err := c.getNetTotal(list)                  // 流量
	pool, err := c.getPool(list, mysqlClient)             // InnoDB缓冲池

	if err != nil {
		fmt.Printf("get collector error! : %s \n", err)
		common.Error.Printf("get collector error! : %s \n", err)
	}

	tags := map[string]string{
		"entity_id": strconv.Itoa(entityMap.EntityId),
		"ipAddress": entityMap.IpAddress,
		"port":      strconv.Itoa(entityMap.Port),
		"Uptime":    strconv.Itoa(c.uptime),
	}

	points := make([]*client.Point, 0)
	points = append(points, parsePParameter(tags, pParameter)...)
	points = append(points, parseComTotal(tags, comTotal)...)
	points = append(points, parseLockTotal(tags, lockTotal)...)
	points = append(points, parseNetTotal(tags, netTotal)...)
	points = append(points, parsePool(tags, pool)...)
	return points
}

func (c *collect) sendData(points []*client.Point) {
	if len(points) > 0 {
		c.influClient.Conn()
		c.influClient.WritesPoints(points)
		c.influClient.CloseConn()
	}
	fmt.Printf("本次采集结束，共采集入库数据 %d 条 \n", len(points))
}

func (c *collect) getPParameter(list []map[string]interface{}, mysqlClient *common.MysqlClient) (*common.PParameter, error) {
	var (
		err          error
		Value        int
		VariableName string
	)
	pParameter := common.PParameter{}
	// 取当前值 无需计算
	for i := range list {
		resultMap := list[i]
		switch resultMap["Variable_name"].(string) {
		case "Created_tmp_tables":
			pParameter.CreatedTmpTables, err = strconv.Atoi(resultMap["Value"].(string))
		case "Slow_queries":
			pParameter.SlowQueries, err = strconv.Atoi(resultMap["Value"].(string))
		case "Threads_connected":
			pParameter.ThreadsConnected, err = strconv.Atoi(resultMap["Value"].(string))
		case "Threads_running":
			pParameter.ThreadsRunning, err = strconv.Atoi(resultMap["Value"].(string))
		case "Created_tmp_disk_tables":
			pParameter.CreatedTmpDiskTables, err = strconv.Atoi(resultMap["Value"].(string))
		default:
			continue
		}
	}

	// 计算连接使用率(%)
	sql := "show variables like 'max_connections';"
	err = mysqlClient.MysqlConn.QueryRow(sql).Scan(&VariableName, &Value)
	pParameter.MaxConnections = Value
	if Value == 0 {
		pParameter.ConnectionUtilization = 0.00
	} else {
		pParameter.ConnectionUtilization = (float64(pParameter.ThreadsConnected) / float64(pParameter.MaxConnections)) * 100
		pParameter.ConnectionUtilization, err = strconv.ParseFloat(fmt.Sprintf("%.2f", pParameter.ConnectionUtilization), 64)
	}

	// 计算临时表使用率(%)
	if pParameter.CreatedTmpTables == 0 {
		pParameter.TmpTablesUtilization = 0.00
	} else {
		pParameter.TmpTablesUtilization = (float64(pParameter.CreatedTmpDiskTables) / float64(pParameter.CreatedTmpTables)) * 100
		pParameter.TmpTablesUtilization, err = strconv.ParseFloat(fmt.Sprintf("%.2f", pParameter.TmpTablesUtilization), 64)
	}

	if err != nil {
		return nil, err
	} else {
		return &pParameter, nil
	}
}

func (c *collect) getComTotal(list []map[string]interface{}) (*common.CommitTotal, error) {
	var (
		err error
	)
	comTotal := common.CommitTotal{}
	for i := range list {
		resultMap := list[i]
		switch resultMap["Variable_name"].(string) {
		case "Com_delete":
			comTotal.ComDelete, err = strconv.Atoi(resultMap["Value"].(string))
		case "Com_insert":
			comTotal.ComInsert, err = strconv.Atoi(resultMap["Value"].(string))
		case "Com_reads":
			comTotal.ComReads, err = strconv.Atoi(resultMap["Value"].(string))
		case "Com_select":
			comTotal.ComSelect, err = strconv.Atoi(resultMap["Value"].(string))
		case "Com_update":
			comTotal.ComUpdate, err = strconv.Atoi(resultMap["Value"].(string))
		case "Com_writes":
			comTotal.ComWrites, err = strconv.Atoi(resultMap["Value"].(string))
		case "Questions":
			comTotal.Monitor.Questions, err = strconv.Atoi(resultMap["Value"].(string))
		case "Com_commit":
			comTotal.Monitor.Commit, err = strconv.Atoi(resultMap["Value"].(string))
		case "Com_rollback":
			comTotal.Monitor.Rollback, err = strconv.Atoi(resultMap["Value"].(string))
		default:
			continue
		}
	}
	// 计算平均每秒值
	if c.uptime > 0 {
		comTotal.ComDelete = comTotal.ComDelete / c.uptime
		comTotal.ComInsert = comTotal.ComInsert / c.uptime
		comTotal.ComReads = comTotal.ComReads / c.uptime
		comTotal.ComSelect = comTotal.ComSelect / c.uptime
		comTotal.ComUpdate = comTotal.ComUpdate / c.uptime
		comTotal.ComWrites = comTotal.ComWrites / c.uptime
		comTotal.Monitor.Qps, err = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(comTotal.Monitor.Questions)/float64(c.uptime)), 64)
		comTotal.Monitor.Tps, err = strconv.ParseFloat(fmt.Sprintf("%.2f", (float64(comTotal.Monitor.Commit)+float64(comTotal.Monitor.Rollback))/float64(c.uptime)), 64)
	} else { //避免除数为0得情况出现 做得防御性操作
		comTotal.ComDelete = 0.00
		comTotal.ComInsert = 0.00
		comTotal.ComReads = 0.00
		comTotal.ComSelect = 0.00
		comTotal.ComUpdate = 0.00
		comTotal.ComWrites = 0.00
		comTotal.Monitor.Qps = 0.00
		comTotal.Monitor.Tps = 0.00
	}

	if err != nil {
		return nil, err
	} else {
		return &comTotal, nil
	}
}

func (c *collect) getLockTotal(list []map[string]interface{}) (*common.LockTotal, error) {
	var err error
	lockTotal := common.LockTotal{}
	// 取当前值 无需计算
	for i := range list {
		resultMap := list[i]
		switch resultMap["Variable_name"].(string) {
		case "Table_locks_immediate":
			lockTotal.TableLocksImmediate, err = strconv.Atoi(resultMap["Value"].(string))
		case "Table_locks_waited":
			lockTotal.TableLocksWaited, err = strconv.Atoi(resultMap["Value"].(string))
		default:
			continue
		}
	}
	if err != nil {
		return nil, err
	} else {
		return &lockTotal, nil
	}
}

func (c *collect) getNetTotal(list []map[string]interface{}) (*common.NetworkTatal, error) {
	var (
		bytesReceived int
		byresSent     int
		err           error
	)
	netTotal := common.NetworkTatal{}
	// 需计算每秒均值
	for i := range list {
		resultMap := list[i]
		switch resultMap["Variable_name"].(string) {
		case "Bytes_received":
			bytesReceived, err = strconv.Atoi(resultMap["Value"].(string))
		case "Bytes_sent":
			byresSent, err = strconv.Atoi(resultMap["Value"].(string))
		default:
			continue
		}
	}
	if c.uptime > 0 {
		netTotal.BytesReceived, err = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(bytesReceived)/float64(c.uptime)), 64)
		netTotal.BytesSent, err = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(byresSent)/float64(c.uptime)), 64)
	} else { //避免除数为0得情况出现 做得防御性操作
		netTotal.BytesReceived = 0.00
		netTotal.BytesSent = 0.00
	}

	if err != nil {
		return nil, err
	} else {
		return &netTotal, nil
	}
}

func (c *collect) getPool(list []map[string]interface{}, mysqlClient *common.MysqlClient) (*common.Pool, error) {
	var (
		err          error
		Value        int
		VariableName string
		sql          string
	)
	pool := common.Pool{}
	for i := range list {
		resultMap := list[i]
		switch resultMap["Variable_name"].(string) {
		case "Innodb_buffer_pool_reads":
			pool.InnodbBufferPoolReads, err = strconv.Atoi(resultMap["Value"].(string)) //innodb缓冲池的读命中率
		case "Innodb_buffer_pool_read_requests":
			pool.InnodbBufferPoolReadRequests, err = strconv.Atoi(resultMap["Value"].(string)) //innodb缓冲池的读命中率
		case "Innodb_buffer_pool_pages_free":
			pool.InnodbBufferPoolPagesFree, err = strconv.Atoi(resultMap["Value"].(string)) // 计算Innodb缓冲池的利用率
		case "Innodb_buffer_pool_pages_total":
			pool.InnodbBufferPoolPagesTotal, err = strconv.Atoi(resultMap["Value"].(string)) // 计算Innodb缓冲池的利用率
		case "Key_blocks_used":
			pool.KeyBlocksUsed, err = strconv.Atoi(resultMap["Value"].(string)) //   计算 MyISAM缓冲池大小(MB)
		case "Key_blocks_unused":
			pool.KeyBlocksUnused, err = strconv.Atoi(resultMap["Value"].(string)) // 计算 MyISAM缓冲池大小(MB)   MyISAM平均使用率(%)
		case "Key_reads":
			pool.KeyReads, err = strconv.Atoi(resultMap["Value"].(string)) // MyISAM平均读命中率(%)
		case "Key_read_requests":
			pool.KeyReadRequests, err = strconv.Atoi(resultMap["Value"].(string)) // MyISAM平均读命中率(%)
		case "Key_writes":
			pool.KeyWrites, err = strconv.Atoi(resultMap["Value"].(string)) // MyISAM平均写命中率(%)
		case "Key_write_requests":
			pool.KeyWriteRequests, err = strconv.Atoi(resultMap["Value"].(string)) // MyISAM平均写命中率(%)
		default:
			continue
		}
	}

	// InnoDB缓冲池数量
	sql = "show variables like '%innodb_buffer_pool_instances%';"
	err = mysqlClient.MysqlConn.QueryRow(sql).Scan(&VariableName, &Value)
	pool.InnodbBufferPoolInstances = Value

	// InnoDB缓冲池大小(MB)
	sql = "show variables like '%innodb_buffer_pool_size%';"
	err = mysqlClient.MysqlConn.QueryRow(sql).Scan(&VariableName, &Value)
	pool.InnodbBufferPoolSize, err = strconv.ParseFloat(fmt.Sprintf("%.2f", float64(Value/1024/1024)), 64)

	//innodb缓冲池的读命中率
	pool.InnodbBufferPoolReadsHit = (1.0 - float64(pool.InnodbBufferPoolReads)/float64(pool.InnodbBufferPoolReadRequests)) * 100
	if pool.InnodbBufferPoolReadRequests == 0 {
		pool.InnodbBufferPoolReadsHit = 0.00
	}
	pool.InnodbBufferPoolReadsHit, err = strconv.ParseFloat(fmt.Sprintf("%.2f", pool.InnodbBufferPoolReadsHit), 64)

	//计算Innodb缓冲池的利用率
	pool.InnodbBufferPoolUtilization = (1.0 - float64(pool.InnodbBufferPoolPagesFree)/float64(pool.InnodbBufferPoolPagesTotal)) * 100
	if pool.InnodbBufferPoolPagesTotal == 0 {
		pool.InnodbBufferPoolUtilization = 0.00
	}
	pool.InnodbBufferPoolUtilization, err = strconv.ParseFloat(fmt.Sprintf("%.2f", pool.InnodbBufferPoolUtilization), 64)

	//MyISAM缓冲池大小(MB)
	pool.KeyBufferSize = float64(pool.KeyBlocksUsed) + float64(pool.KeyBlocksUnused)
	pool.KeyBufferSize, err = strconv.ParseFloat(fmt.Sprintf("%.2f", pool.KeyBufferSize), 64)

	//MyISAM平均使用率(%)
	pool.KeyBufferUtilization = (float64(pool.KeyBlocksUsed) / (float64(pool.KeyBlocksUsed) + float64(pool.KeyBlocksUnused))) * 100
	if pool.KeyBufferSize == 0.00 {
		pool.KeyBufferUtilization = 0.00
	}
	pool.KeyBufferUtilization, err = strconv.ParseFloat(fmt.Sprintf("%.2f", pool.KeyBufferUtilization), 64)

	//MyISAM平均写命中率(%)
	pool.KeyReadsRate = (1 - float64(pool.KeyReads)/float64(pool.KeyReadRequests)) * 100
	if pool.KeyReadRequests == 0 {
		pool.KeyReadsRate = 0.00
	}
	pool.KeyReadsRate, err = strconv.ParseFloat(fmt.Sprintf("%.2f", pool.KeyReadsRate), 64)

	//MyISAM平均写命中率(%)
	pool.KeyWritesRate = (1 - float64(pool.KeyWrites)/float64(pool.KeyWriteRequests)) * 100
	if pool.KeyWriteRequests == 0 {
		pool.KeyWritesRate = 0.00
	}
	pool.KeyWritesRate, err = strconv.ParseFloat(fmt.Sprintf("%.2f", pool.KeyWritesRate), 64)

	if err != nil {
		return nil, err
	} else {
		return &pool, nil
	}
}
