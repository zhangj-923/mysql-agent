package internal

import (
	"fmt"
	client "github.com/influxdata/influxdb1-client/v2"
	"mysql-agent/common"
	"time"
)

func parsePParameter(tags map[string]string, parameter *common.PParameter) []*client.Point {
	if parameter == nil {
		return nil
	}
	points := make([]*client.Point, 0)
	fileds := map[string]interface{}{
		"connection_utilization":  parameter.ConnectionUtilization,
		"created_tmp_tables":      parameter.CreatedTmpTables,
		"slow_queries":            parameter.SlowQueries,
		"threads_connected":       parameter.ThreadsConnected,
		"threads_running":         parameter.ThreadsRunning,
		"tmp_tables_utilization":  parameter.TmpTablesUtilization,
		"max_connections":         parameter.MaxConnections,
		"created_tmp_disk_tables": parameter.CreatedTmpDiskTables,
	}
	if point, err := client.NewPoint("mysql.parameter", tags, fileds, time.Now()); err == nil {
		points = append(points, point)
	} else {
		fmt.Println(err)
	}
	return points
}

func parseComTotal(tags map[string]string, total *common.CommitTotal) []*client.Point {
	if total == nil {
		return nil
	}
	points := make([]*client.Point, 0)
	comFileds := map[string]interface{}{
		"com_delete": total.ComDelete,
		"com_insert": total.ComInsert,
		"com_reads":  total.ComReads,
		"com_select": total.ComSelect,
		"com_update": total.ComUpdate,
		"com_writes": total.ComWrites,
	}
	if comPoint, err := client.NewPoint("mysql.comtotal", tags, comFileds, time.Now()); err == nil {
		points = append(points, comPoint)
	} else {
		fmt.Println(err)
	}
	monFileds := map[string]interface{}{
		"qps":       total.Monitor.Qps,
		"tps":       total.Monitor.Tps,
		"questions": total.Monitor.Questions,
		"commit":    total.Monitor.Commit,
		"rollback":  total.Monitor.Rollback,
	}
	if monPoint, err := client.NewPoint("mysql.monitor", tags, monFileds, time.Now()); err == nil {
		points = append(points, monPoint)
	} else {
		fmt.Println(err)
	}
	return points
}

func parseLockTotal(tags map[string]string, total *common.LockTotal) []*client.Point {
	if total == nil {
		return nil
	}
	points := make([]*client.Point, 0)
	fileds := map[string]interface{}{
		"table_locks_immediate": total.TableLocksImmediate,
		"table_locks_waited":    total.TableLocksWaited,
	}
	if point, err := client.NewPoint("mysql.locktotal", tags, fileds, time.Now()); err == nil {
		points = append(points, point)
	} else {
		fmt.Println(err)
	}
	return points
}

func parseNetTotal(tags map[string]string, total *common.NetworkTatal) []*client.Point {
	if total == nil {
		return nil
	}
	points := make([]*client.Point, 0)
	fileds := map[string]interface{}{
		"bytes_received": total.BytesReceived,
		"bytes_sent":     total.BytesSent,
	}
	if point, err := client.NewPoint("mysql.nettotal", tags, fileds, time.Now()); err == nil {
		points = append(points, point)
	} else {
		fmt.Println(err)
	}
	return points
}

func parsePool(tags map[string]string, pool *common.Pool) []*client.Point {
	if pool == nil {
		return nil
	}
	points := make([]*client.Point, 0)
	fileds := map[string]interface{}{
		"innodb_buffer_pool_instances":     pool.InnodbBufferPoolInstances,
		"innodb_buffer_pool_reads":         pool.InnodbBufferPoolReads,
		"innodb_buffer_pool_read_requests": pool.InnodbBufferPoolReadRequests,
		"innodb_buffer_pool_reads_hit":     pool.InnodbBufferPoolReadsHit,
		"innodb_buffer_pool_size":          pool.InnodbBufferPoolSize,
		"innodb_buffer_pool_pages_free":    pool.InnodbBufferPoolPagesFree,
		"innodb_buffer_pool_pages_total":   pool.InnodbBufferPoolPagesTotal,
		"innodb_buffer_pool_utilization":   pool.InnodbBufferPoolUtilization,
		"key_blocks_used":                  pool.KeyBlocksUsed,
		"key_blocks_unused":                pool.KeyBlocksUnused,
		"key_buffer_size":                  pool.KeyBufferSize,
		"key_buffer_utilization":           pool.KeyBufferUtilization,
		"key_reads":                        pool.KeyReads,
		"key_read_requests":                pool.KeyReadRequests,
		"key_reads_rate":                   pool.KeyReadsRate,
		"key_writes":                       pool.KeyWrites,
		"key_write_requests":               pool.KeyWriteRequests,
		"key_writes_rate":                  pool.KeyWritesRate,
	}

	if point, err := client.NewPoint("mysql.pool", tags, fileds, time.Now()); err == nil {
		points = append(points, point)
	} else {
		fmt.Println(err)
		common.Error.Println(err)
	}
	return points
}
