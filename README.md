# mysql_agent 使用说明：  
1、mysql\_采集器：  
mysql_agent 是采集器启动脚本，具体命令：  
./mysql_agent  
无法启动： chmod +x mysql_agent 再次执行  

# 必须修改的配置：  
1）config.json  

### 模拟数据模式开关  
"isMock": true 模拟； false 正常  
### 是否采用国密加密  
"isEncrypt": true 加密； false 未加密  
### agentCycle 连接信息  
"agentCycle" 采集周期  
### MYSQL 连接信息  
"mysql" iCop 的存储使用的 mysql 连接信息  
### influxdb 连接信息    
"influxdb" iCop 存储使用的 influxdb 连接信息  

# 非必须修改的配置：  

1、mysql\_资产验证器：  
mysql_service 是采集器启动脚本，具体命令：  
./mysql_service  
无法启动： chmod +x mysql_service 再次执行  
非必须修改的配置：  
1.service_port : 验证连接自定义端口，与 tomcat 配置对应  

## PS:告警行为均依赖 ireport 组件，务必确认 ireport 地址以及服务状态  

