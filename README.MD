# 介绍
> nlog
>> 功能
    
    采集器 inputer
    
    * 实时日志采集,支持多个web日志同时采集 
    * 可指定日志文件大小或指定时间,自动切割文件到指定目录, 虚拟机 ab实测2W并发发下切割日志不丢数据.
    * 可自定义队列服务软件,接受采集的日志信息. 已内置redis 如需kafka ,mq 等其它队列可自行拓展
    * 极低的cpu内存占用 ,低配小主机也能愉快的玩耍
    
    
    解析存储器 outputer
    
    * 实时解析日志并存储到指定的数据库, 已内置 mysql 和 mongodb 如需使用elastic全家桶或其它存储引擎 可自行拓展
    * 采集器,解析器,web服务均可 独立分布到不同的服务器节点上运行
    * 目前已内置 nginx 解析器, 可随意指定日志格式, 只需在配置文件里面指定格式名称即可正确解析并存储
    * 支持按日期天,周,月,年. 自动分表或集合存储日志
    * 支持指定工作进程来快速消费队列数据,大流量也能实时解析并存储日志, 虚拟机中ab 实测2W并发延迟小于1秒
    * 注: 当海量流量来的时候发现解析存储延迟过高的情况,可将解析器部署到集群中其它多个节点同时消费队列数据,提升解析存储效率
    
    
    web服务 web
    
    * 已内置大屏监控web面板,流量情况一目了然
    * 同时支持 mysql 或者 mongodb 作为 解析器的存储引擎
 
    * 简单配置 开箱即用
    * 无入侵 低占用 高拓展
    * 高拓展 采集器 已内置 mysql 和 mongodb 解析存储器 已内置 mysql 和 mongodb 
    
    
    
# 快速开始
> 安装拓展
     
    pip3 install -r requirements.txt 
    
> 启动 采集器
    
     python3 main.py -run inputer
     
> 启动 解析存储器

    python3 main.py -run inputer
    
> 启动 web服务

    python3 main.py -run web
    
   * 以上三个应用均可单独启用
    
   


# 配置详解

> 公共配置
    
    [nginx]                                         # 当 input 和 outputer 中指定了 server_type = nginx 才需此配置  
    pid_path = /www/server/nginx/logs/nginx.pid     # 指定nginx.pid 的绝对路径       
    server_conf = /www/server/nginx/conf/nginx.conf # 指定nginx配置文件的路径   
    
    [redis]                                         # 当inputer 和 outputer 中指定了 queue = redis 才需此配置      
    host = 127.0.0.1
    port = 6379
    password = xxxxxxxx
    db = 1
    prefix = logger_watch:

    [mysql]                                         # 当 outputer 中指定了 save_engine = mysql 才需此配置     
    host = 127.0.0.1
    port = 3306
    username = nginx_logger
    password = xxxxxxxx
    db = nginx_logger
    table = logger
    split_save = day                                # 当有该配置项则代表开启自动分表 目前支持按 天，周，月，年 ；参数：[day, week, month ,year] ,进行存储
    
    [mongodb]                                       # 当 outputer 中指定了 save_engine = mongodb 才需此配置 
    host = 192.168.0.166
    port = 7002
    username = logger_watcher
    password = xxxxxxxx
    db = nginx_logger
    collection = logger
    split_save = day                                # 当有该配置项则代表开启自动分集合 目前支持按 天，周，月，年 ；参数：[day, week, month ,year] ,进行存储
    

> 日志采集端 配置

    [inputer]
    log_debug = True                       # 开启日志debug模式
    node_id = server_80                    # 当前节点ID 唯一
    queue = redis                          # 队列配置 目前内置了 [redis , mongodb]
    queue_name = queue_logger              # 队列 key 的名称
    max_batch_push_queue_size = 5000       # 每次最多批量插入队列多少条数据
    max_retry_open_file_time = 10          # 当文件读取失败之后重新打开日志文件，最多重试多少次
    max_retry_reconnect_time = 20          # 连接队列失败的时候，最多重试多少次
    
    [inputer.log_file.web1]
    server_type = nginx                    # 服务器应用目前只支持 nginx
    file_path = /wwwlogs/ww.aaa.com.log    # 日志绝对路径
    log_format_name = online               # 配置文件中 日志名称 example : "access_log  /www/wwwlogs/xxx.log online;" 中的 `online` 则代表启用的日志配置名称
    read_type = tail                       # 读取文件方式 支持 tail 从末尾最后一行开始 ; head 从头第一行开始 * 当文件较大的时候 建议使用 tail 
    cut_file_type = filesize               # 切割文件方式 支持 filesize 文件大小单位M ;time 指定当天时间 24:00
    cut_file_point = 200                   # 切割文件条件节点 当 filesize 时 200 代表200M 切一次  ; 当 time 时 24:00 代表今天该时间 切一次 
    cut_file_save_dir = /wwwlogs/cut_file/ # 日志切割后存储绝对路径
    
    
    [inputer.log_file.web2]                # 支持同时采集多个应用日志 追加配置即可
    ..........................
    
             

> 日志解析输出端

    [outputer]
    log_debug = True                      # 开启日志debug模式
    save_engine = mongodb                 # 解析后的日志存储引擎目前支持 [mysql,mongodb]
    queue = redis                         # 队列引擎 此处需要和 inputer 采集端保持一致
    queue_name = queue_logger             # 队列中 key 或 collection 集合的名称  此处需要和 inputer 采集端保持一致
    server_type = nginx                   # 服务器的类型 
    worker_process_num = 1                # 指定工作进程数量 
    max_batch_insert_db_size = 1          # 最多每次批量写入存储引擎的数量
    max_retry_reconnect_time = 200        # 连接存储引擎失败后，最多重试连接次数
    
> 大屏监控端

    [web]
    env = development                     # 是否开启调试模式 development | production
    debug = True                          # 是否开启 debug
    secret_key = xxxx                     # flask session key 
    server_name = 127.0.0.1:5000          # 指定域名和端口
    data_engine = mysql                   # 指定日志存储引擎

