# 执行流程
    main.py 
        1: 开启webserver 进入设置界面
            
            -> 选择要监控的web 服务器  
                    web_server_type = options  [nginx ,apache ,tomact,other]
            -> 输入配置文件的所在的路径
                    web_server_conf_path = input 日志绝对路径
            -> 配置想要的日志格式 
                    web_server_log_format = tags 拖拽组件 拖拽编辑 / 排序 后 保存
            -> 点击保存 / 开始监控
            
            
        2: 调用指定的 module 验证module 的合法性
        3: 
        
        
    client 客户端 负责读取日志内容 并上报给 server
    
    server 服务端 监听 N个 client 端 , 负责处理 N个 clien 上报的数据 进行 一系列操作 比如 : 持久化存储,看板的数据输出
            
        process 1 
            msg = {client_host:'127.0.0.1:2666',client_pid:'66',position:'123',file:'/www/wwwlogs/a.log',msg:'asdasdasd'}
            
            收到 client 端口 上报数据的时候 同时记录 client端口文件读取当前的position (当client 断开或者 重启的时候 会继续 上一次position 继续读取文件)
        process 2
            统计各个client 端口来的数据供看板 调取数据 进行可视化
        process 3
            监听各个 client 的存活情况 
        