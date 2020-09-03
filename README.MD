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