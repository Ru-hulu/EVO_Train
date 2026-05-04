start：
      python3 server_tcp/server_connection.py --host 0.0.0.0 --port 9000 --workers 4
structure：
                              ┌────────────────────┐
                                      Client
                             roboclaw 训练中心-在线训练
                                JSON request + \n  
                              └─────────┬──────────┘
                                        │ TCP 长连接
                                        ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     server_connection.py                              
│               reactor-actor 模型响应用户链接/请求                                                                                     
│  1. 监听端口 9000                                                    
│  2. accept 客户端连接                                                
│  3. selector/epoll 管理 socket 事件，建立链接 / 响应任务              
│  4. 维护长连接 idle timeout                                           
│  5. 生成 TrainTaskEvent，交给线程池                                   
└───────────────────────────────┬──────────────────────────────────────┘
                                │ submit(event)
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         thread_pool.py                              
│                  线程池，启动 4/8 个 worker 线程                                                                                   
│  1. 维护任务队列，线程消费 train_task_queue                          
│  2. worker 消费 TrainTaskEvent                                       
│  3. 调用业务函数 handle_request 处理请求                              
└───────────────────────────────┬──────────────────────────────────────┘
                                │ 数据库管理用户任务信息 sql_xxx()
                                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          sql_pack.py                                                                                                      
│  1. SQLite 初始化                                                    
│  2. sql_get_user_all_task    获取一个用户的所有任务                  
│  3. sql_add_user_task        为一个用户添加任务                       
│  4. sql_delete_user_task     删除一个用户的某个任务                   
│  5. 数据持久化到：/Users/hongru/project/EVO_Train/sql_lite_data
└─────────────────────────────────────────────────────────────────────┘

logging：
      所有 log 通过 evo_train_logging 包统一输出，线程安全 (QueueHandler/
      QueueListener)。每条 log 带 client_id / worker_id / event_id 三个
      extra 字段，可按 event_id grep 出一条请求的全生命周期：
        accept → read → submit → handling → finished → response → close

      默认输出：
        console (stderr)  人类可读，开发调试用
        ./logs/server.log JSON Lines, 100MiB × 5 滚动，生产用

      环境变量覆盖默认：
        EVO_LOG_DIR             默认 ./logs
        EVO_LOG_LEVEL           DEBUG/INFO/WARNING/ERROR，默认 INFO
        EVO_LOG_FORMAT_CONSOLE  human (默认) | json
        EVO_LOG_FORMAT_FILE     json (默认)  | human

      跑测试：
        python3 -m unittest discover tests