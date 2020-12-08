// 每个loader 定时设定 ms 毫秒
var each_loader_timmer = {

    total_ip: global_timer_secends,
    total_pv: global_timer_secends,

    map_chart: global_timer_secends,
    top_ip_chart : global_timer_secends ,

    status_code_chart : global_timer_secends,

    network_traffic_by_minute : global_timer_secends,
    request_ip_pv_by_minute : global_timer_secends,

    request_num_by_url : global_timer_secends,
    spider_by_ua : global_timer_secends,

}

// window.chart_load_func['total_ip']()
// window.chart_load_func['total_pv']()
// window.chart_load_func['map_chart']()
// window.chart_load_func['top_ip_chart']()
// window.chart_load_func['status_code_chart']()
// window.chart_load_func['request_ip_pv_by_minute']()
// window.chart_load_func['network_traffic_by_minute']()
// window.chart_load_func['request_num_by_url']()
// window.chart_load_func['spider_by_ua']()



loader_key = Object.keys(each_loader_timmer)
var timer = []

for (var i=0 ;i < loader_key.length ; i++){

    let func = window.chart_load_func[loader_key[i]]
    // first run
   if (!func) continue;
    func()
    timer = setInterval(function(){
                // for timer
           func()
    },each_loader_timmer[ loader_key[i] ])

}


