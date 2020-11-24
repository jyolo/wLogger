// 每个loader 定时设定 ms 毫秒
var each_loader_timmer = {

    // total_ip: 10000,
    // total_pv: 10000,


    // map_chart: 10000,
    // top_ip_chart : 20000 ,
    //
    // status_code_chart : 20000,
    //
    // pv_num_by_minute : 2000,
    // request_ip_by_minute : 2000,
    //
    // request_num_by_url : 60000,
    // spider_by_ua : 60000,


}

window.chart_load_func['total_ip']()
window.chart_load_func['total_pv']()
window.chart_load_func['map_chart']()
window.chart_load_func['top_ip_chart']()
window.chart_load_func['status_code_chart']()
window.chart_load_func['request_pv_by_minute']()
window.chart_load_func['request_ip_by_minute']()
window.chart_load_func['request_num_by_url']()
window.chart_load_func['spider_by_ua']()



loader_key = Object.keys(each_loader_timmer)
var timer = []

for (var i=0 ;i < loader_key.length ; i++){

    let func = window.chart_load_func[loader_key[i]]
    // first run
   if (!func) continue;

    timer = setInterval(function(){
                // for timer
           func()
    },each_loader_timmer[ loader_key[i] ])

}


