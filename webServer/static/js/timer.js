// 每个loader 定时设定 ms 毫秒
var each_loader_timmer = {

    total_ip: 10000,
    // get_total_member_load: 10000,

    map_chart: 10000,
    top_ip_chart : 20000 ,

    status_code_chart : 20000,

    request_num_by_secends : 5000,
    // weeks_member_chart_load : 30000,

    request_num_by_url : 60000,
    spider_by_ua : 60000,


}

// window.chart_load_func['map_chart']()
// window.chart_load_func['total_ip']()
// window.chart_load_func['top_ip_chart']()
// window.chart_load_func['status_code_chart']()
// window.chart_load_func['request_num_by_secends']('init')
// window.chart_load_func['request_num_by_url']()


loader_key = Object.keys(each_loader_timmer)
var timer = []

for (var i=0 ;i < loader_key.length ; i++){

    let func = window.chart_load_func[loader_key[i]]
    // first run
   if (!func) continue;

   if(loader_key[i] == 'request_num_by_secends'){
        func('init')
    }else{
        func()
    }


    timer = setInterval(function(){
                // for timer

               func()
    },each_loader_timmer[ loader_key[i] ])

}


