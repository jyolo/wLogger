window.host = 'http://127.0.0.1:5000';
window.Authorization = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpc3MiOiJsb2NhbC5rcGkuY29tIiwiaWF0IjoxNTkxNjg1OTA1LCJzdWIiOiJ3ZWIiLCJhdWQiOiJBcGFjaGUtSHR0cENsaWVudFwvNC41LjYgKEphdmFcLzEuOC4wXzIwMi1yZWxlYXNlKSIsImV4cCI6MTYyMzIyMTkwNSwidWlkIjo4NCwibmlja25hbWUiOiJ3ZWljb25nIn0.fX1RrO4aOJ-v0QKQ4lSBcjIWyDzzl4F96yDv7_aySHnacUU-4VZbYeec4804-iJBLBmWcM3YheoO-XFqyY9ffdQTjNfobD9WiYPBNJBJAooSQJMOo2H7mwJrXgPTGlFEds1rpXfGHEH2yl7SidPwa4Hq4itR6B1aJOdEY23-8GU'
window.global_timer_secends = 20000 // 全局定时器 20秒
window.chart_load_func = []

//total ip today
window.chart_load_func['total_ip'] = function(){
    $.ajax({
        url: host + '/get_total_ip',
        type:'GET',
        async:true,
        headers: { 'Authorization':Authorization,},
        success:function(msg){
            let total_ip = msg.data['total_num'].toString().split('.')[0]
            $('.total_ip').html(total_ip)

        }

    })
}


//total_member
// window.chart_load_func['get_total_member_load']  = function(){
//     $.ajax({
//         url: host + '/v1/screen/get_member_total',
//         type:'GET',
//         async:true,
//         headers: { 'Authorization':Authorization,},
//         success:function(msg){
//             let num = msg.data.toString().split('.')[0]
//             $('.total_member').html(num)
//
//         }
//     })
// }

function timestampToTime(timestamp) {
  var date = new Date(timestamp);//时间戳为10位需*1000，时间戳为13位的话不需乘1000
  var Y = date.getFullYear() + '-';
  var M = (date.getMonth()+1 < 10 ? '0'+(date.getMonth()+1) : date.getMonth()+1) + '-';
  var D = (date.getDate() < 10 ? '0'+date.getDate() : date.getDate()) + ' ';
  var h = (date.getHours() < 10 ? '0'+date.getHours() : date.getHours()) + ':';
  var m = (date.getMinutes() < 10 ? '0'+date.getMinutes() : date.getMinutes()) + ':';
  var s = (date.getSeconds() < 10 ? '0'+date.getSeconds() : date.getSeconds());

  // let strDate = Y+M+D+h+m+s;
  let strDate = h+m+s;
　
  return strDate;
}


