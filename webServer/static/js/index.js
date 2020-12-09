/*********************left start********************************/

// 请求量最高的IP TOP50 ------------------------------- start -------------------------------
var top_ip_chart = echarts.init(document.querySelector(".bar .chart"));
window.addEventListener("resize", function () {
  top_ip_chart.resize();
});
window.chart_load_func['top_ip_chart'] = function () {

  $.ajax({
    url: host + '/get_request_num_by_ip',
    type:'GET',
    async:true,
    headers: { 'Authorization':Authorization,},
    success:function(msg){
      data = msg.data
      var xdata = []
      var ydata = []
      for (var i=0; i< data.length ;i++){
        xdata.push(data[i]['ip'])
        ydata.push(data[i]['total_num'])
      }
      // 指定配置和数据
      var option = {
        color: ["#2f89cf"],
        tooltip: {
          trigger: "axis",
          axisPointer: {
            // 坐标轴指示器，坐标轴触发有效
            type: "line" // 默认为直线，可选为：'line' | 'shadow'
          }
        },
        grid: {
          left: "0%",
          top: "10px",
          right: "0%",
          bottom: "4%",
          containLabel: true
        },
        xAxis: [
          {
            type: "category",
            data: xdata,
            axisTick: {
              alignWithLabel: true
            },
            axisLabel: {
              textStyle: {
                color: "rgba(255,255,255,.6)",
                fontSize: "12"
              }
            },
            axisLine: {
              show: true
            }
          }
        ],
        yAxis: [
          {
            type: "value",
            axisLabel: {
              textStyle: {
                color: "rgba(255,255,255,.6)",
                fontSize: "12"
              }
            },
            axisLine: {
              lineStyle: {
                color: "rgba(255,255,255,.1)"
                // width: 1,
                // type: "solid"
              }
            },
            splitLine: {
              lineStyle: {
                color: "rgba(255,255,255,.1)"
              }
            }
          }
        ],
        series: [
          {
            name: "IP",
            type: "bar",
            barWidth: "35%",
            // data: [200, 300, 300, 900, 1500, 1200, 600],
            data: ydata,
            itemStyle: {
              barBorderRadius: 5
            }
          }
        ]
      };

      // 把配置给实例对象
      top_ip_chart.setOption(option);
    }
  })



}
// 请求量最高的IP TOP50 ------------------------------- end -------------------------------


// 最近10分钟每分钟流量 ------------------------------- start -------------------------------
var network_traffic_by_minute = echarts.init(document.querySelector(".line .chart"));
window.addEventListener("resize", function () {
  network_traffic_by_minute.resize();
});
window.chart_load_func['network_traffic_by_minute']  = function (type = null) {
  if(type == 'init'){
    __url =  host + '/get_network_traffic_by_minute?type=init'
  }else {
    __url =  host + '/get_network_traffic_by_minute'
  }
  $.ajax({
    url:__url,
    type:'GET',
    async:true,
    headers: { 'Authorization':Authorization,},
    success:function(msg){
      data = msg.data

      secends_values = {
        'in_network':[],
        'out_network':[],
        'xAxis':[]
      }
      secends_xAxis = []
      $.each(data,function(k,v){
        secends_values['in_network'].push(Math.round(v['in_network'],0))
        secends_values['out_network'].push(Math.round(v['out_network'],0))
        secends_values['xAxis'].push(timestampToTime(v['time_str']))
      })

      // 2. 指定配置和数据
      var option = {
        color: ["#00f2f1", "#ed3f35"],
        tooltip: {
          // 通过坐标轴来触发
          trigger: "axis"
        },
        legend: {
          // 距离容器10%
          right: "10%",
          // 修饰图例文字的颜色
          textStyle: {
            color: "#4c9bfd"
          }
          // 如果series 里面设置了name，此时图例组件的data可以省略
          // data: ["邮件营销", "联盟广告"]
        },
        grid: {
          top: "20%",
          left: "3%",
          right: "4%",
          bottom: "3%",
          show: true,
          borderColor: "#012f4a",
          containLabel: true
        },

        xAxis: {
          type: "category",
          boundaryGap: false,
          data: secends_values['xAxis'],
          // 去除刻度
          axisTick: {
            show: false
          },
          // 修饰刻度标签的颜色
          axisLabel: {
            color: "rgba(255,255,255,.7)"
          },
          // 去除x坐标轴的颜色
          axisLine: {
            show: false
          }
        },
        yAxis: {
          type: "value",
          // 去除刻度
          axisTick: {
            show: false
          },
          // 修饰刻度标签的颜色
          axisLabel: {
            color: "rgba(255,255,255,.7)"
          },
          // 修改y轴分割线的颜色
          splitLine: {
            lineStyle: {
              color: "#012f4a"
            }
          }
        },
        series: [
          {
            name:'入网 KB',
            type: "line",
            // stack: "总量",
            smooth: true,
            data: secends_values['in_network']
          },
          {
            name:'出网 KB',
            type: "line",
            // stack: "总量",
            smooth: true,
            data: secends_values['out_network']
          }
        ]
      };
      // 3. 把配置和数据给实例对象
      network_traffic_by_minute.setOption(option);
    }


  })



}
// 最近10分钟pv ------------------------------- end -------------------------------


// 非200状态码 二级分类  ------------------------------- start -------------------------------
var status_code_chart = echarts.init(document.querySelector(".bar1 .chart"));
window.addEventListener("resize", function () {
  status_code_chart.resize();
});
window.chart_load_func['status_code_chart']  = function () {
  $.ajax({
    url: host + '/get_request_num_by_status',
    type:'GET',
    async:true,
    headers: { 'Authorization':Authorization,},
    success:function(msg){
      var data = [];
      var titlename = [];
      var valdata = [];
      var maxRang = []
      var total = 0
      $.each(msg.data ,function(k,v){
        total = total + v.total_num
        valdata.push(v.total_num)
        titlename.push('状态码：' + v.status_code.toString())
      })

      $.each(msg.data ,function(k,v){
        percent = (v.total_num / total) * 100
        data.push(percent.toFixed(2))
        maxRang.push(100)
      })



      var myColor = ["#1089E7", "#F57474", "#56D0E3", "#F8B448", "#8B78F6"];
      option = {
        //图标位置
        grid: {
          top: "10%",
          left: "22%",
          bottom: "10%"
        },
        xAxis: {
          show: false
        },
        yAxis: [
          {
            show: true,
            data: titlename,
            inverse: true,
            axisLine: {
              show: false
            },
            splitLine: {
              show: false
            },
            axisTick: {
              show: false
            },
            axisLabel: {
              color: "#fff",
              rich: {
                lg: {
                  backgroundColor: "#339911",
                  color: "#fff",
                  borderRadius: 15,
                  // padding: 5,
                  align: "center",
                  width: 15,
                  height: 15
                }
              }
            }
          },
          {
            show: true,
            inverse: true,
            data: valdata,
            axisLabel: {
              textStyle: {
                fontSize: 12,
                color: "#fff"
              }
            }
          }
        ],
        series: [
          {
            name: "条",
            type: "bar",
            yAxisIndex: 0,
            data: data,
            barCategoryGap: 50,
            barWidth: 10,
            itemStyle: {
              normal: {
                barBorderRadius: 20,
                color: function (params) {
                  var num = myColor.length;
                  return myColor[params.dataIndex % num];
                }
              }
            },
            label: {
              normal: {
                show: true,
                position: "right",
                formatter: "{c}%"
              }
            }
          },
          {
            name: "框",
            type: "bar",
            yAxisIndex: 1,
            barCategoryGap: 50,
            data: maxRang,
            barWidth: 15,
            itemStyle: {
              normal: {
                color: "none",
                borderColor: "#00c1de",
                borderWidth: 3,
                barBorderRadius: 15
              }
            }
          }
        ]
      };

      // 使用刚指定的配置项和数据显示图表。
      status_code_chart.setOption(option);


    }

  })
}
// 非200状态码 二级分类  ------------------------------- end -------------------------------


// 最近10分钟IP / PV ------------------------------- start -------------------------------
var request_ip_pv_by_minute = echarts.init(document.querySelector(".member .chart"));
window.addEventListener("resize", function () {
  request_ip_pv_by_minute.resize();
});
window.chart_load_func['request_ip_pv_by_minute'] = function () {
  $.ajax({
    url: host + '/get_ip_pv_num_by_minute',
    type:'GET',
    async:true,
    headers: { 'Authorization':Authorization,},
    success:function(msg){
      data = msg.data


      nums = {
        'ip_num':[],
        'pv_num':[]
      }
      timestamp = []
      $.each(data,function(k,v){

        nums['ip_num'].push(v['ip_num'])
        nums['pv_num'].push(v['pv_num'])
        timestamp.push(timestampToTime(v['time_str']))
      })



      option = {
        tooltip: {
          trigger: "axis",
          axisPointer: {
            lineStyle: {
              color: "#dddc6b"
            }
          }
        },
        legend: {
          top: "0%",
          textStyle: {
            color: "rgba(255,255,255,.5)",
            fontSize: "12"
          }
        },
        grid: {
          left: "10",
          top: "30",
          right: "10",
          bottom: "10",
          containLabel: true
        },

        xAxis: [
          {
            type: "category",
            boundaryGap: false,
            axisLabel: {
              textStyle: {
                color: "rgba(255,255,255,.6)",
                fontSize: 12
              }
            },
            axisLine: {
              lineStyle: {
                color: "rgba(255,255,255,.2)"
              }
            },

            data: timestamp
          },
          {
            axisPointer: { show: false },
            axisLine: { show: false },
            position: "bottom",
            offset: 20
          }
        ],
        yAxis: [
          {
            type: "value",
            axisTick: { show: false },
            axisLine: {
              lineStyle: {
                color: "rgba(255,255,255,.1)"
              }
            },
            axisLabel: {
              textStyle: {
                color: "rgba(255,255,255,.6)",
                fontSize: 12
              }
            },

            splitLine: {
              lineStyle: {
                color: "rgba(255,255,255,.1)"
              }
            }
          }
        ],
        series: [
          {
            name: "每分钟IP数",
            type: "line",
            smooth: true,
            symbol: "circle",
            symbolSize: 5,
            showSymbol: true,
            lineStyle: {
              normal: {
                color: "#00d887",
                width: 2
              }
            },
            areaStyle: {
              normal: {
                color: new echarts.graphic.LinearGradient(
                    0,
                    0,
                    0,
                    1,
                    [
                      {
                        offset: 0,
                        color: "rgba(0, 216, 135, 0.4)"
                      },
                      {
                        offset: 0.8,
                        color: "rgba(0, 216, 135, 0.1)"
                      }
                    ],
                    false
                ),
                shadowColor: "rgba(0, 0, 0, 0.1)"
              }
            },
            itemStyle: {
              normal: {
                color: "#00d887",
                borderColor: "rgba(221, 220, 107, .1)",
                borderWidth: 12
              }
            },
            data: nums['ip_num']
          },
          {
            name: "每分钟PV数",
            type: "line",
            smooth: true,
            symbol: "circle",
            symbolSize: 5,
            showSymbol: true,
            lineStyle: {
              normal: {
                color: "#006cff",
                width: 2
              }
            },
            areaStyle: {
              normal: {
                color: new echarts.graphic.LinearGradient(
                    0,
                    0,
                    0,
                    1,
                    [
                      {
                        offset: 0,
                        color: "rgba(0, 216, 135, 0.4)"
                      },
                      {
                        offset: 0.8,
                        color: "rgba(0, 216, 135, 0.1)"
                      }
                    ],
                    false
                ),
                shadowColor: "rgba(0, 0, 0, 0.1)"
              }
            },
            itemStyle: {
              normal: {
                color: "#006cff",
                borderColor: "rgba(221, 220, 107, .1)",
                borderWidth: 12
              }
            },
            data: nums['pv_num']
          }
        ]
      };
      // 使用刚指定的配置项和数据显示图表。
      request_ip_pv_by_minute.setOption(option);
    }

  })
}
// 最近10分钟IP ------------------------------- end -------------------------------


// 热门接口URL请求TOP 10分布 ------------------------------- start -------------------------------
var request_num_by_url = echarts.init(document.querySelector(".pie .chart"));
window.addEventListener("resize", function () {
  request_num_by_url.resize();
});
window.chart_load_func['request_num_by_url'] = function () {
  $.ajax({
    url: host + '/get_request_num_by_url',
    type:'GET',
    async:true,
    headers: { 'Authorization':Authorization,},
    success:function(msg){
      data = msg.data
      range_keys = []
      range_values = []
      $.each(data ,function(k,v){
        range_keys.push(v['request_url'])
        range_values.push({value:v['total_num'] ,name:v['request_url']})
      })
      option = {
        tooltip: {
          trigger: "item",
          formatter: "{a} <br/>{b}: {c} ({d}%)",
          position: function (p) {
            //其中p为当前鼠标的位置
            return [p[0] + 10, p[1] - 10];
          }
        },
        legend: {
          top: "90%",
          itemWidth: 10,
          itemHeight: 10,
          data: range_keys,
          textStyle: {
            color: "rgba(255,255,255,.5)",
            fontSize: "12"
          }
        },
        series: [
          {
            name: "接口分布",
            type: "pie",
            center: ["50%", "42%"],
            radius: ["40%", "60%"],
            color: [
            "#006cff",
            "#60cda0",
            "#ed8884",
            "#ff9f7f",
            "#0096ff",
            "#9fe6b8",
            "#32c5e9",
            "#1d9dff"
            ],
            label: { show: true },
            labelLine: { show: true },
            data:range_values
          }
        ]
      };

      // 使用刚指定的配置项和数据显示图表。
      request_num_by_url.setOption(option);


    }

  })
}
// 热门接口URL请求TOP 10 ------------------------------- end -------------------------------


// 搜索引擎蜘蛛占比 ------------------------------- start -------------------------------
var spider_by_ua = echarts.init(document.querySelector(".pie1  .chart"));
window.addEventListener("resize", function () {
  spider_by_ua.resize();
});
window.chart_load_func['spider_by_ua'] = function () {
  $.ajax({
    url: host + '/get_spider_by_ua',
    type:'GET',
    async:true,
    headers: { 'Authorization':Authorization,},
    success:function(msg){
      data = msg.data

      company_data = [ ]

      $.each(data,function(k,v){
        company_data.push({value:v['total_num'] , name:v['ua']})
      })

      var option = {
        legend: {
          top: "90%",
          itemWidth: 10,
          itemHeight: 10,
          textStyle: {
            color: "rgba(255,255,255,.5)",
            fontSize: "12"
          }
        },
        tooltip: {
          trigger: "item",
          formatter: "{a} <br/>{b} : {c} ({d}%)"
        },
        // 注意颜色写的位置
        color: [
          "#006cff",
          "#60cda0",
          "#ed8884",
          "#ff9f7f",
          "#0096ff",
          "#9fe6b8",
          "#32c5e9",
          "#1d9dff"
        ],
        series: [
          {
            name: "前十占比",
            type: "pie",
            // 如果radius是百分比则必须加引号
            radius: ["10%", "70%"],
            center: ["50%", "42%"],
            // roseType: "radius",
            data: company_data,
            // 修饰饼形图文字相关的样式 label对象
            label: {
              fontSize: 10
            },
            // 修饰引导线样式
            labelLine: {
              // 连接到图形的线长度
              length: 10,
              // 连接到文字的线长度
              length2: 10
            }
          }
        ]
      };
      // 3. 配置项和数据给我们的实例化对象
      spider_by_ua.setOption(option);
    }
  })

}
// 搜索引擎蜘蛛占比 ------------------------------- end -------------------------------