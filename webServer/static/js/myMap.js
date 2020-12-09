//固定的深圳市
var toCityName = GEO[440300]
// 1. 实例化对象
var map_chart = echarts.init(document.querySelector(".map .chart"));
window.addEventListener("resize", function() {
  map_chart.resize();
});
window.chart_load_func['map_chart'] = function() {
  $.ajax({
    url:host + '/get_request_num_by_province',
    type:'GET',
    headers: { 'Authorization':Authorization,},
    success:function(msg){
      var api_data = msg.data
      var province_geo = GEO[100000]
      var map_lines_data = []
      var map_effectScatter_data = []
      for (var i= 0 ; i < api_data.length ;i++){
        let province_name = api_data[i]['province'].slice(0,2)
        for (var j = 0 ; j < province_geo.childrenNum ; j++){
          if ( province_geo.children[j]['name'].slice(0,2) == province_name){
            let _lines_data = {
              fromName: province_geo.children[j]['name'],
              toName: toCityName.name,
              coords: [province_geo.children[j].center, toCityName.center],
              value: api_data[i]['value']
            }

            let _effectScatter_data = {
              name :province_geo.children[j]['name'],
              value: province_geo.children[j].center.concat(api_data[i]['value'])
            }
            map_lines_data.push(_lines_data)
            map_effectScatter_data.push(_effectScatter_data)
            break
          }
        }
      }

      var color = ["#fff"]; //航线的颜色
      var series = [];
      series.push(
          {
            name: " 运动方向",
            type: "lines",
            zlevel: 1,
            effect: {
              show: true,
              period: 6,
              trailLength: 0.7,
              color: "white", //arrow箭头的颜色
              symbolSize: 3
            },
            lineStyle: {
              normal: {
                color: color[i],
                width: 0,
                curveness: 0.2
              }
            },
            data: map_lines_data
          },
          // {
          //   name: "运动方向 实心线",
          //   type: "lines",
          //   zlevel: 2,
          //   symbol: ["none", "arrow"],
          //   symbolSize: 10,
          //   effect: {
          //     show: true,
          //     period: 6,
          //     trailLength: 0,
          //     symbol: planePath,
          //     symbolSize: 15
          //   },
          //   lineStyle: {
          //     normal: {
          //       color: "#fff",
          //       width: 1,
          //       opacity: 0.6,
          //       curveness: 0.2
          //     }
          //   },
          //   data: map_lines_data
          // },
          {
            name: "effectScatter Top3",
            type: "effectScatter",
            coordinateSystem: "geo",
            zlevel: 2,
            rippleEffect: {
              brushType: "stroke"
            },
            label: {
              color:'#fff',
              normal: {
                show: true,
                position: "right",
                formatter: function(params){

                  return params.data.name +' ：' + params.data.value[2]
                }
              }
            },
            symbolSize: function(val) {
              return val[2] / 10000;
              // return  10;
            },
            itemStyle: {
              normal: {
                color: '#fff'
              },
              emphasis: {
                areaColor: "#fff"
              }
            },
            data: map_effectScatter_data
          }
      );

      var option = {

        geo: {
          map: "china",
          label: {
            show:false,
            color:"#fff",
            emphasis:{
              itemStyle: {
                areaColor: null,
                shadowOffsetX: 0,
                shadowOffsetY: 0,
                shadowBlur: 20,
                borderWidth: 0,
                shadowColor: 'rgba(0, 0, 0, 0.5)'
              }
            }
          },
          roam: false,
          //   放大我们的地图
          zoom: 1.2,
          itemStyle: {
            normal: {
              areaColor: "rgba(43, 196, 243, 0.42)",
              borderColor: "rgba(43, 196, 243, 1)",
              borderWidth: 1
            },
            emphasis: {
              areaColor: "#2f89cf"
            }
          }
        },
        series: series
      };
      map_chart.setOption(option);

    }

  })

}

