import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Map

# 数据可视化
def vision(Li):
    print('正在导出数据')
    # 得到各个地点的数据
    df = pd.DataFrame(Li)
    # 只选用省份、城市、ip三个属性，构造dataframe
    df = df[['pro', 'city', 'ip']]
    df = df.dropna()
    # 计算数据，各个ip所在的省份，然后统计数量
    data = pd.pivot_table(df, values='ip', index='pro', aggfunc='count').reset_index()

    province = []
    v = []
    _max = 0
    # 由于存在一些地点的名字和接口不一样，所以进行修改，例如“新疆”改为“新疆维吾尔族自治区”
    for i in range(len(data)):
        if data['pro'][i] != '':
            # provice.append(data['city'][i])
            # 组装每个省份和确诊人数为元组，并各个省的数据都封装入列表内
            # 处理省份不匹配问题
            province_name = data['pro'][i]
            if province_name == "新疆":
                province_name = "新疆维吾尔自治区"
            elif province_name == "广西":
                province_name = "广西壮族自治区"
            elif province_name == "宁夏":
                province_name = "宁夏回族自治区"
            elif province_name in ["内蒙古", "西藏"]:
                province_name = province_name + "自治区"
            elif province_name in ["北京", "天津", "重庆", "上海"]:
                province_name = province_name + "市"
            elif province_name in ["香港", "澳门"]:
                province_name = province_name + "特别行政区"
            province.append(province_name)
            v.append(data['ip'][i])
            _max = max(_max, data['ip'][i])
    # 将结果进行保存
    values = [int(value) for value in v]
    print(province)
    print(values)
    # 绘图相关
    c = Map()
    # 传入 省份、数值参数，地图选用china
    c.add("ip分布", [list(z) for z in zip(province, values)], "china")
    c.set_global_opts(
        title_opts=opts.TitleOpts(title="ip分布情况--省"),
        # visualmap_opts=opts.VisualMapOpts(max_=int(_max)),
        visualmap_opts=opts.VisualMapOpts(max_=int(70)),
    )

    c.width = '1200px'
    c.height = '900px'
    c.render('1000_ip分布_省.html')