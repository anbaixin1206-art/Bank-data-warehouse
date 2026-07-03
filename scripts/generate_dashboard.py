#!/usr/bin/env python3
"""
银行管理驾驶舱 — 静态 HTML 生成器
使用 PySpark 查询 Hive → 生成 ECharts 自包含 HTML
"""
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DashboardGenerator") \
    .master("local[*]") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .config("hive.metastore.uris", "thrift://localhost:9083") \
    .enableHiveSupport() \
    .getOrCreate()

TD = '2026-06-14'
print(f"Querying Hive for date: {TD}...")

def query(sql):
    return spark.sql(sql.replace("${TD}", TD))

# ============================================================
# 1. 核心 KPI
# ============================================================
kpi_rows = query(f"""
    SELECT kpi_code, kpi_name, kpi_value, kpi_unit
    FROM bank_ads.ads_mgmt_kpi WHERE dt='{TD}'
""").collect()

kpi_data = {}
for r in kpi_rows:
    kpi_data[r.kpi_code] = {"name": r.kpi_name, "value": float(r.kpi_value), "unit": r.kpi_unit}

def kpi(code, fmt='number'):
    val = kpi_data.get(code, {}).get('value', 0)
    if fmt == 'yi':
        return f"{val/100000000:,.2f}亿"
    elif fmt == 'wan':
        return f"{val/10000:,.2f}万"
    return f"{val:,.0f}"

# ============================================================
# 2. 客户 AUM 分布
# ============================================================
aum_rows = query(f"""
    SELECT
        CASE
            WHEN total_asset_amt >= 10000000 THEN '1000万+'
            WHEN total_asset_amt >= 5000000  THEN '500-1000万'
            WHEN total_asset_amt >= 1000000  THEN '100-500万'
            WHEN total_asset_amt >= 500000   THEN '50-100万'
            WHEN total_asset_amt >= 100000   THEN '10-50万'
            WHEN total_asset_amt >= 10000    THEN '1-10万'
            ELSE '1万以下'
        END AS bucket,
        COUNT(1) AS cnt
    FROM bank_dws.dws_cust_daily_summary WHERE dt='{TD}'
    GROUP BY 1 ORDER BY MIN(total_asset_amt)
""").collect()
aum_data = [{"bucket": r.bucket, "cnt": r.cnt} for r in aum_rows]

# ============================================================
# 3. 交易小时趋势
# ============================================================
hourly_rows = query(f"""
    SELECT CAST(HOUR(trans_time) AS INT) AS h, COUNT(1) AS cnt
    FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
    GROUP BY HOUR(trans_time) ORDER BY h
""").collect()
hourly = [(r.h, r.cnt) for r in hourly_rows]

# ============================================================
# 4. 渠道分布
# ============================================================
ch_rows = query(f"""
    SELECT channel, COUNT(1) AS cnt
    FROM bank_dwd.dwd_sat_transaction WHERE dt='{TD}'
    GROUP BY channel ORDER BY cnt DESC
""").collect()
channels = [{"name": r.channel, "cnt": r.cnt} for r in ch_rows]

# ============================================================
# 5. 账户类型
# ============================================================
acct_rows = query(f"""
    SELECT a.ACCT_TYPE, COUNT(1) AS cnt
    FROM bank_ods.ods_core_t_account a
    WHERE a.dt='{TD}' GROUP BY a.ACCT_TYPE ORDER BY cnt DESC
""").collect()
acct_types = [{"type": r.ACCT_TYPE, "cnt": r.cnt} for r in acct_rows]

# ============================================================
# 6. Top 20 客户
# ============================================================
top_rows = query(f"""
    SELECT si.cust_name, cs.total_asset_amt
    FROM bank_dws.dws_cust_daily_summary cs
    JOIN bank_dwd.dwd_sat_customer_info si ON cs.customer_hash_key = si.customer_hash_key
    WHERE cs.dt='{TD}' AND si.is_current = TRUE
    ORDER BY cs.total_asset_amt DESC LIMIT 20
""").collect()
top_cust = [{"name": (r.cust_name[:3] + "**" if r.cust_name else "***"), "amt": float(r.total_asset_amt)} for r in top_rows]

spark.stop()
print("Data loaded. Generating HTML...")

# ============================================================
# 7. 生成 HTML
# ============================================================
html = f'''<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>银行数据驾驶舱 — {TD}</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js"></script>
<style>
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:'Microsoft YaHei',sans-serif; background:#0a1628; color:#e0e6ed; }}
.header {{ text-align:center; padding:18px; background:linear-gradient(90deg,#0d2137,#0a1628,#0d2137); border-bottom:2px solid #1a3a5c; }}
.header h1 {{ font-size:28px; color:#00d4ff; letter-spacing:6px; margin-bottom:4px; }}
.header span {{ color:#4a6a8a; font-size:13px; }}
.kpi-row {{ display:flex; justify-content:center; padding:16px 20px; flex-wrap:wrap; gap:12px; }}
.kpi-card {{ background:linear-gradient(135deg,#0d2137,#132d4a); border:1px solid #1a3a5c; border-radius:10px; padding:18px 28px; text-align:center; min-width:150px; }}
.kpi-card .value {{ font-size:28px; font-weight:bold; color:#00d4ff; }}
.kpi-card .label {{ font-size:12px; color:#6889a8; margin-top:6px; }}
.row {{ display:flex; padding:0 20px 16px; gap:16px; flex-wrap:wrap; }}
.box {{ flex:1; min-width:420px; background:#0d1a2d; border:1px solid #1a3a5c; border-radius:10px; padding:14px; }}
.box h3 {{ font-size:14px; color:#00d4ff; margin-bottom:6px; text-align:center; }}
.chart {{ width:100%; height:330px; }}
</style>
</head>
<body>

<div class="header">
  <h1>🏦 银行数据驾驶舱</h1>
  <span>数据日期: {TD} | 数据源: Hive on HDFS | 离线 T+1 日批 | 表: bank_ads.ads_mgmt_kpi</span>
</div>

<div class="kpi-row">
  <div class="kpi-card"><div class="value">{kpi('TOTAL_CUSTOMER')}</div><div class="label">总客户数</div></div>
  <div class="kpi-card"><div class="value">{kpi('TOTAL_DEPOSIT','yi')}</div><div class="label">存款余额</div></div>
  <div class="kpi-card"><div class="value">{kpi('DAILY_TXN_CNT')}</div><div class="label">当日交易笔数</div></div>
  <div class="kpi-card"><div class="value">{kpi('DAILY_TXN_AMT','yi')}</div><div class="label">当日交易金额</div></div>
  <div class="kpi-card"><div class="value">{kpi('TOTAL_ACCOUNT')}</div><div class="label">账户总数</div></div>
  <div class="kpi-card"><div class="value">{kpi('TOTAL_LOAN')}</div><div class="label">贷款合同数</div></div>
  <div class="kpi-card"><div class="value">{kpi('TOTAL_CC')}</div><div class="label">信用卡发卡量</div></div>
</div>

<div class="row">
  <div class="box"><h3>📊 交易趋势（24小时）</h3><div id="c1" class="chart"></div></div>
  <div class="box"><h3>📈 客户AUM分层分布</h3><div id="c2" class="chart"></div></div>
</div>

<div class="row">
  <div class="box"><h3>🏪 渠道交易分布</h3><div id="c3" class="chart"></div></div>
  <div class="box"><h3>💰 账户类型分布</h3><div id="c4" class="chart"></div></div>
</div>

<div class="row">
  <div class="box" style="flex:2"><h3>👤 Top 20 高净值客户</h3><div id="c5" class="chart"></div></div>
</div>

<div style="text-align:center;padding:14px;color:#3a5a7a;font-size:12px;">
  Bank Data Warehouse v1.0 | Hadoop 3.3.6 / Hive 3.1.3 / Spark 3.5.8 | {TD}
</div>

<script>
var c = function(id,opt){{ echarts.init(document.getElementById(id)).setOption(opt); }};

c('c1',{{
  tooltip:{{trigger:'axis'}},
  grid:{{left:50,right:20,top:10,bottom:30}},
  xAxis:{{type:'category',data:{json.dumps([h[0] for h in hourly])},axisLabel:{{color:'#6889a8'}}}},
  yAxis:{{type:'value',axisLabel:{{color:'#6889a8'}},splitLine:{{lineStyle:{{color:'#1a2a3a'}}}}}},
  series:[{{name:'交易笔数',type:'bar',data:{json.dumps([h[1] for h in hourly])},
    itemStyle:{{color:{{type:'linear',x:0,y:0,x2:0,y2:1,colorStops:[{{offset:0,color:'#00d4ff'}},{{offset:1,color:'#004466'}}]}}}}}}]
}});

c('c2',{{
  tooltip:{{trigger:'item'}},
  series:[{{type:'pie',radius:['45%','75%'],center:['50%','55%'],
    label:{{color:'#6889a8',fontSize:11}},
    data:{json.dumps([{"name":a["bucket"],"value":a["cnt"]} for a in aum_data])},
    itemStyle:{{borderColor:'#0a1628',borderWidth:2}}
  }}]
}});

c('c3',{{
  tooltip:{{trigger:'item'}},
  series:[{{type:'pie',radius:'70%',center:['50%','55%'],
    label:{{color:'#6889a8'}},
    data:{json.dumps([{"name":c["name"],"value":c["cnt"]} for c in channels[:8]])},
    itemStyle:{{borderColor:'#0a1628',borderWidth:2}}
  }}]
}});

c('c4',{{
  tooltip:{{trigger:'axis'}},
  grid:{{left:110,right:20,top:10,bottom:30}},
  yAxis:{{type:'category',data:{json.dumps([a["type"] for a in acct_types][::-1])},axisLabel:{{color:'#6889a8'}}}},
  xAxis:{{type:'value',axisLabel:{{color:'#6889a8'}},splitLine:{{lineStyle:{{color:'#1a2a3a'}}}}}},
  series:[{{type:'bar',data:{json.dumps([a["cnt"] for a in acct_types][::-1])},itemStyle:{{color:'#00d4ff'}}}}]
}});

c('c5',{{
  tooltip:{{trigger:'axis',formatter:'{{b}}<br/>资产: {{c}}万'}},
  grid:{{left:110,right:60,top:10,bottom:30}},
  yAxis:{{type:'category',data:{json.dumps([c["name"] for c in top_cust][::-1])},axisLabel:{{color:'#c0d0e0',fontSize:12}}}},
  xAxis:{{type:'value',axisLabel:{{color:'#6889a8'}},splitLine:{{lineStyle:{{color:'#1a2a3a'}}}}}},
  series:[{{type:'bar',data:{json.dumps([round(c["amt"]/10000,0) for c in top_cust][::-1])},
    itemStyle:{{color:{{type:'linear',x:1,y:0,x2:0,y2:0,colorStops:[{{offset:0,color:'#00d4ff'}},{{offset:1,color:'#004466'}}]}}}},
    label:{{show:true,position:'right',color:'#6889a8',formatter:'{{c}}万'}}
  }}]
}});
</script>
</body>
</html>'''

output = f"/mnt/d/bigdata-lab/bank-data-warehouse/dashboard_{TD}.html"
with open(output, 'w', encoding='utf-8') as f:
    f.write(html)

print(f"\n✅ Dashboard saved!")
print(f"   Open: D:\\bigdata-lab\\bank-data-warehouse\\dashboard_{TD}.html")
