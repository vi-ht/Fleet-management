import json
import os
import threading
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, render_template_string, request
from pymongo import MongoClient
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType


app = Flask(__name__)
client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
database = client[os.getenv("MONGO_DATABASE", "taxi")]
collection = database[os.getenv("MONGO_COLLECTION", "hotspot_predictions")]
vehicle_collection = database[os.getenv("MONGO_VEHICLE_COLLECTION", "vehicle_status")]
dispatch_commands_collection = database[os.getenv("MONGO_DISPATCH_COLLECTION", "vehicle_dispatch_commands")]
forecast_lock = threading.Lock()
forecast_cache = {"created_at": 0.0, "payload": None}
serving_spark = None
serving_model = None
APP_TIMEZONE = "America/New_York"


PAGE = """
<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Điều hành đội xe NYC</title>
  <link rel="icon" href="data:,">
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    :root { color-scheme:light; --canvas:#f3f5f8; --panel:#fff; --line:#e3e8ef; --text:#172438; --muted:#68778b; --navy:#102a43; --yellow:#f2b632; --green:#168465; --blue:#3478b8; --orange:#a96609; --red:#c44747; }
    * { box-sizing:border-box; } html { scroll-behavior:smooth; scroll-padding-top:18px; } body { margin:0; color:var(--text); font:14px Inter,"Segoe UI",Arial,sans-serif; background:var(--canvas); }
    main { max-width:1480px; margin:auto; padding:30px 30px 48px; } header { display:flex; align-items:center; justify-content:space-between; gap:18px; margin-bottom:23px; }
    .brand { display:flex; gap:15px; align-items:center; } .logo { display:grid; place-items:center; width:48px; height:48px; border-radius:14px; color:var(--navy); background:var(--yellow); font-size:23px; font-weight:900; box-shadow:0 5px 14px #c58d2630; } .eyebrow { color:#7b5b12; font-size:11px; font-weight:750; letter-spacing:.12em; text-transform:uppercase; } h1 { margin:5px 0; font-size:clamp(25px,3vw,36px); letter-spacing:-.045em; line-height:1.1; } .subtitle { margin:0; color:var(--muted); font-size:14px; }
    .live { display:flex; align-items:center; gap:8px; padding:9px 13px; border:1px solid #b8e0d1; border-radius:999px; background:#e9f6f0; color:#12694f; font-size:12px; font-weight:700; white-space:nowrap; } .live::before { content:""; width:8px; height:8px; border-radius:50%; background:#168465; box-shadow:0 0 0 3px #16846520; } .live[data-state="error"] { border-color:#f0c5c5; background:#fff0f0; color:#a53131; } .live[data-state="error"]::before { background:#c44747; box-shadow:0 0 0 3px #c4474720; } .live[data-state="loading"] { border-color:#ead8a8; background:#fff9e8; color:#806112; } .live[data-state="loading"]::before { background:#c28d13; }
    nav { display:flex; gap:5px; overflow:auto; scrollbar-width:none; padding:6px; margin-bottom:15px; background:#e9edf3; border:1px solid #e1e6ee; border-radius:12px; } nav::-webkit-scrollbar { display:none; } nav a { color:#56667b; text-decoration:none; padding:9px 14px; border-radius:8px; white-space:nowrap; font-weight:600; transition:background .15s,color .15s; } nav a:hover, nav a.active { background:#fff; color:var(--navy); box-shadow:0 1px 4px #15253b16; }
    .toolbar { display:flex; flex-wrap:wrap; align-items:center; gap:11px; margin-bottom:17px; padding:12px 14px; background:var(--panel); border:1px solid var(--line); border-radius:12px; box-shadow:0 2px 8px #15253b08; } label { color:#53647a; font-size:13px; } select, button { border:1px solid #d5dde7; border-radius:8px; color:var(--text); background:#fff; padding:9px 12px; font:inherit; } select { min-width:170px; } button { cursor:pointer; font-weight:650; transition:background .15s,border-color .15s,transform .15s; } button:hover { border-color:#b18422; background:#fff9e8; } button:active { transform:translateY(1px); } button:focus-visible,select:focus-visible,a:focus-visible,input:focus-visible { outline:3px solid #e5b63880; outline-offset:2px; } .refresh-info { margin-left:auto; color:var(--muted); font-size:12px; }
    .cards { display:grid; grid-template-columns:repeat(8,minmax(0,1fr)); gap:11px; margin-bottom:17px; } .card, section { background:var(--panel); border:1px solid var(--line); border-radius:14px; box-shadow:0 3px 12px #15253b08; } .card { position:relative; padding:15px; min-height:103px; overflow:hidden; } .card:first-child { border-top:3px solid var(--yellow); } .label { color:var(--muted); font-size:10px; text-transform:uppercase; letter-spacing:.07em; line-height:1.35; font-weight:700; } .value { margin-top:14px; font-size:26px; line-height:1; font-weight:750; letter-spacing:-.04em; } .value.small { font-size:14px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; } .metric-meta { margin-top:8px; color:var(--muted); font-size:10px; line-height:1.3; white-space:normal; } .green { color:var(--green); } .orange { color:var(--orange); } .red { color:var(--red); }
    .map-layout { display:grid; grid-template-columns:minmax(0,1.7fr) minmax(310px,.72fr); gap:15px; margin-bottom:16px; } section { padding:19px; min-width:0; } h2 { margin:0 0 15px; font-size:16px; letter-spacing:-.02em; } .section-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:13px; } .section-head h2 { margin:0; } .hint { color:var(--muted); font-size:12px; }
    #map { height:490px; border:1px solid #e2e7ec; border-radius:10px; overflow:hidden; background:#e7edf1; } .leaflet-popup-content-wrapper, .leaflet-popup-tip { background:#fff; color:var(--text); } .leaflet-popup-content-wrapper { box-shadow:0 5px 20px #17243826; } .leaflet-control-attribution { font-size:9px; }
    .taxi-marker { position:relative; display:grid; place-items:center; width:30px; height:30px; border:2px solid #fff; border-radius:50%; color:#07101e; font-size:17px; line-height:1; box-shadow:0 2px 7px #07101e55; } .taxi-marker.available { background:#75d5b5; } .taxi-marker.occupied { background:#f2bf55; } .taxi-glyph { transform:translateY(1px); } .taxi-arrow { position:absolute; top:-10px; right:-6px; color:var(--navy); font-size:12px; font-weight:900; text-shadow:0 1px 3px #fff; transform-origin:50% 100%; }
    .legend { display:flex; flex-wrap:wrap; gap:13px; margin-top:12px; color:#596b80; font-size:11px; } .dot { display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:5px; } .dot.available { background:#42b991; } .dot.occupied { background:#e6aa2c; } .dot.hotspot { background:#dc2626; } .dot.medium { background:#f97316; } #movementStatus { color:#137553; font-weight:700; } .map-controls { display:flex; align-items:center; gap:9px; margin:9px 0; color:var(--muted); font-size:12px; } .map-controls select { min-width:155px; padding:6px 9px; }
    .fleet-list { max-height:490px; overflow:auto; padding-right:3px; } .vehicle { display:flex; align-items:center; justify-content:space-between; gap:9px; padding:12px 2px; border-bottom:1px solid #edf0f4; } .vehicle:last-child { border-bottom:0; } .vehicle-id { font-weight:700; font-size:13px; } .vehicle-meta { color:var(--muted); font-size:11px; line-height:1.45; margin-top:4px; } .status { padding:5px 8px; border-radius:999px; font-size:10px; font-weight:700; white-space:nowrap; } .status.available { color:#12694f; background:#e6f5ee; } .status.occupied { color:#86570e; background:#fff4d9; }
    .main-grid { display:grid; grid-template-columns:minmax(0,1.35fr) minmax(300px,.65fr); gap:15px; margin-bottom:16px; } .chart-wrap { height:255px; position:relative; } .chart-wrap svg { width:100%; height:100%; overflow:visible; } .axis { stroke:#e1e6ed; stroke-width:1; } .axis-label { fill:#718096; font-size:12px; } .line { fill:none; stroke:#168465; stroke-width:3; stroke-linejoin:round; stroke-linecap:round; } .area { fill:url(#area); opacity:.32; } .chart-note { color:var(--muted); font-size:12px; margin-top:6px; }
    .bars { display:flex; flex-direction:column; gap:14px; padding-top:3px; } .bar-row { display:grid; grid-template-columns:48px 1fr 58px; align-items:center; gap:9px; font-size:12px; } .bar-bg { height:9px; border-radius:99px; background:#edf0f4; overflow:hidden; } .bar-fill { height:100%; border-radius:99px; background:linear-gradient(90deg,#4e9bc5,var(--green)); } .bar-value { text-align:right; color:var(--muted); font-variant-numeric:tabular-nums; }
    .dispatch-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; } .dispatch-card { border:1px solid #e7ebf0; background:#fbfcfd; border-radius:10px; padding:13px; } .dispatch-card strong { display:block; color:#17694f; margin-bottom:6px; font-size:13px; } .dispatch-card span { color:var(--muted); font-size:12px; line-height:1.5; } .forecast-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; } .forecast-card { border:1px solid #e7ebf0; background:#fbfcfd; border-radius:10px; padding:14px; min-height:135px; } .forecast-card h3 { margin:0 0 10px; color:#315e80; font-size:14px; } .forecast-zone { display:flex; justify-content:space-between; gap:8px; padding:7px 0; border-bottom:1px solid #edf0f4; font-size:12px; } .forecast-zone b { color:var(--text); } .forecast-zone span { color:#8a5a11; font-variant-numeric:tabular-nums; } .forecast-note { color:var(--muted); font-size:12px; margin:-3px 0 12px; }
    .table-wrap { max-height:350px; overflow:auto; } table { width:100%; border-collapse:collapse; font-size:12px; } th,td { padding:11px 9px; text-align:left; border-bottom:1px solid #edf0f4; white-space:nowrap; } th { position:sticky; top:0; background:#f7f9fb; color:#627187; font-weight:700; } tbody tr:hover { background:#f8fafc; } .pill { padding:4px 8px; border-radius:999px; background:#e7f5ef; color:#17694f; font-size:10px; font-weight:700; } .pill.alert { color:#9f1239; background:#ffe4e6; } .pill.normal { color:#17694f; background:#e7f5ef; }
    .dispatch-card { min-width:0; } .dispatch-controls { display:flex; gap:7px; margin-top:11px; min-width:0; } .dispatch-controls select { flex:1; width:0; min-width:0; max-width:100%; padding:7px 8px; font-size:12px; } .dispatch-controls button { padding:7px 9px; font-size:12px; background:#eaf6f0; border-color:#b8e0d1; color:#12694f; } .dispatch-controls button:disabled { opacity:.6; cursor:wait; } .command-list { display:flex; flex-wrap:wrap; gap:7px; margin-top:12px; } .command-item { border:1px solid #e7ebf0; border-radius:8px; padding:7px 10px; color:#58697e; font-size:11px; } .command-item b { color:#24344a; } .command-item .command-status { color:#17694f; font-weight:700; } .dispatch-message { min-height:17px; margin-top:8px; color:#17694f; font-size:12px; }
    footer { color:var(--muted); font-size:11px; margin-top:15px; }
    @media (max-width:1250px) { .cards { grid-template-columns:repeat(4,minmax(0,1fr)); } .map-layout { grid-template-columns:minmax(0,1.4fr) minmax(285px,.8fr); } } @media (max-width:950px) { main { padding:22px 18px 38px; } .cards { grid-template-columns:repeat(4,minmax(0,1fr)); } .map-layout,.main-grid { grid-template-columns:1fr; } .fleet-list { max-height:360px; } #map { height:420px; } nav { overflow:visible; flex-wrap:wrap; } nav a { white-space:normal; } }
    @media (max-width:650px) { main { padding:18px 12px 30px; } header { align-items:flex-start; } .brand { gap:11px; } .logo { width:40px; height:40px; } .eyebrow { font-size:9px; } h1 { font-size:24px; } .subtitle { font-size:12px; } .live { padding:7px 9px; font-size:0; } .live::after { content:"API"; font-size:10px; } nav { margin-left:-2px; margin-right:-2px; } nav a { padding:9px 11px; font-size:12px; } .toolbar { gap:9px; padding:11px; } .toolbar label:first-child { width:100%; } select { flex:1; min-width:0; } .refresh-info { width:100%; margin-left:0; } .cards { grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; } .card { min-height:90px; padding:12px; } .label { font-size:10px; } .value { margin-top:12px; font-size:23px; } section { padding:15px; } #map { height:350px; } .section-head { align-items:flex-start; } .hint { text-align:right; } .dispatch-grid,.forecast-grid { grid-template-columns:1fr; } .dispatch-card { padding:12px; } .forecast-card { min-height:auto; } .table-wrap { margin:0 -5px; } }
  </style>
</head>
<body><main>
  <header><div class="brand"><div class="logo">T</div><div><div class="eyebrow">BDA501 · NYC TAXI REPLAY DEMO</div><h1>Phân tích hotspot và mô phỏng đội xe</h1><p class="subtitle">Dữ liệu TLC đã replay · điểm hotspot là điểm tương đối 0–100, không phải số chuyến</p></div></div><div class="live" id="connectionStatus" data-state="loading" role="status" aria-live="polite"><span id="connection">Đang kết nối</span></div></header>
  <nav><a class="active" href="#tong-quan">Tổng quan</a><a href="#ban-do">Bản đồ & xe mô phỏng</a><a href="#du-bao">Phân tích hotspot</a><a href="#dieu-phoi">Gợi ý mô phỏng</a></nav>
  <div class="toolbar"><label for="zone">Lọc score theo pickup zone</label><select id="zone"><option value="">Tất cả zone</option></select><button id="refresh" type="button">↻ Cập nhật</button><label><input id="auto" type="checkbox" checked> Tự động cập nhật</label><span class="refresh-info">Cập nhật lần cuối: <span id="updated">Chưa có dữ liệu</span></span></div>

  <div id="tong-quan" class="cards"><div class="card"><div class="label">Dashboard API</div><div class="value small" id="pipeline">Đang kết nối</div></div><div class="card"><div class="label">Xe mô phỏng</div><div class="value" id="fleetTotal">—</div></div><div class="card"><div class="label">Xe đang di chuyển</div><div class="value green" id="fleetMoving">—</div></div><div class="card"><div class="label">Xe rảnh · chờ điều phối</div><div class="value green" id="fleetAvailable">—</div></div><div class="card"><div class="label">Xe mô phỏng có khách</div><div class="value orange" id="fleetOccupied">—</div></div><div class="card"><div class="label">Bản ghi score replay</div><div class="value" id="count">—</div></div><div class="card"><div class="label">Pickup zone có dữ liệu</div><div class="value" id="zones">—</div></div><div class="card"><div class="label">MAE / RMSE / R² (điểm)</div><div class="value small" id="modelMetrics">Chưa có chỉ số</div><div class="metric-meta" id="modelClassificationMetrics">Precision / Recall: —</div></div></div>

  <div id="ban-do" class="map-layout"><section><div class="section-head"><h2>Hotspot và đội xe mô phỏng</h2><span class="hint">260 LocationID · 263 polygon parts · NYC Taxi Zone</span></div><div class="map-controls"><label for="mapForecastHour">Score forecast map cho:</label><select id="mapForecastHour"><option value="0">Đang tải forecast...</option></select></div><div id="map" role="region" aria-label="Bản đồ taxi zone NYC tô màu theo dự báo hotspot cùng vị trí xe mô phỏng"></div><div class="legend"><span><i class="dot hotspot"></i>Hotspot ≥ 50</span><span><i class="dot medium"></i>Score 25–49</span><span><i class="dot available"></i>Xe rảnh · chờ duyệt</span><span><i class="dot occupied"></i>Xe có khách</span><span id="movementStatus">—</span><span id="latestVehicle">—</span></div><div class="chart-note">Di chuột lên zone để xem xe rảnh tại chỗ, xe điều phối đang tới và số xe mục tiêu ước tính. Mục tiêu dùng quy tắc mô phỏng ceil(score/25), không phải đầu ra số xe của model. Score là dự báo tương đối 0–100; một LocationID có thể gồm nhiều polygon rời nhau. Xe rảnh chỉ chạy sau khi người điều phối duyệt lệnh. Nền zone: NYC TLC Taxi Zone boundaries.</div></section><section><div class="section-head"><h2>Danh sách xe mô phỏng</h2><span class="hint" id="fleetCount">—</span></div><div id="fleetList" class="fleet-list"><div class="chart-note">Đang tải dữ liệu xe...</div></div></section></div>

  <div id="du-bao" class="main-grid"><section><h2>Score hotspot theo giờ replay</h2><div class="chart-wrap"><svg id="trend" role="img" aria-label="Điểm hotspot trung bình theo giờ replay, thang 0 đến 100" viewBox="0 0 760 240" preserveAspectRatio="none"><defs><linearGradient id="area" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="#55d6be" stop-opacity=".45"/><stop offset="1" stop-color="#55d6be" stop-opacity="0"/></linearGradient></defs><path class="axis" d="M30 210H750M30 115H750M30 20H750"/><text class="axis-label" x="2" y="24">100</text><text class="axis-label" x="8" y="119">50</text><text class="axis-label" x="18" y="214">0</text><text id="trendEmpty" class="axis-label" x="390" y="118" text-anchor="middle">Chưa có dữ liệu theo giờ</text><text id="trendStart" class="axis-label" x="30" y="234">—</text><text id="trendEnd" class="axis-label" x="750" y="234" text-anchor="end">—</text><path id="areaPath" class="area"/><path id="linePath" class="line"/></svg></div><div class="chart-note">Điểm hotspot trung bình 0–100 của các bản ghi đã replay; đây không phải số chuyến xe.</div></section><section><h2>Top pickup zone theo score replay</h2><div id="ranking" class="bars"><div class="chart-note">Đang tải...</div></div></section></div>
  <section id="dieu-phoi" style="margin-bottom:16px"><div class="section-head"><h2>Điều phối xe có người duyệt</h2><span class="hint">Xe rảnh chỉ tái bố trí sau khi bạn chọn khu và xác nhận</span></div><div class="chart-note">Đề xuất chỉ mang tính tham khảo. Bấm “Duyệt điều xe” mới gửi lệnh tới simulator; xe đang chở khách tiếp tục hoàn tất chuyến hiện tại.</div><div id="dispatch" class="dispatch-grid"><div class="chart-note">Đang tính đề xuất...</div></div><div id="dispatchMessage" class="dispatch-message" role="status" aria-live="polite"></div><div id="dispatchCommands" class="command-list" aria-live="polite"></div></section>
  <section id="diem-nong-sap-toi" style="margin-bottom:16px"><div class="section-head"><h2>Dự báo hotspot 3 giờ tới</h2><span class="hint" id="forecastHeadline">Đang tải model forecast...</span></div><div class="forecast-note" id="forecastNote">Dự báo từng pickup zone cho các giờ kế tiếp. Điểm 0–100 là tương đối; điều phối bên dưới chỉ là gợi ý mô phỏng.</div><div id="upcomingHotspots" class="forecast-grid"><div class="chart-note">Đang chạy dự báo...</div></div></section>
  <section><h2>Score model mới nhất trong replay</h2><div class="table-wrap"><table><thead><tr><th>Mã sự kiện</th><th>Thời gian event (replay)</th><th>Pickup zone</th><th>Hotspot score</th><th>Phân loại</th><th>Trạng thái</th></tr></thead><tbody id="rows"><tr><td colspan="6">Đang tải...</td></tr></tbody></table></div></section>
  <footer>Mô phỏng liên tục: producer → Kafka → model inference → MongoDB · Tự động làm mới dashboard 3 giây</footer>
</main>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); const zoneSelect=document.querySelector('#zone'); let map,markers,zoneLayer,activeForecasts=[],knownZones=[],dispatchZoneCatalog=[],fleetVehicles=[];
if(typeof L!=='undefined'){map=L.map('map').setView([40.73,-73.96],11);L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);zoneLayer=L.featureGroup().addTo(map);markers=L.layerGroup().addTo(map);}else{document.querySelector('#map').innerHTML='<div class="chart-note" style="padding:30px">Không tải được bản đồ nền. Kiểm tra kết nối Internet để tải OpenStreetMap.</div>';}
async function loadTaxiZones(){if(!map)return;const response=await fetch('/static/taxi_zones.geojson');if(!response.ok)throw new Error(`HTTP ${response.status} loading taxi zones`);const geojson=await response.json(),zoneNames=new Map();geojson.features.forEach(feature=>{const properties=feature.properties||{},id=String(properties.LocationID||'');if(id)zoneNames.set(id,String(properties.zone||''));});dispatchZoneCatalog=[...zoneNames].map(([id,name])=>({id,name})).sort((a,b)=>Number(a.id)-Number(b.id));L.geoJSON(geojson,{style:{color:'#64748b',weight:.7,fillColor:'#94a3b8',fillOpacity:.12},onEachFeature:(feature,layer)=>{const properties=feature.properties||{};layer.bindTooltip(`Zone ${esc(properties.LocationID)} · ${esc(properties.zone||'')}`);}}).eachLayer(layer=>zoneLayer.addLayer(layer));if(!window.mapHasFitted){map.fitBounds(zoneLayer.getBounds(),{padding:[18,18]});window.mapHasFitted=true;}updateHotspotMap();}
function updateHotspotMap(){if(!zoneLayer||!zoneLayer.getLayers().length)return;const index=Number(document.querySelector('#mapForecastHour').value||0),forecast=activeForecasts[index],scores=new Map((forecast?.zones||[]).map(zone=>[String(zone.zone),zone]));zoneLayer.eachLayer(layer=>{const properties=layer.feature?.properties||{},zoneId=String(properties.LocationID||''),zone=scores.get(zoneId),score=Number(zone?.hotspot_score||0),fill=score>=50?'#dc2626':score>=25?'#f97316':'#64748b',available=fleetVehicles.filter(vehicle=>String(vehicle.pickup_zone)===zoneId&&vehicle.status==='available'&&!['routing','en_route'].includes(vehicle.dispatch_status)),incoming=fleetVehicles.filter(vehicle=>vehicle.dispatch_status==='en_route'&&String(vehicle.dropoff_zone)===zoneId),target=zone?Math.ceil(score/25):null,additional=target===null?null:Math.max(0,target-available.length-incoming.length),incomingIds=incoming.map(vehicle=>esc(vehicle.vehicle_id)).join(', ')||'Không có';layer.setStyle({color:score>=50?'#991b1b':score>=25?'#c2410c':'#64748b',weight:score>=25?1.2:.65,fillColor:fill,fillOpacity:zone?(score>=50?.62:.48):.1});layer.unbindTooltip();layer.bindTooltip(`<b>Zone ${esc(zoneId)} · ${esc(properties.zone||'')}</b><br>${zone?`Forecast ${esc(forecast.label)} · score ${score.toFixed(1)}/100<br>Xe mục tiêu ước tính: ${target}<br>Xe rảnh tại zone: ${available.length}<br>Cần điều thêm ước tính: ${additional}`:'Chưa có forecast cho khu vực này'}<br>Xe điều phối đang tới (${incoming.length}): ${incomingIds}`,{sticky:true});layer.unbindPopup();if(zone){layer.bindPopup(`<b>Pickup zone ${esc(zoneId)} · ${esc(properties.zone||'')}</b><br>Forecast ${esc(forecast.label)}<br>Score: ${score.toFixed(1)}/100 · ${esc(zone.hotspot_level)}<br>Ngưỡng cảnh báo 3σ: ${Number(zone.anomaly_threshold).toFixed(1)} · trung bình train ${Number(zone.historical_mean_score).toFixed(1)} · σ ${Number(zone.historical_stddev_score).toFixed(1)} · n=${Number(zone.historical_baseline_samples)}<br>${zone.alert_flag==='CRITICAL_ANOMALY'?'⚠️ Vượt ngưỡng bất thường 3σ':'Trong ngưỡng lịch sử'}`);}});}
async function loadZones(){const zones=await fetch('/api/zones').then(r=>r.json());knownZones=zones;zones.forEach(z=>{const o=document.createElement('option');o.value=z;o.textContent=`Pickup zone ${z}`;zoneSelect.appendChild(o);});}
function drawTrend(series){const values=series.map(x=>Number(x.value||0)),max=100,left=30,top=20,width=720,height=190;document.querySelector('#trendEmpty').style.display=values.length?'none':'block';const stamp=value=>{const text=String(value||'');return text.length>=13?`${text.slice(11,13)}:00`:text.slice(0,10);};document.querySelector('#trendStart').textContent=values.length?stamp(series[0].bucket):'—';document.querySelector('#trendEnd').textContent=values.length?stamp(series[series.length-1].bucket):'—';const points=values.map((v,i)=>`${left+(values.length===1?width/2:i*width/(values.length-1))},${top+height-(Math.max(0,Math.min(max,v))/max)*height}`).join(' ');document.querySelector('#linePath').setAttribute('d',points?`M ${points}`:'');document.querySelector('#areaPath').setAttribute('d',points?`M ${left} ${top+height} L ${points} L ${left+width} ${top+height} Z`:'');}
 function renderDispatch(data){const hotspots=data.hotspots||[],scoreByZone=new Map(hotspots.map(zone=>[String(zone.zone),zone])),suggestedZones=hotspots.map(zone=>({id:String(zone.zone),name:'',score:Number(zone.hotspot_score||0)})),suggestedIds=new Set(suggestedZones.map(zone=>zone.id)),allZones=[...suggestedZones,...dispatchZoneCatalog.filter(zone=>!suggestedIds.has(zone.id)).map(zone=>({...zone,score:null}))];document.querySelector('#dispatch').innerHTML=data.recommendations.length?data.recommendations.map(x=>`<div class="dispatch-card" data-recommendation="${esc(x.vehicle_id)}"><strong>🚕 ${esc(x.vehicle_id)} · hiện ở zone ${esc(x.from_zone)}</strong><span>Đề xuất: Zone ${esc(x.target_zone)} · score replay ${Number(x.hotspot_score).toFixed(0)}/100 · ${esc(x.reason)}</span><div class="dispatch-controls"><select class="dispatch-target" aria-label="Chọn điểm đến cho ${esc(x.vehicle_id)}">${allZones.map(zone=>{const score=scoreByZone.get(zone.id);return `<option value="${esc(zone.id)}" ${zone.id===String(x.target_zone)?'selected':''}>Zone ${esc(zone.id)}${zone.name?` · ${esc(zone.name)}`:''}${score?` · ${Number(score.hotspot_score).toFixed(0)}/100`:''}</option>`;}).join('')}</select><button type="button" data-approve-vehicle="${esc(x.vehicle_id)}">Duyệt điều xe</button></div></div>`).join(''):'<div class="chart-note">Hiện không có xe rảnh cần điều phối.</div>';const labels={approved:'Đã duyệt · chờ simulator',routing:'Đang tạo tuyến',en_route:'Đang đi tới điểm đã duyệt',arrived:'Đã tới điểm đến',rejected:'Không thực hiện được'};document.querySelector('#dispatchCommands').innerHTML=(data.commands||[]).map(x=>`<span class="command-item"><b>${esc(x.vehicle_id)}</b> → Zone ${esc(x.target_zone)} · <span class="command-status">${esc(labels[x.status]||x.status)}</span></span>`).join('');}
async function approveDispatch(button){const card=button.closest('[data-recommendation]'),vehicleId=card?.dataset.recommendation,targetZone=card?.querySelector('.dispatch-target')?.value,message=document.querySelector('#dispatchMessage');if(!vehicleId||!targetZone||!window.confirm(`Xác nhận điều xe ${vehicleId} tới pickup zone ${targetZone}?`))return;button.disabled=true;button.textContent='Đang gửi lệnh…';message.textContent='';try{const response=await fetch(`/api/vehicles/${encodeURIComponent(vehicleId)}/dispatch`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({target_zone:targetZone})}),result=await response.json();if(!response.ok)throw new Error(result.error||`HTTP ${response.status}`);message.textContent=`Đã duyệt ${vehicleId} tới zone ${targetZone}. Simulator sẽ xác nhận và tạo tuyến đường.`;await refresh();}catch(error){message.textContent=`Chưa gửi được lệnh: ${error.message}`;button.disabled=false;button.textContent='Duyệt điều xe';}}
 renderUpcoming=function renderUpcomingCalendarModel(data){const headline=document.querySelector('#forecastHeadline');headline.textContent=data.available?`Cập nhật ${esc(data.generated_at)} · ${esc(data.model)}`:`Forecast chưa sẵn sàng: ${esc(data.message||'Không có model')}`;document.querySelector('#forecastNote').textContent='Dự báo hotspot từ pickup zone, giờ và thứ trong tuần. Điểm 0–100 là tương đối; xe và gợi ý điều phối là mô phỏng.';const select=document.querySelector('#mapForecastHour');activeForecasts=data.forecasts||[];select.innerHTML=activeForecasts.map((window,index)=>`<option value="${index}">${esc(window.label)}</option>`).join('')||'<option value="0">Forecast chưa có</option>';select.onchange=updateHotspotMap;document.querySelector('#upcomingHotspots').innerHTML=activeForecasts.map(window=>`<div class="forecast-card"><h3>${esc(window.label)}</h3>${window.zones.slice(0,5).map(zone=>`<div class="forecast-zone"><b>Pickup zone ${esc(zone.zone)}</b><span>${Number(zone.hotspot_score).toFixed(0)}/100 · ${esc(zone.hotspot_level)}${zone.alert_flag==='CRITICAL_ANOMALY'?' · ⚠️ bất thường':''}<br>Ngưỡng 3σ ${Number(zone.anomaly_threshold).toFixed(1)} · train μ=${Number(zone.historical_mean_score).toFixed(1)}, σ=${Number(zone.historical_stddev_score).toFixed(1)}, n=${Number(zone.historical_baseline_samples)}</span></div>`).join('')||'<div class="chart-note">Không có zone để dự báo.</div>'}</div>`).join('')||'<div class="chart-note">Không có dự báo sẵn có.</div>';updateHotspotMap();}; function renderFleet(items){const available=items.filter(x=>x.status==='available').length,occupied=items.filter(x=>x.status==='occupied').length;document.querySelector('#fleetTotal').textContent=items.length;document.querySelector('#fleetAvailable').textContent=available;document.querySelector('#fleetOccupied').textContent=occupied;document.querySelector('#fleetCount').textContent=`${items.length} xe`;document.querySelector('#fleetList').innerHTML=items.length?items.map(v=>`<div class="vehicle"><div><div class="vehicle-id">${esc(v.vehicle_id)}</div><div class="vehicle-meta">Khu ${esc(v.pickup_zone)} → ${esc(v.dropoff_zone)} · ${Number(v.speed_kmh||0)} km/h</div></div><span class="status ${v.status}">${v.status==='available'?'Rảnh':'Có khách'}</span></div>`).join(''):'<div class="chart-note">Chưa có snapshot xe.</div>';if(markers){markers.clearLayers();const bounds=[];items.forEach(v=>{const position=[Number(v.latitude),Number(v.longitude)];bounds.push(position);const icon=L.divIcon({className:'',html:`<div class="taxi-marker ${v.status}" title="${esc(v.vehicle_id)}">🚕</div>`,iconSize:[42,42],iconAnchor:[21,21]});L.marker(position,{icon,zIndexOffset:1000}).bindPopup(`<b>${esc(v.vehicle_id)}</b><br>Trạng thái: ${v.status==='available'?'Xe rảnh':'Đang có khách'}<br>Khu vực: ${esc(v.pickup_zone)} → ${esc(v.dropoff_zone)}<br>Tốc độ: ${Number(v.speed_kmh||0)} km/h`).addTo(markers);});if(bounds.length && !window.mapHasFitted){map.fitBounds(bounds,{padding:[30,30],maxZoom:12});window.mapHasFitted=true;}}document.querySelector('#latestVehicle').textContent=items.length?`Snapshot #${items[0].simulation_cycle} · ${items[0].updated_at}`:'Chưa có snapshot';}
  let refreshInProgress=false;
async function fetchJson(url){const response=await fetch(url);if(!response.ok)throw new Error(`HTTP ${response.status} from ${url}`);return response.json();}
async function loadModelMetrics(){try{const metrics=await fetchJson('/api/model-metrics'),value=document.querySelector('#modelMetrics'),classification=document.querySelector('#modelClassificationMetrics'),formatMetric=number=>Number.isFinite(Number(number))?Number(number).toFixed(2):'—',formatRate=number=>Number.isFinite(Number(number))?`${(Number(number)*100).toFixed(1)}%`:'—',evaluation=String(metrics.evaluation_method||'').toLowerCase().includes('temporal')?'Temporal holdout':'Holdout';value.textContent=metrics.available?`${formatMetric(metrics.mae)} / ${formatMetric(metrics.rmse)} / ${formatMetric(metrics.r2)}`:'Chưa có chỉ số';classification.textContent=metrics.available?`${evaluation} · P ${formatRate(metrics.hotspot_precision)} · R ${formatRate(metrics.hotspot_recall)} · ngưỡng ${formatMetric(metrics.hotspot_classification_threshold)}`:'Precision / Recall: —';value.title=metrics.available?`${metrics.model} · ${Number(metrics.evaluation_rows).toLocaleString()} holdout rows · ${metrics.evaluation_method}; MAE/RMSE tính bằng điểm hotspot; R² có thể âm.`:'Chạy bước train model để tạo kết quả holdout';}catch(error){document.querySelector('#modelMetrics').textContent='Không khả dụng';document.querySelector('#modelClassificationMetrics').textContent='Precision / Recall: —';}}
  async function loadUpcoming(){try{renderUpcoming(await fetchJson('/api/upcoming-hotspots?hours=3'));}catch(error){renderUpcoming({available:false,message:error.message,forecasts:[]});}}
async function refresh(){if(refreshInProgress)return;refreshInProgress=true;const connection=document.querySelector('#connection'),connectionStatus=document.querySelector('#connectionStatus'),pipeline=document.querySelector('#pipeline'),refreshButton=document.querySelector('#refresh');refreshButton.disabled=true;connection.textContent='Đang cập nhật';connectionStatus.dataset.state='loading';try{const zone=encodeURIComponent(zoneSelect.value),[data,fleet,dispatch]=await Promise.all([fetchJson(`/api/dashboard?zone=${zone}`),fetchJson('/api/vehicles'),fetchJson('/api/dispatch')]);connection.textContent='API kết nối';connectionStatus.dataset.state='ok';pipeline.textContent='Đang hoạt động';pipeline.className='value small green';document.querySelector('#count').textContent=Number(data.summary.count).toLocaleString();document.querySelector('#zones').textContent=Number(data.summary.zones).toLocaleString();document.querySelector('#updated').textContent=new Date().toLocaleTimeString();renderFleet(fleet.vehicles);renderDispatch(dispatch);drawTrend(data.series);const max=Math.max(...data.ranking.map(x=>Number(x.value||0)),1);document.querySelector('#ranking').innerHTML=data.ranking.length?data.ranking.map(x=>`<div class="bar-row"><span>Zone ${esc(x.zone)}</span><div class="bar-bg"><div class="bar-fill" style="width:${Math.max(3,Number(x.value)/max*100)}"></div></div><span class="bar-value">${Number(x.value).toFixed(0)}/100</span></div>`).join(''):'<div class="chart-note">Chưa có dữ liệu để xếp hạng.</div>';document.querySelector('#rows').innerHTML=data.predictions.length?data.predictions.map(p=>`<tr><td>${esc(p.event_id)}</td><td>${esc(p.event_time)}</td><td>Zone ${esc(p.pickup_zone)}</td><td><b>${Number(p.hotspot_score||0).toFixed(0)}/100</b></td><td>${esc(p.hotspot_level||'—')}</td><td><span title="Threshold 3σ ${Number(p.anomaly_threshold||100).toFixed(1)} · n=${Number(p.historical_baseline_samples||0)}" class="pill ${p.alert_flag==='CRITICAL_ANOMALY'?'alert':'normal'}">${esc(p.alert_flag||'NORMAL')}</span></td></tr>`).join(''):'<tr><td colspan="6">Chưa có bản ghi replay.</td></tr>';document.querySelector('#updated').title=`Lần làm mới thành công: ${new Date().toLocaleString()}`;}catch(error){console.error('Dashboard refresh failed',error);connection.textContent='Mất kết nối';connectionStatus.dataset.state='error';pipeline.textContent='Không truy cập được';pipeline.className='value small red';document.querySelector('#updated').textContent='Làm mới thất bại';}finally{refreshInProgress=false;refreshButton.disabled=false;}}
zoneSelect.addEventListener('change',refresh);document.querySelector('#dispatch').addEventListener('click',event=>{const button=event.target.closest('[data-approve-vehicle]');if(button)approveDispatch(button);});document.querySelector('#refresh').addEventListener('click',()=>{refresh();loadModelMetrics();loadUpcoming();});loadTaxiZones().then(()=>refresh()).catch(error=>{console.error('Taxi zone GeoJSON failed',error);document.querySelector('#map').setAttribute('aria-label','Không tải được ranh giới Taxi Zone');});loadZones().then(refresh).catch(error=>{console.error('Zone list failed',error);refresh();});loadModelMetrics();loadUpcoming();setInterval(()=>{if(document.querySelector('#auto').checked){refresh();loadModelMetrics();}},3000);setInterval(loadUpcoming,600000);

// Keep one Leaflet marker per vehicle and interpolate between Kafka snapshots.
// This makes the simulation visibly continuous instead of replacing all markers
// with a new static snapshot every refresh.
const liveMarkers={};
function liveVehicleIcon(vehicle){const heading=Number(vehicle.heading_degrees||0);return L.divIcon({className:'',html:`<div class="taxi-marker ${vehicle.status}" title="${esc(vehicle.vehicle_id)}"><span class="taxi-glyph">🚕</span><span class="taxi-arrow" style="transform:rotate(${heading}deg)">▲</span></div>`,iconSize:[34,34],iconAnchor:[17,17]});}
function liveVehiclePopup(vehicle){const status=vehicle.route_status||(vehicle.status==='occupied'?'Đang chở khách · hoàn tất chuyến mô phỏng':'Rảnh · chờ người điều phối');return `<b>${esc(vehicle.vehicle_id)}</b><br>Trạng thái: ${esc(status)}<br>Tuyến: Khu ${esc(vehicle.pickup_zone)} → Khu ${esc(vehicle.dropoff_zone)}<br>Tốc độ: ${Number(vehicle.speed_kmh||0).toFixed(1)} km/h · Hướng: ${Number(vehicle.heading_degrees||0).toFixed(0)}°<br>Tiến độ: ${Number(vehicle.progress_pct||0).toFixed(0)}% · ETA: ${Number(vehicle.eta_minutes||0).toFixed(1)} phút`;}
function animateLiveMarker(marker,target){if(marker._motionFrame)cancelAnimationFrame(marker._motionFrame);const start=marker.getLatLng(),started=performance.now(),duration=1800;const tick=now=>{const progress=Math.min(1,(now-started)/duration);marker.setLatLng([start.lat+(target[0]-start.lat)*progress,start.lng+(target[1]-start.lng)*progress]);if(progress<1)marker._motionFrame=requestAnimationFrame(tick);};marker._motionFrame=requestAnimationFrame(tick);}
renderFleet=function renderLiveFleet(items){fleetVehicles=items;const activeDispatch=['routing','en_route'],available=items.filter(x=>x.status==='available'&&!activeDispatch.includes(x.dispatch_status)).length,occupied=items.filter(x=>x.status==='occupied').length,moving=items.filter(x=>Number(x.speed_kmh||0)>0).length;document.querySelector('#fleetTotal').textContent=items.length;document.querySelector('#fleetMoving').textContent=moving;document.querySelector('#fleetAvailable').textContent=available;document.querySelector('#fleetOccupied').textContent=occupied;document.querySelector('#fleetCount').textContent=`${items.length} xe mô phỏng · ${moving} đang chạy`;document.querySelector('#movementStatus').textContent=`MÔ PHỎNG · ${moving}/${items.length} xe đang chạy`;document.querySelector('#fleetList').innerHTML=items.length?items.map(v=>`<div class="vehicle"><div><div class="vehicle-id">${esc(v.vehicle_id)}</div><div class="vehicle-meta">${esc(v.route_status||'Đang chạy')} · Zone ${esc(v.pickup_zone)} → Zone ${esc(v.dropoff_zone)} · ${Number(v.speed_kmh||0).toFixed(1)} km/h · ETA ${Number(v.eta_minutes||0).toFixed(1)} phút</div></div><span class="status ${v.status}">${v.status==='occupied'?'Có khách':v.dispatch_status==='routing'?'Đang tạo tuyến':v.dispatch_status==='en_route'?'Đã duyệt · đang chạy':v.dispatch_status==='arrived'?'Đã tới điểm đến':'Rảnh'}</span></div>`).join(''):'<div class="chart-note">Chưa có snapshot xe.</div>';if(markers){const bounds=[];const activeIds=new Set(items.map(v=>v.vehicle_id));items.forEach(vehicle=>{const target=[Number(vehicle.latitude),Number(vehicle.longitude)];bounds.push(target);let marker=liveMarkers[vehicle.vehicle_id];if(!marker){marker=L.marker(target,{icon:liveVehicleIcon(vehicle),zIndexOffset:1000}).addTo(markers);liveMarkers[vehicle.vehicle_id]=marker;}else{marker.setIcon(liveVehicleIcon(vehicle));animateLiveMarker(marker,target);}marker.bindPopup(liveVehiclePopup(vehicle));});Object.keys(liveMarkers).forEach(id=>{if(!activeIds.has(id)){if(liveMarkers[id]._motionFrame)cancelAnimationFrame(liveMarkers[id]._motionFrame);markers.removeLayer(liveMarkers[id]);delete liveMarkers[id];}});if(bounds.length&&!window.mapHasFitted){map.fitBounds(bounds,{padding:[30,30],maxZoom:12});window.mapHasFitted=true;}}updateHotspotMap();document.querySelector('#latestVehicle').textContent=items.length?`Snapshot #${items[0].simulation_cycle} · ${items[0].updated_at}`:'Chưa có snapshot';};
</script></body></html>
"""


def match_for_zone():
    zone = request.args.get("zone", "").strip()
    return ({"pickup_zone": zone} if zone else {}, zone)


def hotspot_level(score):
    score = float(score or 0)
    if score >= 75:
        return "Rất nóng"
    if score >= 50:
        return "Nóng"
    if score >= 25:
        return "Trung bình"
    return "Thấp"


@app.get("/health")
def health():
    client.admin.command("ping")
    return {"status": "ok"}


@app.get("/api/model-metrics")
def model_metrics():
    metrics_path = Path(os.getenv("MODEL_METRICS_PATH", "/workspace/data/results/model_metrics.json"))
    try:
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return jsonify({"available": False})
    return jsonify({**metrics, "available": True})


@app.get("/")
def index():
    return render_template_string(PAGE)


@app.get("/api/zones")
def zones():
    def sort_zone(value):
        try:
            return (0, int(value))
        except (TypeError, ValueError):
            return (1, str(value))

    return jsonify(sorted(collection.distinct("pickup_zone"), key=sort_zone))


@app.get("/api/vehicles")
def vehicles():
    rows = list(vehicle_collection.find({}, {"_id": 0}).sort("vehicle_id", 1))
    return jsonify({"vehicles": rows, "count": len(rows)})


def zone_representative_point(target_zone):
    geojson_path = Path(__file__).resolve().parent / "static" / "taxi_zones.geojson"
    geojson = json.loads(geojson_path.read_text(encoding="utf-8"))
    for feature in geojson["features"]:
        if str(feature.get("properties", {}).get("LocationID")) != str(target_zone):
            continue
        geometry = feature.get("geometry") or {}
        polygons = geometry.get("coordinates", [])
        if geometry.get("type") == "Polygon":
            polygons = [polygons]
        points = [point for polygon in polygons for point in (polygon[0] if polygon else [])]
        if not points:
            break
        return (
            sum(float(point[0]) for point in points) / len(points),
            sum(float(point[1]) for point in points) / len(points),
        )
    raise ValueError("Không tìm thấy tọa độ điểm đến cho zone này")


@app.get("/api/dispatch")
def dispatch():
    hotspots = list(
        collection.aggregate(
            [
                {"$group": {"_id": "$pickup_zone", "hotspot_score": {"$avg": "$hotspot_score"}, "sample_count": {"$sum": 1}}},
                {"$sort": {"hotspot_score": -1}},
                {"$limit": 10},
            ]
        )
    )
    active_statuses = ["approved", "routing", "en_route"]
    active_vehicle_ids = dispatch_commands_collection.distinct(
        "vehicle_id", {"status": {"$in": active_statuses}}
    )
    available = list(
        vehicle_collection.find(
            {"status": "available", "vehicle_id": {"$nin": active_vehicle_ids}},
            {"_id": 0, "vehicle_id": 1, "pickup_zone": 1},
        ).sort("updated_at", -1)
    )
    recommendations = []
    for vehicle, hotspot in zip(available, hotspots):
        recommendations.append(
            {
                "vehicle_id": vehicle.get("vehicle_id"),
                "from_zone": vehicle.get("pickup_zone"),
                "target_zone": hotspot.get("_id"),
                "hotspot_score": round(float(hotspot.get("hotspot_score", 0)), 2),
                "reason": "Được xếp hạng theo score trung bình của replay",
            }
        )
    return jsonify(
        {
            "hotspots": [
                {
                    "zone": row.get("_id"),
                    "hotspot_score": round(float(row.get("hotspot_score", 0)), 2),
                    "hotspot_level": hotspot_level(row.get("hotspot_score", 0)),
                    "sample_count": int(row.get("sample_count", 0)),
                }
                for row in hotspots
            ],
            "recommendations": recommendations,
            "commands": list(
                dispatch_commands_collection.find(
                    {}, {"_id": 0, "command_id": 1, "vehicle_id": 1, "target_zone": 1, "status": 1, "created_at": 1}
                ).sort("created_at", -1).limit(12)
            ),
        }
    )


@app.post("/api/vehicles/<vehicle_id>/dispatch")
def approve_vehicle_dispatch(vehicle_id):
    payload = request.get_json(silent=True) or {}
    target_zone = str(payload.get("target_zone", "")).strip()
    if not target_zone.isdigit():
        return jsonify({"error": "Chọn một pickup zone hợp lệ trước khi duyệt"}), 400
    vehicle = vehicle_collection.find_one({"vehicle_id": vehicle_id}, {"_id": 0})
    if not vehicle:
        return jsonify({"error": "Không tìm thấy xe mô phỏng"}), 404
    if vehicle.get("status") != "available":
        return jsonify({"error": "Chỉ có thể điều xe đang rảnh"}), 409
    active_statuses = ["approved", "routing", "en_route"]
    if dispatch_commands_collection.find_one(
        {"vehicle_id": vehicle_id, "status": {"$in": active_statuses}}, {"_id": 1}
    ):
        return jsonify({"error": "Xe này đã có lệnh điều phối đang xử lý"}), 409
    try:
        longitude, latitude = zone_representative_point(target_zone)
    except (OSError, ValueError, KeyError, TypeError, IndexError, ZeroDivisionError) as error:
        return jsonify({"error": str(error) or "Không thể xác định zone đến"}), 400

    command_id = uuid.uuid4().hex
    command = {
        "command_id": command_id,
        "vehicle_id": vehicle_id,
        "from_zone": str(vehicle.get("pickup_zone", "")),
        "target_zone": target_zone,
        "target_longitude": longitude,
        "target_latitude": latitude,
        "status": "approved",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    dispatch_commands_collection.insert_one(command)
    return jsonify({"command_id": command_id, "status": "approved", "vehicle_id": vehicle_id, "target_zone": target_zone}), 202


@app.get("/api/upcoming-hotspots")
def upcoming_hotspots():
    """Forecast future hourly hotspot scores from zone and calendar features."""
    global serving_spark, serving_model
    now = time.monotonic()
    hours = min(max(request.args.get("hours", 3, type=int), 1), 6)
    with forecast_lock:
        if (
            forecast_cache["payload"]
            and forecast_cache["payload"].get("requested_hours") == hours
            and now - forecast_cache["created_at"] < 600
        ):
            return jsonify(forecast_cache["payload"])
        try:
            if serving_model is None:
                serving_spark = (
                    SparkSession.builder.master("local[2]")
                    .appName("taxi-hotspot-dashboard-forecast")
                    .config("spark.sql.session.timeZone", APP_TIMEZONE)
                    .getOrCreate()
                )
                serving_spark.sparkContext.setLogLevel("ERROR")
                model_path = os.getenv("HOTSPOT_MODEL_PATH", "/workspace/models/hotspot_model")
                serving_model = PipelineModel.load(model_path)

            try:
                historical_baseline = json.loads(
                    Path(os.getenv("HISTORICAL_BASELINE_PATH", "/workspace/models/historical_baseline.json")).read_text(encoding="utf-8")
                )
            except (OSError, ValueError):
                historical_baseline = {}

            local_now = datetime.now(ZoneInfo(APP_TIMEZONE)).replace(minute=0, second=0, microsecond=0)
            first_hour = local_now + timedelta(hours=1)
            forecast_hours = [first_hour + timedelta(hours=index) for index in range(hours)]
            geojson_path = Path(__file__).resolve().parent / "static" / "taxi_zones.geojson"
            try:
                geojson = json.loads(geojson_path.read_text(encoding="utf-8"))
                zones = sorted({str(feature["properties"]["LocationID"]) for feature in geojson["features"]}, key=int)
            except (OSError, ValueError, KeyError, TypeError):
                zones = sorted(collection.distinct("pickup_zone"), key=lambda value: int(value) if str(value).isdigit() else 9999)

            input_schema = StructType(
                [
                    StructField("forecast_hour", StringType(), False),
                    StructField("pickup_zone", StringType(), False),
                    StructField("pickup_hour", IntegerType(), False),
                    StructField("pickup_dow", IntegerType(), False),
                ]
            )
            predictions_input = [
                {
                    "forecast_hour": forecast_time.strftime("%Y-%m-%dT%H"),
                    "pickup_zone": str(zone),
                    "pickup_hour": forecast_time.hour,
                    "pickup_dow": (forecast_time.weekday() + 1) % 7,
                }
                for forecast_time in forecast_hours
                for zone in zones
            ]
            scored = (
                serving_model.transform(serving_spark.createDataFrame(predictions_input, input_schema))
                .select("forecast_hour", "pickup_zone", "pickup_hour", "pickup_dow", "predicted_hotspot_score")
                .collect()
            )
            by_hour = {item.strftime("%Y-%m-%dT%H"): [] for item in forecast_hours}
            for row in scored:
                score = max(0.0, min(100.0, float(row.predicted_hotspot_score or 0.0)))
                baseline_key = f"{row.pickup_zone}|{row.pickup_hour}|{row.pickup_dow}"
                baseline = historical_baseline.get(baseline_key)
                if baseline and int(baseline.get("count", 0)) >= 5:
                    mean = float(baseline.get("mean", 0))
                    stddev = float(baseline.get("stddev", 0))
                    threshold = min(100.0, mean + 3 * stddev)
                    sample_count = int(baseline["count"])
                else:
                    mean, stddev, sample_count = 0.0, 0.0, int((baseline or {}).get("count", 0))
                    threshold = 100.0
                by_hour[row.forecast_hour].append(
                    {
                        "zone": row.pickup_zone,
                        "hotspot_score": round(score, 2),
                        "hotspot_level": hotspot_level(score),
                        "alert_flag": "CRITICAL_ANOMALY" if score > threshold else "NORMAL",
                        "anomaly_threshold": round(threshold, 2),
                        "historical_mean_score": round(mean, 2),
                        "historical_stddev_score": round(stddev, 2),
                        "historical_baseline_samples": sample_count,
                    }
                )
            forecasts = [
                {
                    "hour": forecast_time.hour,
                    "label": forecast_time.strftime("%Y-%m-%d %H:00"),
                    "zones": sorted(
                        by_hour[forecast_time.strftime("%Y-%m-%dT%H")],
                        key=lambda row: row["hotspot_score"],
                        reverse=True,
                    ),
                }
                for forecast_time in forecast_hours
            ]
            payload = {
                "available": True,
                "model": "GBTRegressor (zone/hour/weekday)",
                "generated_at": datetime.now(ZoneInfo(APP_TIMEZONE)).isoformat(timespec="seconds"),
                "timezone": APP_TIMEZONE,
                "requested_hours": hours,
                "forecasts": forecasts,
            }
            forecast_cache.update({"created_at": time.monotonic(), "payload": payload})
            return jsonify(payload)
        except Exception as error:
            app.logger.exception("Future hotspot forecast failed")
            return jsonify({"available": False, "message": str(error)[:200], "forecasts": []}), 503

@app.get("/api/dashboard")
def dashboard_data():
    match, _ = match_for_zone()
    latest = list(collection.find(match, {"_id": 0}).sort("event_time", -1).limit(30))
    ranking = list(collection.aggregate([{ "$match": match }, { "$group": {"_id": "$pickup_zone", "value": {"$avg": "$hotspot_score"}}}, { "$sort": {"value": -1} }, { "$limit": 10 }]))
    series = list(collection.aggregate([{ "$match": match }, { "$project": {"bucket": {"$substr": ["$event_time", 0, 13]}, "hotspot_score": 1}}, { "$group": {"_id": "$bucket", "value": {"$avg": "$hotspot_score"}}}, { "$sort": {"_id": -1} }, { "$limit": 24 }, { "$sort": {"_id": 1} }]))
    return jsonify({"summary": {"count": collection.count_documents(match), "zones": len(collection.distinct("pickup_zone", match)), "latest": latest[0].get("event_time") if latest else None}, "ranking": [{"zone": row["_id"], "value": row.get("value", 0)} for row in ranking], "series": [{"bucket": row["_id"], "value": row.get("value", 0)} for row in series], "predictions": latest})


@app.get("/api/predictions")
def predictions():
    limit = min(max(request.args.get("limit", 20, type=int), 1), 100)
    match, _ = match_for_zone()
    return jsonify(list(collection.find(match, {"_id": 0}).sort("event_time", -1).limit(limit)))


@app.get("/api/summary")
def summary():
    match, _ = match_for_zone()
    latest = collection.find_one(match, {"_id": 0, "event_time": 1}, sort=[("event_time", -1)])
    return jsonify({"count": collection.count_documents(match), "zones": len(collection.distinct("pickup_zone", match)), "latest": latest.get("event_time") if latest else None})


if __name__ == "__main__":
    print("[PASS] Dashboard ready", flush=True)
    app.run(host="0.0.0.0", port=8088)
