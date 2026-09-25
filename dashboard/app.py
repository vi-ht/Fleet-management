import json
import os
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, render_template_string, request
from pymongo import MongoClient


app = Flask(__name__)
client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
database = client[os.getenv("MONGO_DATABASE", "taxi")]
collection = database[os.getenv("MONGO_COLLECTION", "hotspot_predictions")]
vehicle_collection = database[os.getenv("MONGO_VEHICLE_COLLECTION", "vehicle_status")]


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
    .cards { display:grid; grid-template-columns:repeat(8,minmax(0,1fr)); gap:11px; margin-bottom:17px; } .card, section { background:var(--panel); border:1px solid var(--line); border-radius:14px; box-shadow:0 3px 12px #15253b08; } .card { position:relative; padding:15px; min-height:103px; overflow:hidden; } .card:first-child { border-top:3px solid var(--yellow); } .label { color:var(--muted); font-size:10px; text-transform:uppercase; letter-spacing:.07em; line-height:1.35; font-weight:700; } .value { margin-top:14px; font-size:26px; line-height:1; font-weight:750; letter-spacing:-.04em; } .value.small { font-size:14px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; } .green { color:var(--green); } .orange { color:var(--orange); } .red { color:var(--red); }
    .map-layout { display:grid; grid-template-columns:minmax(0,1.7fr) minmax(310px,.72fr); gap:15px; margin-bottom:16px; } section { padding:19px; min-width:0; } h2 { margin:0 0 15px; font-size:16px; letter-spacing:-.02em; } .section-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:13px; } .section-head h2 { margin:0; } .hint { color:var(--muted); font-size:12px; }
    #map { height:490px; border:1px solid #e2e7ec; border-radius:10px; overflow:hidden; background:#e7edf1; } .leaflet-popup-content-wrapper, .leaflet-popup-tip { background:#fff; color:var(--text); } .leaflet-popup-content-wrapper { box-shadow:0 5px 20px #17243826; } .leaflet-control-attribution { font-size:9px; }
    .taxi-marker { position:relative; display:grid; place-items:center; width:30px; height:30px; border:2px solid #fff; border-radius:50%; color:#07101e; font-size:17px; line-height:1; box-shadow:0 2px 7px #07101e55; } .taxi-marker.available { background:#75d5b5; } .taxi-marker.occupied { background:#f2bf55; } .taxi-glyph { transform:translateY(1px); } .taxi-arrow { position:absolute; top:-10px; right:-6px; color:var(--navy); font-size:12px; font-weight:900; text-shadow:0 1px 3px #fff; transform-origin:50% 100%; }
    .legend { display:flex; flex-wrap:wrap; gap:13px; margin-top:12px; color:#596b80; font-size:11px; } .dot { display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:5px; } .dot.available { background:#42b991; } .dot.occupied { background:#e6aa2c; } .dot.hotspot { background:#d65d5d; } .boundary-note { color:var(--blue); } #movementStatus { color:#137553; font-weight:700; }
    .fleet-list { max-height:490px; overflow:auto; padding-right:3px; } .vehicle { display:flex; align-items:center; justify-content:space-between; gap:9px; padding:12px 2px; border-bottom:1px solid #edf0f4; } .vehicle:last-child { border-bottom:0; } .vehicle-id { font-weight:700; font-size:13px; } .vehicle-meta { color:var(--muted); font-size:11px; line-height:1.45; margin-top:4px; } .status { padding:5px 8px; border-radius:999px; font-size:10px; font-weight:700; white-space:nowrap; } .status.available { color:#12694f; background:#e6f5ee; } .status.occupied { color:#86570e; background:#fff4d9; }
    .main-grid { display:grid; grid-template-columns:minmax(0,1.35fr) minmax(300px,.65fr); gap:15px; margin-bottom:16px; } .chart-wrap { height:255px; position:relative; } .chart-wrap svg { width:100%; height:100%; overflow:visible; } .axis { stroke:#e1e6ed; stroke-width:1; } .axis-label { fill:#718096; font-size:12px; } .line { fill:none; stroke:#168465; stroke-width:3; stroke-linejoin:round; stroke-linecap:round; } .area { fill:url(#area); opacity:.32; } .chart-note { color:var(--muted); font-size:12px; margin-top:6px; }
    .bars { display:flex; flex-direction:column; gap:14px; padding-top:3px; } .bar-row { display:grid; grid-template-columns:48px 1fr 58px; align-items:center; gap:9px; font-size:12px; } .bar-bg { height:9px; border-radius:99px; background:#edf0f4; overflow:hidden; } .bar-fill { height:100%; border-radius:99px; background:linear-gradient(90deg,#4e9bc5,var(--green)); } .bar-value { text-align:right; color:var(--muted); font-variant-numeric:tabular-nums; }
    .dispatch-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; } .dispatch-card { border:1px solid #e7ebf0; background:#fbfcfd; border-radius:10px; padding:13px; } .dispatch-card strong { display:block; color:#17694f; margin-bottom:6px; font-size:13px; } .dispatch-card span { color:var(--muted); font-size:12px; line-height:1.5; } .forecast-grid { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:10px; } .forecast-card { border:1px solid #e7ebf0; background:#fbfcfd; border-radius:10px; padding:14px; min-height:135px; } .forecast-card h3 { margin:0 0 10px; color:#315e80; font-size:14px; } .forecast-zone { display:flex; justify-content:space-between; gap:8px; padding:7px 0; border-bottom:1px solid #edf0f4; font-size:12px; } .forecast-zone b { color:var(--text); } .forecast-zone span { color:#8a5a11; font-variant-numeric:tabular-nums; } .forecast-note { color:var(--muted); font-size:12px; margin:-3px 0 12px; }
    .table-wrap { max-height:350px; overflow:auto; } table { width:100%; border-collapse:collapse; font-size:12px; } th,td { padding:11px 9px; text-align:left; border-bottom:1px solid #edf0f4; white-space:nowrap; } th { position:sticky; top:0; background:#f7f9fb; color:#627187; font-weight:700; } tbody tr:hover { background:#f8fafc; } .pill { padding:4px 8px; border-radius:999px; background:#e7f5ef; color:#17694f; font-size:10px; font-weight:700; }
    .pipeline { display:grid; grid-template-columns:repeat(6,1fr); gap:8px; } .stage { border:1px solid #e7ebf0; background:#fbfcfd; border-radius:9px; padding:10px 8px; text-align:center; color:#58697e; font-size:11px; font-weight:600; } .stage b { display:block; color:var(--green); font-size:17px; margin-bottom:4px; } footer { color:var(--muted); font-size:11px; margin-top:15px; }
    @media (max-width:1250px) { .cards { grid-template-columns:repeat(4,minmax(0,1fr)); } .map-layout { grid-template-columns:minmax(0,1.4fr) minmax(285px,.8fr); } } @media (max-width:950px) { main { padding:22px 18px 38px; } .cards { grid-template-columns:repeat(4,minmax(0,1fr)); } .map-layout,.main-grid { grid-template-columns:1fr; } .fleet-list { max-height:360px; } #map { height:420px; } }
    @media (max-width:650px) { main { padding:18px 12px 30px; } header { align-items:flex-start; } .brand { gap:11px; } .logo { width:40px; height:40px; } .eyebrow { font-size:9px; } h1 { font-size:24px; } .subtitle { font-size:12px; } .live { padding:7px 9px; font-size:0; } .live::after { content:"LIVE"; font-size:10px; } nav { margin-left:-2px; margin-right:-2px; } nav a { padding:9px 11px; font-size:12px; } .toolbar { gap:9px; padding:11px; } .toolbar label:first-child { width:100%; } select { flex:1; min-width:0; } .refresh-info { width:100%; margin-left:0; } .cards { grid-template-columns:repeat(2,minmax(0,1fr)); gap:8px; } .card { min-height:90px; padding:12px; } .label { font-size:10px; } .value { margin-top:12px; font-size:23px; } section { padding:15px; } #map { height:350px; } .section-head { align-items:flex-start; } .hint { text-align:right; } .dispatch-grid,.forecast-grid { grid-template-columns:1fr; } .dispatch-card { padding:12px; } .forecast-card { min-height:auto; } .pipeline { grid-template-columns:repeat(3,1fr); } .table-wrap { margin:0 -5px; } }
  </style>
</head>
<body><main>
  <header><div class="brand"><div class="logo">T</div><div><div class="eyebrow">Fleet management · realtime operations</div><h1>Điều hành đội xe NYC</h1><p class="subtitle">Theo dõi nhu cầu taxi, vị trí xe và dự báo AI theo thời gian gần thực</p></div></div><div class="live" id="connectionStatus" data-state="loading" role="status" aria-live="polite"><span id="connection">Đang kết nối</span></div></header>
  <nav><a class="active" href="#tong-quan">Tổng quan</a><a href="#ban-do">Bản đồ & xe</a><a href="#du-bao">Dự báo AI</a><a href="#dieu-phoi">Điều phối taxi</a><a href="#he-thong">Pipeline hệ thống</a></nav>
  <div class="toolbar"><label for="zone">Lọc dự báo theo khu vực đón</label><select id="zone"><option value="">Tất cả khu vực</option></select><button id="refresh" type="button">↻ Cập nhật</button><label><input id="auto" type="checkbox" checked> Tự động cập nhật</label><span class="refresh-info">Cập nhật lần cuối: <span id="updated">Chưa có dữ liệu</span></span></div>

  <div id="tong-quan" class="cards"><div class="card"><div class="label">Dashboard API</div><div class="value small" id="pipeline">Đang kết nối</div></div><div class="card"><div class="label">Xe trên hệ thống</div><div class="value" id="fleetTotal">—</div></div><div class="card"><div class="label">Xe đang di chuyển</div><div class="value green" id="fleetMoving">—</div></div><div class="card"><div class="label">Xe đang rảnh</div><div class="value green" id="fleetAvailable">—</div></div><div class="card"><div class="label">Xe đang có khách</div><div class="value orange" id="fleetOccupied">—</div></div><div class="card"><div class="label">Bản ghi dự báo</div><div class="value" id="count">—</div></div><div class="card"><div class="label">Khu vực có dữ liệu</div><div class="value" id="zones">—</div></div><div class="card"><div class="label">RMSE / MAE · holdout</div><div class="value small" id="modelMetrics">Chưa có chỉ số</div></div></div>

  <div id="ban-do" class="map-layout"><section><div class="section-head"><h2>Bản đồ xe và khu vực có nhu cầu</h2><span class="hint">Vùng đỏ đậm hơn = hotspot cao hơn · bấm vùng đỏ để xem lý do</span></div><div id="map"></div><div class="legend"><span><i class="dot available"></i>Đang tái bố trí</span><span><i class="dot occupied"></i>Đang có khách</span><span><i class="dot hotspot"></i>Khu vực có nhu cầu</span><span class="boundary-note">▧ Ranh giới zone mô phỏng</span><span id="movementStatus">—</span><span id="latestVehicle">—</span></div></section><section><div class="section-head"><h2>Danh sách xe</h2><span class="hint" id="fleetCount">—</span></div><div id="fleetList" class="fleet-list"><div class="chart-note">Đang tải dữ liệu xe...</div></div></section></div>

  <div id="du-bao" class="main-grid"><section><h2>Mức độ nóng theo giờ</h2><div class="chart-wrap"><svg id="trend" role="img" aria-label="Điểm nóng trung bình theo giờ, thang 0 đến 100" viewBox="0 0 760 240" preserveAspectRatio="none"><defs><linearGradient id="area" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="#55d6be" stop-opacity=".45"/><stop offset="1" stop-color="#55d6be" stop-opacity="0"/></linearGradient></defs><path class="axis" d="M30 210H750M30 115H750M30 20H750"/><text class="axis-label" x="2" y="24">100</text><text class="axis-label" x="8" y="119">50</text><text class="axis-label" x="18" y="214">0</text><text id="trendEmpty" class="axis-label" x="390" y="118" text-anchor="middle">Chưa có dữ liệu theo giờ</text><path id="areaPath" class="area"/><path id="linePath" class="line"/></svg></div><div class="chart-note">Điểm nóng trung bình theo thang 0–100; 100 là mức nóng nhất.</div></section><section><h2>Top điểm nóng</h2><div id="ranking" class="bars"><div class="chart-note">Đang tải...</div></div></section></div>
  <section id="dieu-phoi" style="margin-bottom:16px"><div class="section-head"><h2>Điều phối taxi đề xuất</h2><span class="hint">Ghép xe rảnh vào điểm nóng</span></div><div id="dispatch" class="dispatch-grid"><div class="chart-note">Đang tính đề xuất...</div></div></section>
  <section id="diem-nong-sap-toi" style="margin-bottom:16px"><div class="section-head"><h2>Điểm nóng sắp tới</h2><span class="hint" id="forecastHeadline">Đang đọc giờ hiện tại...</span></div><div class="forecast-note">Xếp hạng các khu vực có điểm hotspot cao nhất trong từng khung giờ kế tiếp, dựa trên dữ liệu trong hệ thống.</div><div id="upcomingHotspots" class="forecast-grid"><div class="chart-note">Đang tính dự báo theo giờ...</div></div></section>
  <section id="he-thong" style="margin-bottom:16px"><div class="section-head"><h2>Các tầng dữ liệu</h2><span class="hint">Kiến trúc đang sử dụng · trạng thái dịch vụ chưa được probe riêng</span></div><div class="pipeline"><div class="stage"><b>01</b>HDFS</div><div class="stage"><b>02</b>MapReduce</div><div class="stage"><b>03</b>Parquet ETL</div><div class="stage"><b>04</b>Model AI</div><div class="stage"><b>05</b>Kafka realtime</div><div class="stage"><b>06</b>MongoDB</div></div></section>
  <section><h2>Cập nhật điểm nóng mới nhất</h2><div class="table-wrap"><table><thead><tr><th>Mã sự kiện</th><th>Thời gian</th><th>Khu vực</th><th>Mức nóng</th><th>Phân loại</th><th>Trạng thái</th></tr></thead><tbody id="rows"><tr><td colspan="6">Đang tải...</td></tr></tbody></table></div></section>
  <footer>Simulation chạy liên tục: producer → Kafka → AI inference → MongoDB · Tự động cập nhật 3 giây</footer>
</main>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); const zoneSelect=document.querySelector('#zone'); let map,markers,zoneBoundaryLayer,knownZones=[];
if(typeof L!=='undefined'){map=L.map('map').setView([40.73,-73.96],11);L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);map.createPane('demandPane');map.getPane('demandPane').style.zIndex=525;zoneBoundaryLayer=L.layerGroup().addTo(map);markers=L.layerGroup().addTo(map);}else{document.querySelector('#map').innerHTML='<div class="chart-note" style="padding:30px">Không tải được bản đồ nền. Kiểm tra kết nối Internet để tải OpenStreetMap.</div>';}
async function loadZones(){const zones=await fetch('/api/zones').then(r=>r.json());knownZones=zones;zones.forEach(z=>{const o=document.createElement('option');o.value=z;o.textContent=`Khu vực ${z}`;zoneSelect.appendChild(o);});renderZoneBoundaries([]);}
function drawTrend(series){const values=series.map(x=>Number(x.value||0)),max=100,left=30,top=20,width=720,height=190;document.querySelector('#trendEmpty').style.display=values.length?'none':'block';const points=values.map((v,i)=>`${left+(values.length===1?width/2:i*width/(values.length-1))},${top+height-(Math.max(0,Math.min(max,v))/max)*height}`).join(' ');document.querySelector('#linePath').setAttribute('d',points?`M ${points}`:'');document.querySelector('#areaPath').setAttribute('d',points?`M ${left} ${top+height} L ${points} L ${left+width} ${top+height} Z`:'');}
 function renderDispatch(data){document.querySelector('#dispatch').innerHTML=data.recommendations.length?data.recommendations.map(x=>`<div class="dispatch-card"><strong>🚕 ${esc(x.vehicle_id)} → Khu ${esc(x.target_zone)}</strong><span>Điểm nóng: ${Number(x.hotspot_score).toFixed(0)}/100 · Xe hiện tại: Khu ${esc(x.from_zone)} · ${x.reason}</span></div>`).join(''):'<div class="chart-note">Chưa có xe rảnh hoặc chưa có điểm nóng.</div>';}
 function renderZoneBoundaries(hotspots){
  if(!zoneBoundaryLayer||!knownZones.length)return;
  zoneBoundaryLayer.clearLayers();
  const hotspotMap={},hotspotBounds=L.latLngBounds();
  hotspots.forEach(item=>{hotspotMap[String(item.zone)]={hotspot_score:Number(item.hotspot_score||0),sample_count:Number(item.sample_count||0)};});
  const minLat=40.68,minLon=-74.02,latStep=.0138,lonStep=.007,columns=20;
  knownZones.forEach(zone=>{
   const index=Math.max(0,Number(zone)-4),row=Math.floor(index/columns),column=index%columns;
   const lat=minLat+row*latStep,lon=minLon+column*lonStep,hotspot=hotspotMap[String(zone)],score=hotspot?.hotspot_score||0;
   const fillColor=score>=75?'#dc2626':score>=50?'#ef4444':score>=25?'#f87171':'#fca5a5';
   const corners=[[lat,lon],[lat+latStep,lon],[lat+latStep,lon+lonStep],[lat,lon+lonStep]];
   const polygon=L.polygon(corners,{pane:'demandPane',color:hotspot?'#b91c1c':'#436181',weight:hotspot?2:.7,fillColor:hotspot?fillColor:'#1c3858',fillOpacity:hotspot?.4:.015,interactive:true});
   if(hotspot){
    const level=score>=75?'Rất nóng':score>=50?'Nóng':score>=25?'Trung bình':'Thấp';
    const samples=Number(hotspot.sample_count||0).toLocaleString();
    polygon.bindTooltip(`Khu ${esc(zone)} · Hotspot ${score.toFixed(0)}/100`,{sticky:true,direction:'top'});
    polygon.bindPopup(`<strong>Khu ${esc(zone)} · ${level}</strong><p><b>Điểm hotspot trung bình: ${score.toFixed(0)}/100</b></p><p>Đây là trung bình của ${samples} bản ghi dự báo đã lưu cho khu vực này.</p><p><b>Vì sao nhu cầu cao?</b> Model học mật độ chuyến đón NYC TLC theo khu vực, giờ và thứ trong tuần. Điểm cao nghĩa là model dự đoán khu này thường có nhiều lượt đón hơn các khu khác trong cùng khung giờ và thứ; đây là điểm tương đối, không phải số chuyến thực tế.</p>`,{maxWidth:300});
   }else{
    polygon.bindTooltip(`Khu ${esc(zone)} · Chưa nằm trong top hotspot hiện tại`,{sticky:true,direction:'top'});
   }
   polygon.addTo(zoneBoundaryLayer);
   if(hotspot){
    hotspotBounds.extend(corners);
    L.circleMarker([lat+latStep/2,lon+lonStep/2],{pane:'demandPane',radius:24,color:'#b91c1c',weight:1,fillColor,fillOpacity:.3,interactive:false}).addTo(zoneBoundaryLayer);
   }
  });
  if(!window.mapHotspotsFitted&&hotspotBounds.isValid()){
   const visibleBounds=map.getBounds().extend(hotspotBounds);
   map.fitBounds(visibleBounds,{padding:[24,24],maxZoom:13});
   window.mapHotspotsFitted=true;
  }
 }
  function renderUpcoming(data){const headline=document.querySelector('#forecastHeadline');headline.textContent=`Đồng hồ mô phỏng ${esc(data.current_label)} · điểm nóng 3 giờ tới`;document.querySelector('#upcomingHotspots').innerHTML=data.forecasts.map(window=>`<div class="forecast-card"><h3>${esc(window.label)}</h3>${window.zones.length?window.zones.map(zone=>`<div class="forecast-zone"><b>Khu ${esc(zone.zone)}</b><span>${Number(zone.hotspot_score).toFixed(0)}/100 · ${esc(zone.hotspot_level)}</span></div>`).join(''):'<div class="chart-note">Chưa có dữ liệu cho khung giờ này.</div>'}</div>`).join('');}
 function renderFleet(items){const available=items.filter(x=>x.status==='available').length,occupied=items.filter(x=>x.status==='occupied').length;document.querySelector('#fleetTotal').textContent=items.length;document.querySelector('#fleetAvailable').textContent=available;document.querySelector('#fleetOccupied').textContent=occupied;document.querySelector('#fleetCount').textContent=`${items.length} xe`;document.querySelector('#fleetList').innerHTML=items.length?items.map(v=>`<div class="vehicle"><div><div class="vehicle-id">${esc(v.vehicle_id)}</div><div class="vehicle-meta">Khu ${esc(v.pickup_zone)} → ${esc(v.dropoff_zone)} · ${Number(v.speed_kmh||0)} km/h</div></div><span class="status ${v.status}">${v.status==='available'?'Rảnh':'Có khách'}</span></div>`).join(''):'<div class="chart-note">Chưa có snapshot xe.</div>';if(markers){markers.clearLayers();const bounds=[];items.forEach(v=>{const position=[Number(v.latitude),Number(v.longitude)];bounds.push(position);const icon=L.divIcon({className:'',html:`<div class="taxi-marker ${v.status}" title="${esc(v.vehicle_id)}">🚕</div>`,iconSize:[42,42],iconAnchor:[21,21]});L.marker(position,{icon,zIndexOffset:1000}).bindPopup(`<b>${esc(v.vehicle_id)}</b><br>Trạng thái: ${v.status==='available'?'Xe rảnh':'Đang có khách'}<br>Khu vực: ${esc(v.pickup_zone)} → ${esc(v.dropoff_zone)}<br>Tốc độ: ${Number(v.speed_kmh||0)} km/h`).addTo(markers);});if(bounds.length && !window.mapHasFitted){map.fitBounds(bounds,{padding:[30,30],maxZoom:12});window.mapHasFitted=true;}}document.querySelector('#latestVehicle').textContent=items.length?`Snapshot #${items[0].simulation_cycle} · ${items[0].updated_at}`:'Chưa có snapshot';}
  let refreshInProgress=false;
async function fetchJson(url){const response=await fetch(url);if(!response.ok)throw new Error(`HTTP ${response.status} from ${url}`);return response.json();}
async function loadModelMetrics(){try{const metrics=await fetchJson('/api/model-metrics'),value=document.querySelector('#modelMetrics');value.textContent=metrics.available?`${Number(metrics.rmse).toFixed(1)} / ${Number(metrics.mae).toFixed(1)}`:'Chưa có chỉ số';value.title=metrics.available?`${metrics.model} · ${Number(metrics.evaluation_rows).toLocaleString()} holdout rows · ${metrics.evaluation_method}`:'Chạy bước train model để tạo kết quả holdout';}catch(error){document.querySelector('#modelMetrics').textContent='Không khả dụng';}}
  async function refresh(){if(refreshInProgress)return;refreshInProgress=true;const connection=document.querySelector('#connection'),connectionStatus=document.querySelector('#connectionStatus'),pipeline=document.querySelector('#pipeline'),refreshButton=document.querySelector('#refresh');refreshButton.disabled=true;connection.textContent='Đang cập nhật';connectionStatus.dataset.state='loading';try{const zone=encodeURIComponent(zoneSelect.value),[data,fleet,dispatch,upcoming]=await Promise.all([fetchJson(`/api/dashboard?zone=${zone}`),fetchJson('/api/vehicles'),fetchJson('/api/dispatch'),fetchJson('/api/upcoming-hotspots?hours=3')]);connection.textContent='Đang hoạt động';connectionStatus.dataset.state='ok';pipeline.textContent='Đang hoạt động';pipeline.className='value small green';document.querySelector('#count').textContent=Number(data.summary.count).toLocaleString();document.querySelector('#zones').textContent=Number(data.summary.zones).toLocaleString();document.querySelector('#updated').textContent=`${new Date().toLocaleTimeString()} · đồng hồ ${upcoming.current_label}`;renderFleet(fleet.vehicles);renderDispatch(dispatch);renderZoneBoundaries(dispatch.hotspots);renderUpcoming(upcoming);drawTrend(data.series);const max=Math.max(...data.ranking.map(x=>Number(x.value||0)),1);document.querySelector('#ranking').innerHTML=data.ranking.length?data.ranking.map(x=>`<div class="bar-row"><span>Khu ${esc(x.zone)}</span><div class="bar-bg"><div class="bar-fill" style="width:${Math.max(3,Number(x.value)/max*100)}"></div></div><span class="bar-value">${Number(x.value).toFixed(0)}/100</span></div>`).join(''):'<div class="chart-note">Chưa có dữ liệu để xếp hạng.</div>';document.querySelector('#rows').innerHTML=data.predictions.length?data.predictions.map(p=>`<tr><td>${esc(p.event_id)}</td><td>${esc(p.event_time)}</td><td>Khu ${esc(p.pickup_zone)}</td><td><b>${Number(p.hotspot_score||0).toFixed(0)}/100</b></td><td>${esc(p.hotspot_level||'—')}</td><td><span class="pill">Đã suy luận</span></td></tr>`).join(''):'<tr><td colspan="6">Chưa có bản ghi dự báo.</td></tr>';document.querySelector('#updated').title=`Lần làm mới thành công: ${new Date().toLocaleString()}`;}catch(error){console.error('Dashboard refresh failed',error);connection.textContent='Mất kết nối';connectionStatus.dataset.state='error';pipeline.textContent='Không truy cập được';pipeline.className='value small red';document.querySelector('#updated').textContent='Làm mới thất bại';}finally{refreshInProgress=false;refreshButton.disabled=false;}}
zoneSelect.addEventListener('change',refresh);document.querySelector('#refresh').addEventListener('click',()=>{refresh();loadModelMetrics();});loadZones().then(refresh).catch(error=>{console.error('Zone list failed',error);refresh();});loadModelMetrics();setInterval(()=>{if(document.querySelector('#auto').checked){refresh();loadModelMetrics();}},3000);

// Keep one Leaflet marker per vehicle and interpolate between Kafka snapshots.
// This makes the simulation visibly continuous instead of replacing all markers
// with a new static snapshot every refresh.
const liveMarkers={};
function liveVehicleIcon(vehicle){const heading=Number(vehicle.heading_degrees||0);return L.divIcon({className:'',html:`<div class="taxi-marker ${vehicle.status}" title="${esc(vehicle.vehicle_id)}"><span class="taxi-glyph">🚕</span><span class="taxi-arrow" style="transform:rotate(${heading}deg)">▲</span></div>`,iconSize:[34,34],iconAnchor:[17,17]});}
function liveVehiclePopup(vehicle){const status=vehicle.status==='occupied'?'Đang có khách':'Đang tái bố trí tới điểm nóng';return `<b>${esc(vehicle.vehicle_id)}</b><br>Trạng thái: ${status}<br>Tuyến: Khu ${esc(vehicle.pickup_zone)} → Khu ${esc(vehicle.dropoff_zone)}<br>Tốc độ: ${Number(vehicle.speed_kmh||0).toFixed(1)} km/h · Hướng: ${Number(vehicle.heading_degrees||0).toFixed(0)}°<br>Tiến độ: ${Number(vehicle.progress_pct||0).toFixed(0)}% · ETA: ${Number(vehicle.eta_minutes||0).toFixed(1)} phút`;}
function animateLiveMarker(marker,target){if(marker._motionFrame)cancelAnimationFrame(marker._motionFrame);const start=marker.getLatLng(),started=performance.now(),duration=1800;const tick=now=>{const progress=Math.min(1,(now-started)/duration);marker.setLatLng([start.lat+(target[0]-start.lat)*progress,start.lng+(target[1]-start.lng)*progress]);if(progress<1)marker._motionFrame=requestAnimationFrame(tick);};marker._motionFrame=requestAnimationFrame(tick);}
renderFleet=function renderLiveFleet(items){const available=items.filter(x=>x.status==='available').length,occupied=items.filter(x=>x.status==='occupied').length,moving=items.filter(x=>Number(x.speed_kmh||0)>0).length;document.querySelector('#fleetTotal').textContent=items.length;document.querySelector('#fleetMoving').textContent=moving;document.querySelector('#fleetAvailable').textContent=available;document.querySelector('#fleetOccupied').textContent=occupied;document.querySelector('#fleetCount').textContent=`${items.length} xe · ${moving} đang chạy`;document.querySelector('#movementStatus').textContent=`LIVE · ${moving}/${items.length} xe đang chạy`;document.querySelector('#fleetList').innerHTML=items.length?items.map(v=>`<div class="vehicle"><div><div class="vehicle-id">${esc(v.vehicle_id)}</div><div class="vehicle-meta">${esc(v.route_status||'Đang chạy')} · Khu ${esc(v.pickup_zone)} → ${esc(v.dropoff_zone)} · ${Number(v.speed_kmh||0).toFixed(1)} km/h · ETA ${Number(v.eta_minutes||0).toFixed(1)} phút</div></div><span class="status ${v.status}">${v.status==='available'?'Tái bố trí':'Có khách'}</span></div>`).join(''):'<div class="chart-note">Chưa có snapshot xe.</div>';if(markers){const bounds=[];const activeIds=new Set(items.map(v=>v.vehicle_id));items.forEach(vehicle=>{const target=[Number(vehicle.latitude),Number(vehicle.longitude)];bounds.push(target);let marker=liveMarkers[vehicle.vehicle_id];if(!marker){marker=L.marker(target,{icon:liveVehicleIcon(vehicle),zIndexOffset:1000}).addTo(markers);liveMarkers[vehicle.vehicle_id]=marker;}else{marker.setIcon(liveVehicleIcon(vehicle));animateLiveMarker(marker,target);}marker.bindPopup(liveVehiclePopup(vehicle));});Object.keys(liveMarkers).forEach(id=>{if(!activeIds.has(id)){if(liveMarkers[id]._motionFrame)cancelAnimationFrame(liveMarkers[id]._motionFrame);markers.removeLayer(liveMarkers[id]);delete liveMarkers[id];}});if(bounds.length&&!window.mapHasFitted){map.fitBounds(bounds,{padding:[30,30],maxZoom:12});window.mapHasFitted=true;}}document.querySelector('#latestVehicle').textContent=items.length?`Snapshot #${items[0].simulation_cycle} · ${items[0].updated_at}`:'Chưa có snapshot';};
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
    available = list(
        vehicle_collection.find(
            {"status": "available"},
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
                "reason": "Khu vực có khả năng nóng cao",
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
        }
    )


@app.get("/api/upcoming-hotspots")
def upcoming_hotspots():
    """Return the next hourly hotspot windows from the learned model."""
    start_hour = request.args.get("start_hour", type=int)
    if start_hour is None:
        latest = collection.find_one({}, {"event_time": 1}, sort=[("event_time", -1)])
        latest_time = latest.get("event_time") if latest else None
        try:
            start_hour = datetime.fromisoformat(latest_time).hour if latest_time else datetime.now().hour
        except (TypeError, ValueError):
            start_hour = datetime.now().hour
    start_hour = start_hour % 24
    window_count = min(max(request.args.get("hours", 3, type=int), 1), 6)
    target_hours = [(start_hour + offset) % 24 for offset in range(1, window_count + 1)]
    rows = list(
        collection.aggregate(
            [
                {
                    "$project": {
                        "pickup_zone": 1,
                        "hotspot_score": {"$ifNull": ["$hotspot_score", 0]},
                        "event_hour": {
                            "$convert": {
                                "input": {"$substrBytes": ["$event_time", 11, 2]},
                                "to": "int",
                                "onError": -1,
                                "onNull": -1,
                            }
                        },
                    }
                },
                {"$match": {"event_hour": {"$in": target_hours}}},
                {
                    "$group": {
                        "_id": {"hour": "$event_hour", "zone": "$pickup_zone"},
                        "hotspot_score": {"$avg": "$hotspot_score"},
                    }
                },
                {"$sort": {"_id.hour": 1, "hotspot_score": -1}},
            ]
        )
    )
    grouped = {hour: [] for hour in target_hours}
    for row in rows:
        hour = row.get("_id", {}).get("hour")
        if hour in grouped and len(grouped[hour]) < 5:
            grouped[hour].append(
                {
                    "zone": row.get("_id", {}).get("zone"),
                    "hotspot_score": round(float(row.get("hotspot_score", 0)), 2),
                    "hotspot_level": hotspot_level(row.get("hotspot_score", 0)),
                }
            )
    fallback = []
    if any(not zones for zones in grouped.values()):
        fallback = list(
            collection.aggregate(
                [
                    {"$group": {"_id": "$pickup_zone", "hotspot_score": {"$avg": "$hotspot_score"}}},
                    {"$sort": {"hotspot_score": -1}},
                    {"$limit": 5},
                ]
            )
        )
        fallback = [
            {
                "zone": row.get("_id"),
                "hotspot_score": round(float(row.get("hotspot_score", 0)), 2),
                "hotspot_level": hotspot_level(row.get("hotspot_score", 0)),
            }
            for row in fallback
        ]
    forecasts = []
    for hour in target_hours:
        forecasts.append(
            {
                "hour": hour,
                "label": f"{hour:02d}:00–{(hour + 1) % 24:02d}:00",
                "zones": grouped[hour] or fallback,
            }
        )
    return jsonify(
        {
            "current_hour": start_hour,
            "current_label": f"{start_hour:02d}:00",
            "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
            "forecasts": forecasts,
        }
    )


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
