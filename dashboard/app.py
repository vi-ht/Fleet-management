import os

from flask import Flask, jsonify, render_template_string, request
from pymongo import MongoClient


app = Flask(__name__)
client = MongoClient(os.getenv("MONGO_URI", "mongodb://mongodb:27017"))
database = client[os.getenv("MONGO_DATABASE", "taxi")]
collection = database[os.getenv("MONGO_COLLECTION", "demand_predictions")]
vehicle_collection = database[os.getenv("MONGO_VEHICLE_COLLECTION", "vehicle_status")]


PAGE = """
<!doctype html>
<html lang="vi">
<head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Điều hành đội xe NYC</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
  <style>
    :root { color-scheme:dark; --panel:#111a2d; --line:#253653; --text:#eff5ff; --muted:#92a2bd; --green:#4ee0b3; --blue:#66aaff; --orange:#ffbd69; --red:#ff7f8e; }
    * { box-sizing:border-box; } body { margin:0; color:var(--text); font:14px Inter,Segoe UI,Arial,sans-serif; background:radial-gradient(circle at 10% 0,#18355c 0,#0a1020 42%); }
    main { max-width:1380px; margin:auto; padding:22px 22px 42px; } header { display:flex; align-items:center; justify-content:space-between; gap:18px; margin-bottom:16px; }
    .brand { display:flex; gap:13px; align-items:center; } .logo { display:grid; place-items:center; width:43px; height:43px; border-radius:12px; color:#081421; background:var(--green); font-size:22px; font-weight:900; } .eyebrow { color:var(--green); font-size:11px; font-weight:700; letter-spacing:.1em; text-transform:uppercase; } h1 { margin:4px 0; font-size:clamp(23px,3vw,34px); letter-spacing:-.04em; } .subtitle { margin:0; color:var(--muted); font-size:13px; }
    .live { padding:8px 13px; border:1px solid #257b6a; border-radius:999px; background:#10332f; color:var(--green); white-space:nowrap; }
    nav { display:flex; gap:4px; overflow:auto; padding:7px; margin-bottom:16px; background:#0e1729cc; border:1px solid var(--line); border-radius:12px; } nav a { color:var(--muted); text-decoration:none; padding:9px 14px; border-radius:8px; white-space:nowrap; } nav a:hover, nav a.active { background:#1a3150; color:var(--text); }
    .toolbar { display:flex; flex-wrap:wrap; align-items:center; gap:10px; margin-bottom:16px; padding:12px; background:#0e1729cc; border:1px solid var(--line); border-radius:12px; } label { color:var(--muted); font-size:13px; } select, button { border:1px solid #345071; border-radius:8px; color:var(--text); background:#15243b; padding:9px 12px; } button { cursor:pointer; font-weight:600; } button:hover { border-color:var(--green); } .refresh-info { margin-left:auto; color:var(--muted); font-size:12px; }
    .cards { display:grid; grid-template-columns:repeat(6,1fr); gap:11px; margin-bottom:16px; } .card, section { background:linear-gradient(145deg,#14223aee,#10182aee); border:1px solid var(--line); border-radius:15px; box-shadow:0 12px 30px #03071255; } .card { padding:15px; min-height:100px; } .label { color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.07em; } .value { margin-top:13px; font-size:25px; font-weight:750; } .value.small { font-size:14px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; } .green { color:var(--green); } .orange { color:var(--orange); } .red { color:var(--red); }
    .map-layout { display:grid; grid-template-columns:1.55fr .75fr; gap:15px; margin-bottom:16px; } section { padding:17px; min-width:0; } h2 { margin:0 0 14px; font-size:17px; } .section-head { display:flex; align-items:center; justify-content:space-between; gap:10px; } .hint { color:var(--muted); font-size:12px; }
    #map { height:445px; border-radius:11px; overflow:hidden; background:#18263b; } .leaflet-popup-content-wrapper, .leaflet-popup-tip { background:#14223a; color:#eff5ff; } .leaflet-control-attribution { font-size:9px; }
    .taxi-marker { display:grid; place-items:center; width:42px; height:42px; border:3px solid #fff; border-radius:50%; color:#07101e; font-size:25px; line-height:1; box-shadow:0 3px 12px #07101ecc; } .taxi-marker.available { background:#4ee0b3; } .taxi-marker.occupied { background:#ffbd69; }
    .legend { display:flex; gap:15px; margin-top:10px; color:var(--muted); font-size:12px; } .dot { display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:5px; } .dot.available { background:var(--green); } .dot.occupied { background:var(--orange); }
    .fleet-list { max-height:445px; overflow:auto; } .vehicle { display:flex; align-items:center; justify-content:space-between; gap:9px; padding:11px 3px; border-bottom:1px solid #24334d; } .vehicle-id { font-weight:650; } .vehicle-meta { color:var(--muted); font-size:12px; margin-top:4px; } .status { padding:4px 7px; border-radius:999px; font-size:10px; } .status.available { color:var(--green); background:#12352f; } .status.occupied { color:var(--orange); background:#46341b; }
    .main-grid { display:grid; grid-template-columns:1.25fr .75fr; gap:15px; margin-bottom:16px; } .chart-wrap { height:250px; position:relative; } svg { width:100%; height:100%; overflow:visible; } .axis { stroke:#304362; stroke-width:1; } .line { fill:none; stroke:var(--green); stroke-width:3; stroke-linejoin:round; stroke-linecap:round; } .area { fill:url(#area); opacity:.55; } .chart-note { color:var(--muted); font-size:12px; margin-top:6px; }
    .bars { display:flex; flex-direction:column; gap:11px; } .bar-row { display:grid; grid-template-columns:48px 1fr 58px; align-items:center; gap:9px; font-size:12px; } .bar-bg { height:10px; border-radius:99px; background:#243653; overflow:hidden; } .bar-fill { height:100%; border-radius:99px; background:linear-gradient(90deg,var(--blue),var(--green)); } .bar-value { text-align:right; color:var(--muted); }
    .table-wrap { max-height:350px; overflow:auto; } table { width:100%; border-collapse:collapse; font-size:13px; } th,td { padding:10px 8px; text-align:left; border-bottom:1px solid #24334d; white-space:nowrap; } th { position:sticky; top:0; background:var(--panel); color:var(--muted); font-weight:600; } tbody tr:hover { background:#1a2b46; } .pill { padding:4px 8px; border-radius:999px; background:#12352f; color:var(--green); font-size:11px; }
    .pipeline { display:grid; grid-template-columns:repeat(6,1fr); gap:8px; } .stage { border:1px solid #25405d; background:#101b2e; border-radius:10px; padding:10px 8px; text-align:center; color:var(--muted); font-size:12px; } .stage b { display:block; color:var(--green); font-size:18px; margin-bottom:4px; } footer { color:var(--muted); font-size:12px; margin-top:15px; }
    @media (max-width:1050px) { .cards { grid-template-columns:repeat(3,1fr); } .map-layout,.main-grid { grid-template-columns:1fr; } } @media (max-width:650px) { main { padding:16px 11px; } header { align-items:flex-start; } .subtitle { display:none; } .cards { grid-template-columns:repeat(2,1fr); gap:8px; } .pipeline { grid-template-columns:repeat(3,1fr); } .refresh-info { width:100%; margin-left:0; } #map { height:350px; } }
  </style>
</head>
<body><main>
  <header><div class="brand"><div class="logo">T</div><div><div class="eyebrow">Fleet management · realtime operations</div><h1>Điều hành đội xe NYC</h1><p class="subtitle">Theo dõi nhu cầu taxi, vị trí xe và dự báo AI theo thời gian gần thực</p></div></div><div class="live">● <span id="connection">Đang kết nối</span></div></header>
  <nav><a class="active" href="#tong-quan">Tổng quan</a><a href="#ban-do">Bản đồ & xe</a><a href="#du-bao">Dự báo AI</a><a href="#he-thong">Pipeline hệ thống</a></nav>
  <div class="toolbar"><label for="zone">Khu vực đón</label><select id="zone"><option value="">Tất cả khu vực</option></select><button id="refresh">↻ Cập nhật</button><label><input id="auto" type="checkbox" checked> Tự động cập nhật</label><span class="refresh-info">Cập nhật lần cuối: <span id="updated">—</span></span></div>

  <div id="tong-quan" class="cards"><div class="card"><div class="label">Trạng thái pipeline</div><div class="value green" id="pipeline">Ổn định</div></div><div class="card"><div class="label">Xe trên hệ thống</div><div class="value" id="fleetTotal">—</div></div><div class="card"><div class="label">Xe đang rảnh</div><div class="value green" id="fleetAvailable">—</div></div><div class="card"><div class="label">Xe đang có khách</div><div class="value orange" id="fleetOccupied">—</div></div><div class="card"><div class="label">Dự báo đã phục vụ</div><div class="value" id="count">—</div></div><div class="card"><div class="label">Khu vực hoạt động</div><div class="value" id="zones">—</div></div></div>

  <div id="ban-do" class="map-layout"><section><div class="section-head"><h2>Bản đồ vị trí đội xe</h2><span class="hint">Marker cập nhật từ Kafka simulation</span></div><div id="map"></div><div class="legend"><span><i class="dot available"></i>Xe rảnh</span><span><i class="dot occupied"></i>Đang có khách</span><span id="latestVehicle">—</span></div></section><section><div class="section-head"><h2>Danh sách xe</h2><span class="hint" id="fleetCount">—</span></div><div id="fleetList" class="fleet-list"><div class="chart-note">Đang tải dữ liệu xe...</div></div></section></div>

  <div id="du-bao" class="main-grid"><section><h2>Xu hướng nhu cầu theo giờ</h2><div class="chart-wrap"><svg id="trend" viewBox="0 0 760 240" preserveAspectRatio="none"><defs><linearGradient id="area" x1="0" x2="0" y1="0" y2="1"><stop offset="0" stop-color="#55d6be" stop-opacity=".45"/><stop offset="1" stop-color="#55d6be" stop-opacity="0"/></linearGradient></defs><path class="axis" d="M30 210H750M30 115H750M30 20H750"/><path id="areaPath" class="area"/><path id="linePath" class="line"/></svg></div><div class="chart-note">Giá trị trung bình dự báo demand theo từng giờ</div></section><section><h2>Top khu vực có nhu cầu</h2><div id="ranking" class="bars"><div class="chart-note">Đang tải...</div></div></section></div>
  <section id="he-thong" style="margin-bottom:16px"><h2>Trạng thái các tầng hệ thống</h2><div class="pipeline"><div class="stage"><b>✓</b>HDFS</div><div class="stage"><b>✓</b>MapReduce</div><div class="stage"><b>✓</b>Parquet ETL</div><div class="stage"><b>✓</b>Model AI</div><div class="stage"><b>✓</b>Kafka realtime</div><div class="stage"><b>✓</b>MongoDB</div></div></section>
  <section><h2>Dự báo mới nhất</h2><div class="table-wrap"><table><thead><tr><th>Mã sự kiện</th><th>Thời gian</th><th>Khu vực đón</th><th>Khu vực trả</th><th>Demand dự báo</th><th>Trạng thái</th></tr></thead><tbody id="rows"><tr><td colspan="6">Đang tải...</td></tr></tbody></table></div></section>
  <footer>Simulation chạy liên tục: producer → Kafka → AI inference → MongoDB · Tự động cập nhật 3 giây</footer>
</main>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const esc=value=>String(value??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); const zoneSelect=document.querySelector('#zone'); let map,markers;
if(typeof L!=='undefined'){map=L.map('map').setView([40.73,-73.96],11);L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',{maxZoom:19,attribution:'© OpenStreetMap'}).addTo(map);markers=L.layerGroup().addTo(map);}else{document.querySelector('#map').innerHTML='<div class="chart-note" style="padding:30px">Không tải được bản đồ nền. Kiểm tra kết nối Internet để tải OpenStreetMap.</div>';}
async function loadZones(){const zones=await fetch('/api/zones').then(r=>r.json());zones.forEach(z=>{const o=document.createElement('option');o.value=z;o.textContent=`Khu vực ${z}`;zoneSelect.appendChild(o);});}
function drawTrend(series){const values=series.map(x=>Number(x.value||0)),max=Math.max(...values,1),left=30,top=20,width=720,height=190;const points=values.map((v,i)=>`${left+(values.length===1?width/2:i*width/(values.length-1))},${top+height-(v/max)*height}`).join(' ');document.querySelector('#linePath').setAttribute('d',points?`M ${points}`:'');document.querySelector('#areaPath').setAttribute('d',points?`M ${left} ${top+height} L ${points} L ${left+width} ${top+height} Z`:'');}
function renderFleet(items){const available=items.filter(x=>x.status==='available').length,occupied=items.filter(x=>x.status==='occupied').length;document.querySelector('#fleetTotal').textContent=items.length;document.querySelector('#fleetAvailable').textContent=available;document.querySelector('#fleetOccupied').textContent=occupied;document.querySelector('#fleetCount').textContent=`${items.length} xe`;document.querySelector('#fleetList').innerHTML=items.length?items.map(v=>`<div class="vehicle"><div><div class="vehicle-id">${esc(v.vehicle_id)}</div><div class="vehicle-meta">Khu ${esc(v.pickup_zone)} → ${esc(v.dropoff_zone)} · ${Number(v.speed_kmh||0)} km/h</div></div><span class="status ${v.status}">${v.status==='available'?'Rảnh':'Có khách'}</span></div>`).join(''):'<div class="chart-note">Chưa có snapshot xe.</div>';if(markers){markers.clearLayers();const bounds=[];items.forEach(v=>{const position=[Number(v.latitude),Number(v.longitude)];bounds.push(position);const icon=L.divIcon({className:'',html:`<div class="taxi-marker ${v.status}" title="${esc(v.vehicle_id)}">🚕</div>`,iconSize:[42,42],iconAnchor:[21,21]});L.marker(position,{icon,zIndexOffset:1000}).bindPopup(`<b>${esc(v.vehicle_id)}</b><br>Trạng thái: ${v.status==='available'?'Xe rảnh':'Đang có khách'}<br>Khu vực: ${esc(v.pickup_zone)} → ${esc(v.dropoff_zone)}<br>Tốc độ: ${Number(v.speed_kmh||0)} km/h`).addTo(markers);});if(bounds.length && !window.mapHasFitted){map.fitBounds(bounds,{padding:[30,30],maxZoom:12});window.mapHasFitted=true;}}document.querySelector('#latestVehicle').textContent=items.length?`Snapshot #${items[0].simulation_cycle} · ${items[0].updated_at}`:'Chưa có snapshot';}
async function refresh(){try{const zone=encodeURIComponent(zoneSelect.value),[data,fleet]=await Promise.all([fetch(`/api/dashboard?zone=${zone}`).then(r=>r.json()),fetch('/api/vehicles').then(r=>r.json())]);document.querySelector('#connection').textContent='Đang hoạt động';document.querySelector('#pipeline').textContent='Ổn định';document.querySelector('#count').textContent=Number(data.summary.count).toLocaleString();document.querySelector('#zones').textContent=Number(data.summary.zones).toLocaleString();document.querySelector('#updated').textContent=new Date().toLocaleTimeString();renderFleet(fleet.vehicles);drawTrend(data.series);const max=Math.max(...data.ranking.map(x=>Number(x.value||0)),1);document.querySelector('#ranking').innerHTML=data.ranking.length?data.ranking.map(x=>`<div class="bar-row"><span>Khu ${esc(x.zone)}</span><div class="bar-bg"><div class="bar-fill" style="width:${Math.max(3,Number(x.value)/max*100)}%"></div></div><span class="bar-value">${Number(x.value).toFixed(1)}</span></div>`).join(''):'<div class="chart-note">Không có dữ liệu.</div>';document.querySelector('#rows').innerHTML=data.predictions.length?data.predictions.map(p=>`<tr><td>${esc(p.event_id)}</td><td>${esc(p.event_time)}</td><td>Khu ${esc(p.pickup_zone)}</td><td>Khu ${esc(p.dropoff_zone)}</td><td><b>${Number(p.prediction||0).toFixed(0)}</b></td><td><span class="pill">Đã suy luận</span></td></tr>`).join(''):'<tr><td colspan="6">Chưa có prediction.</td></tr>';}catch(error){document.querySelector('#connection').textContent='Mất kết nối';document.querySelector('#pipeline').textContent='Kiểm tra service';}}
zoneSelect.addEventListener('change',refresh);document.querySelector('#refresh').addEventListener('click',refresh);loadZones().then(refresh);setInterval(()=>{if(document.querySelector('#auto').checked)refresh();},3000);
</script></body></html>
"""


def match_for_zone():
    zone = request.args.get("zone", "").strip()
    return ({"pickup_zone": zone} if zone else {}, zone)


@app.get("/health")
def health():
    client.admin.command("ping")
    return {"status": "ok"}


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


@app.get("/api/dashboard")
def dashboard_data():
    match, _ = match_for_zone()
    latest = list(collection.find(match, {"_id": 0}).sort("event_time", -1).limit(30))
    ranking = list(collection.aggregate([{ "$match": match }, { "$group": {"_id": "$pickup_zone", "value": {"$avg": "$prediction"}}}, { "$sort": {"value": -1} }, { "$limit": 10 }]))
    series = list(collection.aggregate([{ "$match": match }, { "$project": {"bucket": {"$substr": ["$event_time", 0, 13]}, "prediction": 1}}, { "$group": {"_id": "$bucket", "value": {"$avg": "$prediction"}}}, { "$sort": {"_id": -1} }, { "$limit": 24 }, { "$sort": {"_id": 1} }]))
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
