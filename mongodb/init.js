db = db.getSiblingDB('taxi');
db.createCollection('hotspot_predictions');
db.hotspot_predictions.createIndex({ event_time: -1 });
db.hotspot_predictions.createIndex({ pickup_zone: 1, event_time: -1 });
db.hotspot_predictions.createIndex({ event_id: 1 });
db.createCollection('vehicle_status');
db.vehicle_status.createIndex({ vehicle_id: 1 }, { unique: true });
db.vehicle_status.createIndex({ status: 1 });
