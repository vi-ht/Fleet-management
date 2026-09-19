db = db.getSiblingDB('taxi');
db.createCollection('demand_predictions');
db.demand_predictions.createIndex({ event_time: -1 });
db.demand_predictions.createIndex({ pickup_zone: 1, event_time: -1 });
db.demand_predictions.createIndex({ event_id: 1 });
db.createCollection('vehicle_status');
db.vehicle_status.createIndex({ vehicle_id: 1 }, { unique: true });
db.vehicle_status.createIndex({ status: 1 });
