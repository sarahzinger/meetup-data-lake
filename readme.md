# Datalakes + Grafana

This package runs: 
- a fake data lake server that returns events in json and metrics in a csv, that can then be queried as sql with new sql expressions feature

To run locally: 
```
docker compose up --build
```

To clear volumes and tear down: 
```
docker compose down -v
```


To query data from command line: 
```
curl "http://localhost:8080/datasets"
curl "http://localhost:8080/query/events?limit=5&sort=ts&order=desc"
curl "http://localhost:8080/query/metrics?format=csv&limit=10"
```

To view grafana: 
navigate to http://localhost:3000/
- should see an instance of the infinity plugin with a dashboard to query the "fake lake" 
- should have new sql expressions feature enabled

To query within grafana: 
change localhost to fakelake: 
http://fakelake:8080/query/events?limit=200&sort=ts&order=desc

