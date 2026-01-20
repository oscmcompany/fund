mask infrastructure images build datamanager server
mask infrastructure images push datamanager server
aws ecs update-service --cluster pocketsizefund-application --service pocketsizefund-datamanager --force-new-deployment > /dev/null

mask infrastructure images build equitypricemodel server
mask infrastructure images push equitypricemodel server
aws ecs update-service --cluster pocketsizefund-application --service pocketsizefund-equitypricemodel --force-new-deployment > /dev/null

mask infrastructure images build portfoliomanager server
mask infrastructure images push portfoliomanager server
aws ecs update-service --cluster pocketsizefund-application --service pocketsizefund-portfoliomanager --force-new-deployment > /dev/null

mask infrastructure stack up
