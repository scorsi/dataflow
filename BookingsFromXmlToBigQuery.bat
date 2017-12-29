mvn compile exec:java -Dexec.mainClass=redoute.dataflow.BookingsFromXmlToBigQuery -Dexec.args="--project=xenon-sunspot-180314 --inputFile=gs://data_bucket_flow/trackingData-20171227T150412161Z.xml --bigQueryTable=xenon-sunspot-180314:dataflow_test.Bookings --stagingLocation=gs://data_bucket_flow/staging --tempLocation=gs://data_bucket_flow/temp --runner=DirectRunner"
