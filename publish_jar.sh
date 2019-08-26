sbt clean assembly
aws s3 cp ./target/spark-sparse-vector-assembly-0.1.jar s3://workbenches-emr-misc/extrajars/
