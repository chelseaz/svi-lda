## Running locally

    # Run LDA on NIPS dataset
    sbt 'runMain RunLDA --dataset nips'

    # Process and run LDA on Clinton dataset
    sbt 'runMain ProcessClinton'
    sbt 'runMain RunLDA --dataset clinton'

    # Package and run
    sbt assembly
    spark-submit \
        --class RunLDA \
        --driver-memory 4G \
         target/scala-2.10/svi-lda-assembly-1.0.jar \
         --dataset nips


## Run Spark cluster in EC2

Obtain your AWS access key ID and secret access key from AWS Security Credentials. 

Create an EC2 key pair named svi-lda and save it to `~/.ssh/svi-lda.pem`.

    export AWS_ACCESS_KEY_ID=
    export AWS_SECRET_ACCESS_KEY=
    export CLUSTER_NAME=svi-lda

    # Start cluster
    $SPARK_HOME/ec2/spark-ec2 \
        -k svi-lda \
        -i ~/.ssh/svi-lda.pem \
        -s 8 \
        --region=us-east-1 \
        --instance-type=c3.2xlarge \
        --master-instance-type=c3.large \
        --spot-price=0.10 \
        launch $CLUSTER_NAME

    SPARK_MASTER=$($SPARK_HOME/ec2/spark-ec2 get-master $CLUSTER_NAME | tail -n 1)
    echo "Run this on master later:\nexport MASTER_DNS=$SPARK_MASTER"

    # SSH into cluster
    $SPARK_HOME/ec2/spark-ec2 \
        -k svi-lda \
        -i ~/.ssh/svi-lda.pem \
        login $CLUSTER_NAME

    mkdir -p svi-lda/log

Start another shell, navigate to this github repo and do

    # Provide JAR to cluster master
    scp -i ~/.ssh/svi-lda.pem \
        target/scala-2.10/svi-lda-assembly-1.0.jar \
        root@${SPARK_MASTER}:svi-lda/

    scp -r -i ~/.ssh/svi-lda.pem \
        data bin \
        root@${SPARK_MASTER}:svi-lda/

Switch back to the shell logged into the cluster:

    alias hadoop="ephemeral-hdfs/bin/hadoop"
    hadoop fs -copyFromLocal svi-lda /

    export MASTER_DNS=<master address from earlier>

    # Submit application
    svi-lda/bin/spark-submit.sh nips > output.log 2>&1 &

When you're done:

    # Stop cluster
    $SPARK_HOME/ec2/spark-ec2 destroy $CLUSTER_NAME