FROM apache/hive:3.1.3

USER root

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.0/hadoop-aws-3.1.0.jar /opt/hive/lib/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar /opt/hive/lib/

RUN chmod 644 /opt/hive/lib/*.jar
