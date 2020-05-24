#!/usr/bin/env sh
mvn clean package
spark-submit target/yzhan737_class_project-1.0-SNAPSHOT.jar $1 $2 $3