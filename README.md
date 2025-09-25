# Problem Statement
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
* trains the user to do a STEDI balance exercise;
* has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
* has a companion mobile app that collects customer data and interacts with the device sensors.
  
STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.
Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.
The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.
Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

# Project Discription
In this project i extracted data produced by the STEDI step-trainer sensors and the mobile app, and curated them into a data lakehouse solution on AWS. The intent is for Data Scientists to use the solution to train machine learning models. The Data lake solution is developed using AWS Glue, AWS S3, Python, and Spark for sensor data that trains machine learning algorithms. AWS infrastructure is used to create storage zones (landing, trusted, curated).

# the solution provided by me in this repository is according to the project rubrics mentioned in the Udacity platform, i have attached the screenshots of the Athena queries , and the SQL DDL Scripts , and the glue jobs (.py file) done by me. 

# NOTE - the "sample.txt" file is generated inorder to create the respective folders in the repository, thus it contains no data.

