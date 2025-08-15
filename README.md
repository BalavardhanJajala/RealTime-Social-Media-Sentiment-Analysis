This project is divided into 3 parts.

1. Setting up a data collection system for collecting comments data from few Subreddits & few Youtube videos.
2. Setting up a data processing system for cleaning and processing the data. We obtain sentiment scores & toxicity values using Text Blob & ModernHateSpeech API respectively.
3. Setting up a Web dashboard to display our results.

Part 1: Data Collection System

- Import the repository. Reddit & Youtube folders contain the Jar files that are required to setup the data collection system. You just need to run the jar files.
(Database setup related information is not included in this repository).
- Once the JAR files are running, you should see the APIs hitting the Reddi & Youtube servers, fetches and stores the data into DB.
- The information about the APIs that we have used can be found in the above documentation folder.

Part 2: Data Processing System

- Now that the data collection system is running, we wanted to retrieve the data in batches to process them.
- We will clean the data and we are detecting the language of the comments. Once we found that a comment is english, we further obtain the sentiment scores using text blob and hate scores using a third part api from ModernHateSpeech.com.
- This phase keeps running in the background and keeps updating all the data that is being collected.

Part 3: Data Visualisation

- The final part of this project contains a "Web Dashboard" that will display our results.
- We are displaying the analyses related to our findings in the Sentiment trend, Hate Speech percentages, relation b/w sentiment & hate speech etc.
- We are using Flask framework to setup the web application. For back end, we are using the Flask to develop our APIs which will help power the front end.
- For front-end, we are using the "chart.js" library to generate the graphs.
