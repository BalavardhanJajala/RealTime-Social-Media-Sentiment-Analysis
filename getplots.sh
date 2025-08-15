#!/bin/bash

# Change to the plots directory
cd /home/akunamneni/plots

# Remove all existing PNG files
rm -f *.png

# Run each .py file
python3 sentiment_hate.py
python3 hate_count.py
python3 sentiment_count.py
#python3 plot1.py
#python3 plot2.py
#python3 plot3.py
#python3 plot4.py
#python3 politics.py
#python3 cdf.py
#python3 comments_daily.py 
#python3 sub_daily.py
#python3 plot5.py
# Add more lines if you have additional .py files

# Change back to the plots directory
cd /home/akunamneni/plots

# Remove all existing PNG files in the static directory
rm -f /home/akunamneni/static/*.png

# Copy all generated PNG files to the static directory
cp *.png /home/akunamneni/static/
