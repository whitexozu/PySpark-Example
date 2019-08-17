#!/bin/bash
for i in 0 1 
do
   spark-submit createRelXQTableUsePythonThread2.py yarn $i
done
