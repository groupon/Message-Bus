#!/bin/bash
git clone https://github.com/hornetq/hornetq.git 
cd hornetq
git checkout HornetQ_2_2_22_AS7_Final
patch -p0 < ../groupon.patch