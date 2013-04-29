SET default_parallel 10;

speeds = LOAD '$SPEEDS_DIR' USING PigStorage('|') 
       AS (num:long, filepath:chararray, lat1:double, lon1:double, 
       lat2:double, lon2:double, time1:datetime, time2:datetime, dist:double, time_between:long, 
       speed:double);
curr = FOREACH speeds 
         GENERATE 
             num, filepath, speed;
prev = FOREACH speeds 
         GENERATE 
             num + 1 AS num, filepath, speed;

speeds_next = JOIN curr BY (filepath, num), prev BY (filepath, num);
accelerations = FOREACH speeds_next 
              GENERATE
                  curr::num AS num,
                  curr::filepath AS filepath,
                  curr::speed AS speed1,
                  prev::speed AS speed2,
                  curr::speed - prev::speed AS acceleration;
STORE accelerations INTO '$OUT_DIR' USING PigStorage('|');
