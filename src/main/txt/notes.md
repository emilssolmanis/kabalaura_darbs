 * The segmentation very often puts a "walking" segment for trolley in the place where it turns 
   from Baložu onto Kristapa. Probably because it slows down ridiculously.

## Calculating the periodogram

The following R code to yields almost equivalent results

    taper <- function(inputVec, p) {
      weights <- c();
      for (i in 1:length(inputVec)) {
        t <- (i / length(inputVec));
        if (t >= 0 && t < p / 2) {
          weights <- c(weights, (1 - cos(2 * pi * t / p)) / 2);
        }
        else if((p / 2) <= t && t < (1 - p / 2)) {
          weights <- c(weights, 1);
        }
        else if ((1 - p / 2) <= t && t <= 1) {
          weights <- c(weights, (1 - cos(2 * pi * (1 - t) / p)) / 2);
        }
      }
      inputVec * weights;
    }

    ds <- d$acceleration[d$segmentId == 16]; 
    s <- spectrum(ds, log="no", fast=FALSE, taper=0.1, spans=c(5, 9));
    k5 <- kernel("modified.daniell", c(1));
    filt5 <- c(k5$coef, k5$coef[1], rev(k5$coef));
    k9 <- kernel("modified.daniell", c(2));
    filt9 <- c(k9$coef, k9$coef[1], rev(k9$coef));
    dFilt <- convolve(filt5, filt9, type="open");
    filtered <- convolve((length(ds) / 4) * (Mod(fft(taper(ds - mean(ds), 0.1))[1:(length(ds)/2 + 1)]) / (length(ds) / 2))^2, dFilt, type="open");
    plot(c(1:(length(filtered))) / (length(filtered)), filtered, type="l");


pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-000 -param SPEED_THR=5.0 -param ACCEL_THR=3.0 -param SEG_TIME_THR=30.0 -param SEG_DIST_THR=150.0 -param SEG_CERT_DIST=250.0 -param UNCERT_MERGE_THR=3 segment_tracks.pig 
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-001 -param SPEED_THR=4.0 -param ACCEL_THR=2.0 -param SEG_TIME_THR=20.0 -param SEG_DIST_THR=125.0 -param SEG_CERT_DIST=200.0 -param UNCERT_MERGE_THR=3 segment_tracks.pig 
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-002 -param SPEED_THR=3.0 -param ACCEL_THR=2.0 -param SEG_TIME_THR=15.0 -param SEG_DIST_THR=100.0 -param SEG_CERT_DIST=150.0 -param UNCERT_MERGE_THR=3 segment_tracks.pig 
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-003 -param SPEED_THR=3.0 -param ACCEL_THR=2.0 -param SEG_TIME_THR=15.0 -param SEG_DIST_THR=75.0 -param SEG_CERT_DIST=100.0 -param UNCERT_MERGE_THR=2 segment_tracks.pig
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-004 -param SPEED_THR=5.0 -param ACCEL_THR=2.0 -param SEG_TIME_THR=35.0 -param SEG_DIST_THR=200.0 -param SEG_CERT_DIST=300.0 -param UNCERT_MERGE_THR=3 segment_tracks.pig
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-005 -param SPEED_THR=5.0 -param ACCEL_THR=3.0 -param SEG_TIME_THR=45.0 -param SEG_DIST_THR=250.0 -param SEG_CERT_DIST=350.0 -param UNCERT_MERGE_THR=4 segment_tracks.pig
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-006 -param SPEED_THR=2.8 -param ACCEL_THR=1.75 -param SEG_TIME_THR=15.0 -param SEG_DIST_THR=50.0 -param SEG_CERT_DIST=250.0 -param UNCERT_MERGE_THR=2 segment_tracks.pig
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-007 -param SPEED_THR=2.8 -param ACCEL_THR=1.75 -param SEG_TIME_THR=10.0 -param SEG_DIST_THR=50.0 -param SEG_CERT_DIST=150.0 -param UNCERT_MERGE_THR=2 segment_tracks.pig
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-008 -param SPEED_THR=2.8 -param ACCEL_THR=1.75 -param SEG_TIME_THR=25.0 -param SEG_DIST_THR=125.0 -param SEG_CERT_DIST=200.0 -param UNCERT_MERGE_THR=2 segment_tracks.pig
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-009 -param SPEED_THR=3.5 -param ACCEL_THR=2.0 -param SEG_TIME_THR=20.0 -param SEG_DIST_THR=100.0 -param SEG_CERT_DIST=200.0 -param UNCERT_MERGE_THR=4 segment_tracks.pig
pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-010 -param SPEED_THR=3.5 -param ACCEL_THR=2.0 -param SEG_TIME_THR=15.0 -param SEG_DIST_THR=150.0 -param SEG_CERT_DIST=200.0 -param UNCERT_MERGE_THR=4 segment_tracks.pig

fine walk
0, 1, 4, 5

fine drive
1, 2, 3, 5, 7, 8

pig -param GPS_DIR=bakalaurs/csv -param OUT_DIR=bakalaurs/segmentation-001 -param SPEED_THR=4.0 -param ACCEL_THR=2.0 -param SEG_TIME_THR=20.0 -param SEG_DIST_THR=125.0 -param SEG_CERT_DIST=200.0 -param UNCERT_MERGE_THR=3 segment_tracks.pig