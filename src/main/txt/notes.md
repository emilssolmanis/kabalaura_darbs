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

    ds <- d$acceleration[d$segmentId == 3]; 
    s <- spectrum(ds, log="no", fast=FALSE, taper=0.1, spans=c(5, 9));
    k5 <- kernel("modified.daniell", c(1));
    filt5 <- c(k5$coef, k5$coef[1], rev(k5$coef));
    k9 <- kernel("modified.daniell", c(2));
    filt9 <- c(k9$coef, k9$coef[1], rev(k9$coef));
    dFilt <- convolve(filt5, filt9, type="open");
    filtered <- convolve((length(ds) / 4) * (Mod(fft(taper(ds - mean(ds), 0.1))[1:(length(ds)/2 + 1)]) / (length(ds) / 2))^2, dFilt, type="open");
    plot(c(1:(length(filtered))) / (length(filtered)), filtered, type="l");



$kernel
mDaniell(1,2) 
coef[-3] = 0.03125
coef[-2] = 0.12500
coef[-1] = 0.21875
coef[ 0] = 0.25000
coef[ 1] = 0.21875
coef[ 2] = 0.12500
coef[ 3] = 0.03125
