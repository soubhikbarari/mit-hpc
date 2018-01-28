#
# Bootstrap regression estimation procedure using serial and
# parallel processing for Fearon data.
#

library(haven)
library(MASS)
library(foreach)
library(doParallel)

d <- read_dta("repdata.dta")
d <- d[d$onset != 4,]
formula <- onset ~ warl + gdpenl + lpopl1 + lmtnest + ncontig + Oil + 
                   nwstate + instab + polity2l + ethfrac + relfrac

dobootstrap <- function(...) {
	  # Create a bootstrap sample
    idxs <- sample(1:nrow(d), replace=TRUE)

    # Create a bootstrap sample
    D <- d[idxs,]
    
    # Get logit estimates
    fit <- glm(formula, data=D, family=binomial(link = "logit"))
}

# Serial execution
system.time(sapply(1:1000, dobootstrap))

# Parallel execution
registerDoParallel(cores=2)
system.time(foreach(i=1:1000, .combine='cbind') %dopar% dobootstrap())

# HPC execution (only works in RCE)
hpc <- makeCluster(6)
registerDoParallel(hpc)
system.time(foreach(i=1:5, .combine='cbind', .packages="MASS") %dopar% dobootstrap())

