#
# Calculating pairwise distances between counties to create network spatial data using 
# both serial and parallel processing.
#

library("foreach")
library("doParallel")

dfCounties <- read.csv("counties.csv")
dfCounties <- na.omit(dfCounties)

mCounties <- as.matrix(dfCounties[, 1:2])
mCountiesSmall <- mCounties[1:400, ]

##########
# SERIAL #
##########

calcPWDe <- function(mat) {
	  # Brute force calculation of each individual pair
    out <- matrix(data = NA, nrow = nrow(mat), ncol = nrow(mat))

    for (row in 1:nrow(out)) {
        for (col in 1:ncol(out)) {
        	  # Calculate Euclidean distance
            out[row, col] <- sqrt(((mat[row, 1] - mat[col, 1]) ^ 2 +
                                   (mat[row, 2] - mat[col, 2]) ^ 2))
        }
    }
    return(out)
}

system.time(calcPWDe(mCountiesSmall))
system.time(calcPWDe(mCounties))

calcPWDv <- function(mat) {
	  # Distance calculation over entire rows
    out <- matrix(data = NA, nrow = nrow(mat), ncol = nrow(mat))

    for (row in 1:nrow(out)) {
        out[row, ] <- sqrt(((mat[row, 1] - mat[, 1]) ^ 2 +
                            ##                !^!
                            (mat[row, 2] - mat[, 2]) ^ 2
                            ##                !^!
                            ))
    }
    return(out)
}


system.time(calcPWDv(mCountiesSmall))
system.time(calcPWDv(mCounties))



#####################
# PARALLEL (single) #
#####################

ncores <- 2
registerDoParallel(ncores)

# To even further optimize,
# let's divide up counties over number of cores.

mRanges <- matrix(NA, nrow = ncores, ncol = 3)
## initial #/per worker
mRanges[, 3] <- floor(nrow(dfCounties)/ncores)
## number short
nShort <- nrow(dfCounties) - sum(mRanges[, 3])
## adj #/per worker
mRanges[, 3] <- mRanges[, 3] + c(rep(1, nShort), rep(0, ncores - nShort))
## worker end points
mRanges[, 2] <- cumsum(mRanges[, 3])
## worker start points
mRanges[, 1] <- mRanges[, 2] - mRanges[, 3] + 1


calcPWDfe <- function(mat) {
    # Let's use the row-wise computation to really
    # optimize this
    mOut <- foreach(i = 1:ncores,
                    .combine = rbind,
                    .noexport = "dfCounties",
                    .export = c("mRanges", "mCounties")) %dopar% {
                        mTmp <- matrix(NA,
                                       nrow = mRanges[, 3],
                                       ncol = nrow(mat)
                                       )
                        for(j in mRanges[i, 1]:mRanges[i, 2]) {
                            mTmp[i, ] <- sqrt((mat[i, 1]  - mat[, 1]) ^ 2 +
                                              (mat[i, 2]  - mat[, 2]) ^ 2)
                        }
                        return(mTmp)
                    }
    return(mOut)
}

# Single benchmark 
system.time(calcPWDfe(mCountiesSmall))
system.time(calcPWDfe(mCounties))

# Average benchmark
bs <- c()
for (i in 1:100) {
  bs <- c(bs, system.time(calcPWDfe(mCountiesSmall))[1])
}
print(mean(bs))


######################
# PARALLEL (cluster) #
######################

# Register a cluster backend
ncores <- 5
myCluster <- makeCluster(ncores)
registerDoParallel(myCluster)

mRanges <- matrix(NA, nrow = ncores, ncol = 3)
mRanges[, 3] <- floor(nrow(dfCounties)/ncores)
nShort <- nrow(dfCounties) - sum(mRanges[, 3])
mRanges[, 3] <- mRanges[, 3] + c(rep(1, nShort), rep(0, ncores - nShort))
mRanges[, 2] <- cumsum(mRanges[, 3])
mRanges[, 1] <- mRanges[, 2] - mRanges[, 3] + 1

calcPWDfe <- function(mat) {
    mOut <- foreach(i = 1:ncores,
                    .combine = rbind,
                    .noexport = "dfCounties",
                    .export = c("mRanges", "mCounties")) %dopar% {
                        mTmp <- matrix(NA,
                                       nrow = mRanges[, 3],
                                       ncol = nrow(mat)
                                       )
                        for(j in mRanges[i, 1]:mRanges[i, 2]) {
                            mTmp[i, ] <- sqrt((mat[i, 1]  - mat[, 1]) ^ 2 +
                                              (mat[i, 2]  - mat[, 2]) ^ 2)
                        }
                        return(mTmp)
                    }
    return(mOut)
}

system.time(calcPWDfe(mCountiesSmall))
system.time(calcPWDfe(mCounties))

print("OK.")

