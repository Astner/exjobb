library(parallel)
library(plyr)
library(ggplot2)
library(scales)
library(GGally)

# TODO: Fix style, vars are both camelCase and dot.named now

histogramAndDensity <- function(plot) {
  newPlot <- plot + geom_histogram(aes(y=..density..), colour="black", fill="white") +
             geom_density(alpha=.2, fill="#FF6666")

  newPlot
}

logPlot <- function(plot) {
  logPlot <- plot + scale_x_log10(
      breaks=trans_breaks('log10', function(x) 10^x),
      labels = trans_format('log10', math_format(10^.x)))
  logPlot
}

savePlotToPath <- function(plot, filename, path) {
  ggsave(file.path(path, filename), plot, width=6, height=6)
}


loadAndClean <- function(filename) {
  # Loads and cleans one file containing numeric values that was exported using Spark's RDD.saveAsTextFile()
  # Expected line format is csv-like: (v1,v2,...,vn). The values are all numeric
  # Args:
  #   filename: The name of the file to read
  #
  # Returns:
  #   A dataframe.

  convertMagic <- function(obj, type){
    # http://stackoverflow.com/a/11263399/209882
    # Converts the columns of a dataframe to a number of types
    FUN1 <- switch(type,
                   character = as.character,
                   numeric = as.numeric,
                   factor = as.factor)
    as.data.frame(lapply(obj, FUN1))
  }
  oneLine <- scan(filename,sep=',', what="character" , nlines=1)
  noItems <- length(oneLine)
  a <- read.table(filename, sep = ',', stringsAsFactors = FALSE, colClasses = "character", strip.white = TRUE)
  # Remove parens from first and last column, keep numbers, decimal point, and negative sign
  a[-1] <- gsub("[(|)]", "", as.matrix(a[,-1]))
  a[1] <- gsub("[(|)]", "", as.matrix(a[,1]))

  convertMagic(a, "numeric")
}

loadAndCleanCommon <- function(filename) {
  # Loads and cleans one file containing numeric values that was exported using Spark's RDD.saveAsTextFile()
  # Expected line format is csv-like: (v1,v2,...,vn). The values are all numeric
  # Args:
  #   filename: The name of the file to read
  #
  # Returns:
  #   A dataframe.

  convertMagic <- function(obj, type){
    # http://stackoverflow.com/a/11263399/209882
    # Converts the columns of a dataframe to a number of types
    FUN1 <- switch(type,
                   character = as.character,
                   numeric = as.numeric,
                   factor = as.factor)
    as.data.frame(lapply(obj, FUN1))
  }
  oneLine <- scan(filename,sep=',', what="character" , nlines=1)
  noItems <- length(oneLine)
  a <- read.table(filename, sep = ',', stringsAsFactors = FALSE, colClasses = c(rep("NULL", 2), rep("character", 2)), strip.white = TRUE)
  # Remove parens from first and last column, keep numbers, decimal point, and negative sign
  a[-1] <- gsub("[(|)]", "", as.matrix(a[,-1]))
  a[1] <- gsub("[(|)]", "", as.matrix(a[,1]))
  a[-1] <- gsub("None", "0", as.matrix(a[,-1]))
  a[1] <- gsub("None", "0", as.matrix(a[,1]))
  a[-1] <- gsub("[^-0-9\\.E\\,]", "", as.matrix(a[,-1]))
  a[1] <- gsub("[^-0-9\\.E\\,]", "", as.matrix(a[,1]))

  convertMagic(a, "numeric")
}


loadSparkFiles <- function(path, loadingFunction=loadAndClean,
                           filePattern="part-.*$",
                           cluster=NULL, cachedFile = "",rebuildData=FALSE) {
    # Loads the files matched by filePattern from path using cluster for creating the dataframes in parallel
    # Arguments:
    #    path: the path containing the graph files
    #    filePattern: a regular expression used to match the filenames apart from the class name
    #    cluster: optionally a cluster object created by the parallel package
    #    cachedFile: The filepath to the R binary file of the dataframe
    #    rebuildData: flag to toggle wether to use cached data for the dataframes or re-read the features from the files
    # Returns:
    #    A dataframe containing the data from the files matched by the pattern
    # fullPattern <- sprintf("(%s)%s", className, filePattern)

    #
    if (rebuildData || !file.exists(cachedFile)) {
        # We rebuild the dataframes by reading the csv files
        sparkFiles <- list.files(path=path,
                                full.names = TRUE,
                                pattern=filePattern)
        if (is.null(cluster)) {
            corrList <- lapply(sparkFiles, loadingFunction)
        }
        else {
            corrList <- parLapply(cluster, sparkFiles, loadingFunction)
        }

        # We can't really parallelize rbind, but using plyr makes it much faster
        # TODO: Maybe try data.table rbindlist?
        corrDF <- rbind.fill(corrList)

        return(corrDF)
    }
    else {
        # We use the cached data
        corrDF <- readRDS(cachedFile)
        return(corrDF)
    }
}

# Function to avoid having to repeat cluster setup etc.
# Usage:
# loadBigrams(path, no.cores = 4) <- {
#   renames <- c("V1"="vtx1.ID", "V2"="vtx2.ID", "V3"="edge.weight")
#   return loadSparkFolder(path, no.cores, renames)
# }
loadSparkFolder <- function(path, no.cores, renames) {
  # Renames is a vector like: c("V1"="vtx1.ID", "V2"="vtx2.ID", "V3"="edge.weight")
  cl <- makeCluster(getOption("cl.cores", no.cores))
  clusterEvalQ(cl, library(reshape2))
  cachedFile <- file.path(path, "cache.rds")

  df <- loadSparkFiles(path, cluster=cl, cachedFile=cachedFile)
  # Will throw warning when the data have already been saved to RDS
  df <- rename(df, renames)

  saveRDS(df, cachedFile)

  stopCluster(cl)
  df

}


loadMutual <- function(path, no.cores = 4) {
  loadSparkFolder(
    path,
    no.cores,
    c("V1"="vtx1.ID", "V2"="vtx2.ID","V3"="px", "V4"="py", "V5"="pxy", "V6"="mi"))
}

# Mutual information plots
# mi <- loadMutual(path, 16)
# pxPlot <- ggplot(mi, aes(x=px))
# pyPlot <- ggplot(mi, aes(x=py))
# pxyPlot <- ggplot(mi, aes(x=pxy))
# miPlot <- ggplot(mi, aes(x=mi))
# pxHist <- histogramAndDensity(pxPlot)
# pyHist <- histogramAndDensity(pyPlot)
# pxyHist <- histogramAndDensity(pxyPlot)
# pxLogHist <- histogramAndDensity(logPlot(pxPlot))
# pyLogHist <- histogramAndDensity(logPlot(pyPlot))
# pxyLogHist <- histogramAndDensity(logPlot(pxyPlot))
# miHist <- histogramAndDensity(miPlot)
# savePlotToPath(pxHist, "px.pdf", path)
# savePlotToPath(pyHist, "py.pdf", path)
# savePlotToPath(pxyHist, "pxy.pdf", path)
# savePlotToPath(pxLogHist, "pxLog.pdf", path)
# savePlotToPath(pyLogHist, "pyLog.pdf", path)
# savePlotToPath(pxyLogHist, "pxyLog.pdf", path)
# savePlotToPath(miHist, "mi.pdf", path)

loadEdges <- function(path, no.cores = 4) {
  loadSparkFolder(path, no.cores, c("V1"="vtx1.ID", "V2"="vtx2.ID", "V3"="edge.weight"))
}

loadVertices <- function(path, no.cores = 4) {
  loadSparkFolder(path, no.cores, c("V1"="vtx.ID", "V2"="vtx.string", "V3"="vtx.weight"))
}

loadEdgeErrorSum <- function(path, no.cores = 4) {
  loadSparkFolder(path, no.cores, c("V1"="vtx.ID", "V2"="discarded.sum"))
}


loadSharedTerms <- function(path, no.cores = 4) {
  loadSparkFolder(
    path,
    no.cores,
    c("V1"="vtx.i.ID", "V2"="vtx.j.ID", "V3"="Sij", "V4"="ri", "V5"="rj", "V6"="di", "V7"="dj"))
}

plotWeightDistributions <- function(experimentName, no.cores = 4) {
    # Assuming working dir is concepts/code/src/R/ and output
    # files are placed in concepts/code/output/experimentName
    path <- file.path("../../output", experimentName)
    verticesPath <- file.path(path, paste(experimentName, "-vertices", sep=""))
    edgesPath <- file.path(path, paste(experimentName, "-edges", sep=""))

    vertices <- loadVertices(verticesPath, no.cores)
    edges <- loadEdges(edgesPath, no.cores)

    vtxWeightPlot <- histogramAndDensity(ggplot(vertices, aes(x=vtx.weight)))
    vtxWeightLogPlot <- logPlot(vtxWeightPlot)

    savePlotToPath(vtxWeightPlot, "vertex_weight.pdf", path)
    savePlotToPath(vtxWeightLogPlot, "vertex_weight_log.pdf", path)

    edgeWeightPlot <- histogramAndDensity(ggplot(edges, aes(x=edge.weight)))
    edgeWeightLogPlot <- logPlot(edgeWeightPlot)

    savePlotToPath(edgeWeightPlot, "edge_weight.pdf", path)
    savePlotToPath(edgeWeightLogPlot, "edge_weight_log.pdf", path)

}

edgeErrorStats <- function(path, noCores = 4) {
  # Max error per vertex: discardedEdgeWeightSums
  # ("vtx.ID", "discarded.sum")
  df <- loadEdgeErrorSum(path, noCores)

  edgeErrorPlot <- histogramAndDensity(ggplot(df, aes(x=discarded.sum)))

  savePlotToPath(edgeErrorPlot, "edge_error.pdf", path)

  edgeErrorPlot
}

sharedTermsStats <- function(path, noCores = 4) {
  # TODO: Provide only experiment name (+timestamp) and do the loading using relative path
  # Max error per similarity relation: (di + dj)/tot
  # Relative (wrt similarity) max error per similarity relation: (di + dj)/(sij + tot)
  # Relative L1 norm: (sij + tot)/tot
  df <- loadSharedTerms(path, noCores)

  sijPlot <- histogramAndDensity(ggplot(df, aes(x=Sij)))
  keptDiscardedRatioPlot <- histogramAndDensity(ggplot(df, aes(x=(ri+rj)/(di+dj))))
  totalErrorPlot <- histogramAndDensity(ggplot(df, aes(x=ri+rj+di+dj)))
  maxErrorPerSimilarityPlot <- histogramAndDensity(ggplot(df, aes(x=(di+dj)/(ri+rj+di+dj))))
  relErrorPerSimilarityPlot <- histogramAndDensity(ggplot(df, aes(x=(di+dj)/(Sij+ri+rj+di+dj))))
  relL1NormPlot <- histogramAndDensity(ggplot(df, aes(x=(Sij+ri+rj+di+dj)/(ri+rj+di+dj))))
  minRelL1NormPlot <- histogramAndDensity(ggplot(df, aes(x=(Sij+ri+rj)/(ri+rj+di+dj))))

  savePlotToPath(sijPlot, "Sij.pdf", path)
  savePlotToPath(keptDiscardedRatioPlot, "kept_discarded_ratio.pdf", path)
  savePlotToPath(totalErrorPlot, "total_error.pdf", path)
  savePlotToPath(maxErrorPerSimilarityPlot, "max_error_per_similarity.pdf", path)
  savePlotToPath(relErrorPerSimilarityPlot, "rel_error_per_similarity.pdf", path)
  savePlotToPath(relL1NormPlot, "rel_L1_norm.pdf", path)
  savePlotToPath(minRelL1NormPlot, "min_rel_L1_norm.pdf", path)


  c(maxErrorPerSimilarityPlot, relErrorPerSimilarityPlot, relL1NormPlot)
}

createCorrelationPlots <- function(path, sharedTermsDF) {
  corrPlot <- ggpairs(
    sharedTermsDF, diag=list(continuous="density"), axisLabels='show',
    columns=c("Sij", "ri", "rj", "di", "dj"))
  png(file.path(path, "correlation_plot.png"), height=1000, width=1000)
  print(corrPlot)
  dev.off()
}


benchmarkCorrelation <- function(filepath) {
  df <- loadAndClean(filepath)
  corVal <- cor(df, method="spearman")
  c(filepath, corVal[2])
}

wordBenchmarksDir <- function(path) {
  # TODO: give both execution path and experiment path as args?
  # List the files in the input dir and apply to correlation function
  # to each of them.
  # path <- file.path("../../output", experimentPath, "wordBenchmarks")
  benchFiles <- list.files(path=path,
                           full.names = TRUE,
                           pattern="*existingPairs*")
  corrList <- lapply(benchFiles, benchmarkCorrelation)
  # saveRDS(corrList, file.path(path, "benchmarkList.rds"))
  # Save output under same directory, by redirecting output
  sink(file.path(path, "benchMarkList.txt"), split=TRUE)
  str(corrList)
  sink()
  corrList
}

# TODO: Histogram creation from Spark output
